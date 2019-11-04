package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type ViewManager interface {
	SetupViews(db *Database) error
	Initialize(db *Database) error
	ListViewFiles() ([]string, error)
	OpenView(viewName string, ddoc *DesignDocument) error
	SelectView(updateSeqID, ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	Close() error
	Vacuum() error
	UpdateDesignDocument(ddocID string, doc *Document) error
	ValidateDDoc(ddoc *DesignDocument) error
	CalculateSignature(ddocv *DesignDocumentView) string
	ParseQuery(query string) (string, []string)
	GetView(name string) (*View, bool)
}

type DefaultViewManager struct {
	viewPath string
	dbPath   string
	dbName   string
	views    map[string]*View
	ddocs    map[string]*DesignDocument

	viewFiles map[string]map[string]bool

	rwmux sync.RWMutex
}

func (mgr *DefaultViewManager) SetupViews(db *Database) error {
	ddoc := &DesignDocument{}
	ddoc.ID = "_design/_views"
	ddoc.Views = make(map[string]*DesignDocumentView)
	ddv := &DesignDocumentView{}
	ddv.Setup = append(ddv.Setup, "CREATE TABLE IF NOT EXISTS all_docs (key TEXT PRIMARY KEY, value TEXT, doc_id TEXT)")
	ddv.Delete = append(ddv.Delete, "DELETE FROM all_docs WHERE doc_id IN (SELECT doc_id FROM latest_changes)")
	ddv.Update = append(ddv.Update, "INSERT INTO all_docs (key, value, doc_id) SELECT doc_id, JSON_OBJECT('version',JSON_EXTRACT(data, '$._version')), doc_id FROM latest_documents")
	ddv.Select = make(map[string]string)
	ddv.Select["default"] = "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) WHERE (${key} IS NULL or key = ${key})"
	ddv.Select["with_docs"] = "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('id', doc_id, 'key', key, 'value', JSON(value), 'doc', JSON((SELECT data FROM docsdb.documents WHERE doc_id = o.doc_id)))),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) o WHERE (${key} IS NULL or key = ${key})"

	ddoc.Views["_all_docs"] = ddv

	ddvlatestchanges := &DesignDocumentView{}
	ddvlatestchanges.Select = make(map[string]string)
	ddvlatestchanges.Select["default"] = "SELECT JSON_GROUP_ARRAY(doc_id) FROM latest_changes"
	ddvlatestchanges.Select["with_docs"] = "SELECT JSON_GROUP_ARRAY(JSON_OBJECT('doc_id', doc_id, 'doc', data)) FROM latest_documents"

	ddoc.Views["latest_changes"] = ddvlatestchanges

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(ddoc)

	designDoc, err := ParseDocument(buffer.Bytes())
	if err != nil {
		panic(err)
	}

	_, err = db.PutDocument(designDoc)
	if err != nil {
		return err
	}
	return nil
}

func (mgr *DefaultViewManager) Initialize(db *Database) error {

	mgr.rwmux = sync.RWMutex{}

	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()

	docs, _ := db.GetAllDesignDocuments()
	for _, x := range docs {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(x.Data, ddoc)
		if err != nil {
			return err
		}
		mgr.ddocs[x.ID] = ddoc
	}

	//load current view files
	viewFiles, _ := mgr.ListViewFiles()
	for _, x := range viewFiles {
		if _, ok := mgr.viewFiles[x]; !ok {
			mgr.viewFiles[x] = make(map[string]bool)
		}
	}

	// view file ref counter
	for _, ddoc := range mgr.ddocs {
		for vname, ddocv := range ddoc.Views {
			viewFile := mgr.dbName + "$" + mgr.CalculateSignature(ddocv)
			qualifiedViewName := ddoc.ID + "$" + vname
			if _, ok := mgr.viewFiles[viewFile]; !ok {
				mgr.viewFiles[viewFile] = make(map[string]bool)
			}
			(mgr.viewFiles[viewFile])[qualifiedViewName] = true
		}
	}

	for fileName, x := range mgr.viewFiles {
		if len(x) <= 0 {
			delete(mgr.viewFiles, fileName)
			os.Remove(filepath.Join(mgr.viewPath, fileName+dbExt))
		}
	}

	return nil
}

func (mgr *DefaultViewManager) ListViewFiles() ([]string, error) {
	list, err := ioutil.ReadDir(mgr.viewPath)
	if err != nil {
		return nil, err
	}
	var viewFiles []string
	for idx := range list {
		name := list[idx].Name()
		if strings.HasPrefix(name, mgr.dbName+"$") && strings.HasSuffix(name, dbExt) {
			viewFiles = append(viewFiles, strings.ReplaceAll(name, dbExt, ""))
		}
	}
	return viewFiles, nil
}

func (mgr *DefaultViewManager) OpenView(viewName string, ddoc *DesignDocument) error {

	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()

	if _, ok := mgr.views[ddoc.ID+"$"+viewName]; ok {
		return errors.New("view_exists")
	}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	viewFilePath := filepath.Join(mgr.viewPath, mgr.dbName+"$"+mgr.CalculateSignature(ddoc.Views[viewName])+dbExt)
	viewFilePath += "?_journal=MEMORY"

	view := NewView(mgr.dbPath, viewFilePath, viewName, ddoc, mgr)
	if err := view.Open(); err != nil {
		return err
	}

	mgr.views[ddoc.ID+"$"+viewName] = view

	return nil
}

func (mgr *DefaultViewManager) SelectView(updateSeqID, ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {

	//fmt.Println(mgr.viewFiles)
	//fmt.Println(mgr.views)

	name := ddocID + "$" + viewName

	mgr.rwmux.RLock()
	unlocked := false
	runlock := func() {
		if !unlocked {
			mgr.rwmux.RUnlock()
		}
		unlocked = true
	}
	defer runlock()

	view, ok := mgr.views[name]

	if !ok {

		ddoc, ok := mgr.ddocs[ddocID]
		if !ok {
			return nil, errors.New(DOC_NOT_FOUND)
		}
		_, ok = ddoc.Views[viewName]
		if !ok {
			return nil, errors.New(VIEW_NOT_FOUND)
		}

		runlock()
		err := mgr.OpenView(viewName, ddoc)
		if err != nil {
			return nil, err
		}
		mgr.rwmux.RLock()
		defer mgr.rwmux.RUnlock()

		view = mgr.views[name]
	}

	if view == nil {
		return nil, errors.New(VIEW_NOT_FOUND)
	}

	if !stale {
		err := view.Build(updateSeqID)
		if err != nil {
			return nil, err
		}
	}

	//fmt.Println(mgr.viewFiles, mgr.views)

	return view.Select(selectName, values)
}

func (mgr *DefaultViewManager) Close() error {
	mgr.rwmux.RLock()
	defer mgr.rwmux.RUnlock()
	for _, v := range mgr.views {
		v.Close()
	}

	return nil
}

func (mgr *DefaultViewManager) GetView(name string) (*View, bool) {
	if view, ok := mgr.views[name]; true {
		return view, ok
	}
	return nil, false
}

func (mgr *DefaultViewManager) Vacuum() error {
	mgr.rwmux.RLock()
	defer mgr.rwmux.RUnlock()
	for _, v := range mgr.views {
		v.Vacuum()
	}
	return nil
}

func (mgr *DefaultViewManager) UpdateDesignDocument(ddocID string, doc *Document) error {

	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()

	var updatedViews map[string]string = make(map[string]string)
	newDDoc := &DesignDocument{}

	if doc.Deleted {
		for qualifiedViewName, view := range mgr.views {
			if strings.HasPrefix(qualifiedViewName, ddocID+"$") {
				view.Close()
				delete(mgr.views, qualifiedViewName)
				updatedViews[qualifiedViewName] = ""
			}
		}
	} else {
		err := json.Unmarshal(doc.Data, newDDoc)
		if err != nil {
			panic("invalid_design_document " + ddocID)
		}

		for vname, nddv := range newDDoc.Views {
			var (
				currentViewFile   string
				newViewFile       string
				qualifiedViewName string = ddocID + "$" + vname
			)
			newViewFile = mgr.dbName + "$" + mgr.CalculateSignature(nddv)
			if _, ok := mgr.viewFiles[newViewFile]; !ok {
				mgr.viewFiles[newViewFile] = make(map[string]bool)
			}
			mgr.viewFiles[newViewFile][qualifiedViewName] = true

			if _, ok := mgr.views[qualifiedViewName]; ok {
				mgr.views[qualifiedViewName].Close()
				delete(mgr.views, qualifiedViewName)
			}

			if currentDDoc, ok := mgr.ddocs[ddocID]; ok {
				if cddv, _ := currentDDoc.Views[vname]; cddv != nil {
					currentViewFile = mgr.dbName + "$" + mgr.CalculateSignature(cddv)
				}
			}

			//To takecare old one
			if currentViewFile != "" && len(mgr.viewFiles[currentViewFile]) <= 0 {
				delete(mgr.viewFiles, currentViewFile)
				os.Remove(filepath.Join(mgr.viewPath, currentViewFile+dbExt))
			}

			updatedViews[qualifiedViewName] = newViewFile
		}
	}

	currentDDoc, ok := mgr.ddocs[ddocID]
	if ok {
		//to takecare of missing ones
		for vname, cddv := range currentDDoc.Views {
			qualifiedViewName := ddocID + "$" + vname
			currentViewFile := mgr.dbName + "$" + mgr.CalculateSignature(cddv)
			if newViewFile, ok := updatedViews[qualifiedViewName]; !ok || newViewFile != currentViewFile {
				delete(mgr.viewFiles[currentViewFile], qualifiedViewName)

				if len(mgr.viewFiles[currentViewFile]) <= 0 {
					delete(mgr.viewFiles, currentViewFile)
					os.Remove(filepath.Join(mgr.viewPath, currentViewFile+dbExt))
				}
			}
		}
	}

	//fmt.Println(mgr.viewFiles, mgr.views)

	if doc.Deleted {
		delete(mgr.ddocs, ddocID)
	} else {
		mgr.ddocs[ddocID] = newDDoc
	}

	return nil
}

func (mgr *DefaultViewManager) ValidateDDoc(ddoc *DesignDocument) error {

	for _, v := range ddoc.Views {
		for _, x := range v.Setup {
			_ = x
		}
	}

	return nil
}

func (mgr *DefaultViewManager) CalculateSignature(ddocv *DesignDocumentView) string {
	content := ""
	if ddocv != nil {
		crc32q := crc32.MakeTable(0xD5828281)
		if ddocv.Select != nil {
			for _, x := range ddocv.Setup {
				content += x
			}
		}
		if ddocv.Update != nil {
			for _, x := range ddocv.Update {
				content += x
			}
		}
		if ddocv.Delete != nil {
			for _, x := range ddocv.Delete {
				content += x
			}
		}
		v := crc32.Checksum([]byte(content), crc32q)
		return strconv.Itoa(int(v))
	}
	return ""
}

func (mgr *DefaultViewManager) ParseQuery(query string) (string, []string) {
	re := regexp.MustCompile(`\$\{(.*?)\}`)
	o := re.FindAllStringSubmatch(query, -1)
	var params []string
	for _, x := range o {
		params = append(params, x[1])
	}
	text := re.ReplaceAllString(query, "?")
	return text, params
}

func NewViewManager(dbPath, viewPath, dbName string) *DefaultViewManager {
	mgr := &DefaultViewManager{
		dbPath:   dbPath,
		viewPath: viewPath,
		dbName:   dbName,
	}
	mgr.views = make(map[string]*View)
	mgr.ddocs = make(map[string]*DesignDocument)
	mgr.viewFiles = make(map[string]map[string]bool)
	return mgr
}

type View struct {
	name   string
	ddocID string

	currentSeqID string

	viewFilePath string
	dbFilePath   string
	con          *sql.DB

	setupScripts  []Query
	deleteScripts []Query
	updateScripts []Query
	selectScripts map[string]Query
}

func NewView(dbFilePath, viewFilePath, viewName string, ddoc *DesignDocument, viewm ViewManager) *View {
	view := &View{}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	view.viewFilePath = viewFilePath
	view.dbFilePath = dbFilePath

	view.name = viewName
	view.ddocID = ddoc.ID

	view.setupScripts = *new([]Query)
	view.deleteScripts = *new([]Query)
	view.updateScripts = *new([]Query)
	view.selectScripts = make(map[string]Query)
	designDocView := ddoc.Views[viewName]

	for _, x := range designDocView.Setup {
		text, params := viewm.ParseQuery(x)
		view.setupScripts = append(view.setupScripts, Query{text: text, params: params})
	}
	for _, x := range designDocView.Delete {
		text, params := viewm.ParseQuery(x)
		view.deleteScripts = append(view.deleteScripts, Query{text: text, params: params})
	}
	for _, x := range designDocView.Update {
		text, params := viewm.ParseQuery(x)
		view.updateScripts = append(view.updateScripts, Query{text: text, params: params})
	}

	for k, v := range designDocView.Select {
		text, params := viewm.ParseQuery(v)
		view.selectScripts[k] = Query{text: text, params: params}
	}

	return view
}

func (view *View) Open() error {
	db, err := sql.Open("sqlite3", view.viewFilePath)
	if err != nil {
		return err
	}

	buildSQL := `CREATE TABLE IF NOT EXISTS view_meta (
		Id						INTEGER PRIMARY KEY,
		current_seq_id		  	TEXT,
		next_seq_id		  		TEXT
	) WITHOUT ROWID;

	INSERT INTO view_meta (Id, current_seq_id, next_seq_id) 
		SELECT 1,"", "" WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	
	`

	if _, err = db.Exec(buildSQL); err != nil {
		return err
	}

	absoluteDBPath, err := filepath.Abs(view.dbFilePath)
	if err != nil {
		return err
	}

	_, err = db.Exec("ATTACH DATABASE '" + absoluteDBPath + "' as docsdb;")
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TEMP VIEW latest_changes AS SELECT DISTINCT doc_id FROM docsdb.changes WHERE seq_id > (SELECT current_seq_id FROM view_meta) AND seq_id <= (SELECT next_seq_id FROM view_meta);
		CREATE TEMP VIEW latest_documents AS SELECT d.doc_id, d.version, JSON(d.data) as data FROM docsdb.documents d JOIN (SELECT DISTINCT doc_id FROM latest_changes) c USING(doc_id);
					`)
	if err != nil {
		return err
	}

	for _, x := range view.setupScripts {
		if _, err = db.Exec(x.text); err != nil {
			return err
		}
	}

	sqlGetViewLastSeq := "SELECT current_seq_id FROM view_meta WHERE id = 1"
	row := db.QueryRow(sqlGetViewLastSeq)
	row.Scan(&view.currentSeqID)

	view.con = db

	return err
}

func (view *View) Close() error {
	return view.con.Close()
}

func (view *View) Build(nextSeqID string) error {

	if view.currentSeqID == nextSeqID {
		return nil
	}

	db := view.con
	tx, err := db.Begin()
	defer tx.Rollback()
	if err != nil {
		return err
	}

	sqlUpdateViewMeta := "UPDATE view_meta SET current_seq_id = next_seq_id, next_seq_id = ? "
	if _, err := tx.Exec(sqlUpdateViewMeta, nextSeqID); err != nil {
		panic(err)
	}

	for _, x := range view.deleteScripts {
		values := make([]interface{}, len(x.params))
		for i, p := range x.params {
			if p == "begin_seq_id" {
				values[i] = view.currentSeqID
			}
			if p == "end_seq_id" {
				values[i] = nextSeqID
			}
		}
		if _, err = tx.Exec(x.text, values...); err != nil {
			return err
		}
	}

	for _, x := range view.updateScripts {
		values := make([]interface{}, len(x.params))
		for i, p := range x.params {
			if p == "begin_seq_id" {
				values[i] = view.currentSeqID
			}
			if p == "end_seq_id" {
				values[i] = nextSeqID
			}
		}
		if _, err = tx.Exec(x.text, values...); err != nil {
			return err
		}
	}

	view.currentSeqID = nextSeqID

	tx.Commit()

	return nil
}

func (view *View) Select(name string, values url.Values) ([]byte, error) {

	var rs string
	selectStmt := view.selectScripts[name]
	pValues := make([]interface{}, len(selectStmt.params))
	for i, p := range selectStmt.params {
		pv := values.Get(p)
		if pv != "" {
			pValues[i] = values.Get(p)
		}
	}

	row := view.con.QueryRow(selectStmt.text, pValues...)
	err := row.Scan(&rs)
	if err != nil {
		m, err := regexp.Match("sql: expected \\d+ destination arguments in Scan, not 1", []byte(err.Error()))
		if m {
			return nil, errors.New("view_result_error")
		}
		return nil, err
	}
	return []byte(rs), nil
}

func (view *View) Vacuum() error {
	if _, err := view.con.Exec("VACUUM"); err != nil {
		return err
	}
	return nil
}
