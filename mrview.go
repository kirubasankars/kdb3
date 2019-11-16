package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
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
	SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	Close() error
	Vacuum() error
	UpdateDesignDocument(doc *Document) error
	ValidateDesignDocument(doc *Document) error
	CalculateSignature(ddocv *DesignDocumentView) string
	ParseQueryParams(query string) (string, []string)
}

type DefaultViewManager struct {
	viewPath             string
	absoluteDatabasePath string
	dbName               string
	views                map[string]*View
	ddocs                map[string]*DesignDocument

	viewFiles      map[string]map[string]bool
	serviceLocator ServiceLocator
	rwmux          sync.RWMutex
}

func (mgr *DefaultViewManager) SetupViews(db *Database) error {
	ddoc := &DesignDocument{}
	ddoc.ID = "_design/_views"
	ddoc.Views = make(map[string]*DesignDocumentView)
	ddv := &DesignDocumentView{}
	ddv.Setup = append(ddv.Setup, "CREATE TABLE IF NOT EXISTS all_docs (key, value, doc_id,  PRIMARY KEY(key)) WITHOUT ROWID")
	ddv.Delete = append(ddv.Delete, "DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes)")
	ddv.Update = append(ddv.Update, "INSERT INTO all_docs (key, value, doc_id) SELECT doc_id, JSON_OBJECT('rev',JSON_EXTRACT(data, '$._rev')), doc_id FROM latest_documents")
	ddv.Select = make(map[string]string)
	ddv.Select["default"] = "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) WHERE (${key} IS NULL or key = ${key})"
	ddv.Select["with_docs"] = "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('id', doc_id, 'key', key, 'value', JSON(value), 'doc', JSON((SELECT data FROM docsdb.documents WHERE doc_id = o.doc_id)))),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) o WHERE (${key} IS NULL or key = ${key})"

	ddoc.Views["_all_docs"] = ddv

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
	qualifiedViewName := ddoc.ID + "$" + viewName
	if _, ok := mgr.views[qualifiedViewName]; ok {
		return nil
	}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	viewPath := filepath.Join(mgr.viewPath, mgr.dbName+"$"+mgr.CalculateSignature(ddoc.Views[viewName])+dbExt)
	viewConnectionString := viewPath + "?_journal=MEMORY"

	view := mgr.serviceLocator.GetView(viewName, viewConnectionString, mgr.absoluteDatabasePath, ddoc, mgr)
	if err := view.Open(); err != nil {
		return err
	}

	mgr.views[qualifiedViewName] = view

	return nil
}

func (mgr *DefaultViewManager) BuildView(qualifiedViewName string, nextSeqID string) error {
	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()
	view, ok := mgr.views[qualifiedViewName]
	if !ok {
		return ErrViewNotFound
	}
	return view.Build(nextSeqID)
}

func (mgr *DefaultViewManager) SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	ddocID := doc.ID
	qualifiedViewName := ddocID + "$" + viewName

	mgr.rwmux.RLock()
	unlocked := false
	RUnlock := func() {
		if !unlocked {
			mgr.rwmux.RUnlock()
		}
		unlocked = true
	}
	ResetRUnlock := func() {
		unlocked = false
	}
	defer RUnlock()

	view, ok := mgr.views[qualifiedViewName]

	if !ok {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, ddoc)
		if err != nil {
			panic("invalid_design_document " + ddocID)
		}

		RUnlock()
		ResetRUnlock()
		err = mgr.OpenView(viewName, ddoc)
		if err != nil {
			return nil, err
		}
		mgr.rwmux.RLock()

		view = mgr.views[qualifiedViewName]
	}

	if view == nil {
		return nil, ErrViewNotFound
	}

	if !stale {
		ddoc, ok := mgr.ddocs[ddocID]

		if !ok || doc.Version != ddoc.Version {
			RUnlock()
			ResetRUnlock()
			err := mgr.UpdateDesignDocument(doc)
			if err != nil {
				return nil, err
			}

			err = mgr.OpenView(viewName, ddoc)
			if err != nil {
				return nil, err
			}
			mgr.rwmux.RLock()
		}

		RUnlock()
		ResetRUnlock()
		err := mgr.BuildView(qualifiedViewName, updateSeqID)
		if err != nil {
			return nil, err
		}
		mgr.rwmux.RLock()
	}

	view = mgr.views[qualifiedViewName]

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

func (mgr *DefaultViewManager) Vacuum() error {
	mgr.rwmux.RLock()
	defer mgr.rwmux.RUnlock()
	for _, v := range mgr.views {
		v.Vacuum()
	}
	return nil
}

func (mgr *DefaultViewManager) UpdateDesignDocument(doc *Document) error {
	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()

	ddocID := doc.ID
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

	if doc.Deleted {
		delete(mgr.ddocs, ddocID)
	} else {
		mgr.ddocs[ddocID] = newDDoc
	}

	return nil
}

func (mgr *DefaultViewManager) ValidateDesignDocument(doc *Document) error {
	newDDoc := &DesignDocument{}
	err := json.Unmarshal(doc.Data, newDDoc)
	if err != nil {
		panic("invalid_design_document " + doc.ID)
	}

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}

	tx, _ := db.Begin()
	defer tx.Rollback()
	defer db.Close()

	_, err = tx.Exec("CREATE table latest_changes(doc_id); CREATE table latest_documents (doc_id, version, data);")

	var sqlErr string = ""

	for _, v := range newDDoc.Views {
		for _, x := range v.Setup {
			_, err := tx.Exec(x)
			if err != nil {
				sqlErr += fmt.Sprintf("%s: %s ;", x, err.Error())
			}
		}

		if sqlErr != "" {
			break
		}

		for _, x := range v.Delete {
			_, err := tx.Exec(x)
			if err != nil {
				sqlErr += fmt.Sprintf("%s: %s ;", x, err.Error())
			}
		}

		if sqlErr != "" {
			break
		}

		for _, x := range v.Update {
			_, err := tx.Exec(x)
			if err != nil {
				sqlErr += fmt.Sprintf("%s: %s ;", x, err.Error())
			}
		}

		if sqlErr != "" {
			break
		}
	}

	if sqlErr != "" {
		return fmt.Errorf("%s : %w", sqlErr, ErrInvalidSQLStmt)
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

func (mgr *DefaultViewManager) ParseQueryParams(query string) (string, []string) {
	re := regexp.MustCompile(`\$\{(.*?)\}`)
	o := re.FindAllStringSubmatch(query, -1)
	var params []string
	for _, x := range o {
		params = append(params, x[1])
	}
	text := re.ReplaceAllString(query, "?")
	return text, params
}

func NewViewManager(dbName, absoluteDatabasePath, viewPath string, serviceLocator ServiceLocator) *DefaultViewManager {
	mgr := &DefaultViewManager{
		absoluteDatabasePath: absoluteDatabasePath,
		viewPath:             viewPath,
		dbName:               dbName,
	}
	mgr.views = make(map[string]*View)
	mgr.ddocs = make(map[string]*DesignDocument)
	mgr.viewFiles = make(map[string]map[string]bool)
	mgr.serviceLocator = serviceLocator
	return mgr
}

type View struct {
	name   string
	ddocID string

	currentSeqID string

	connectionString     string
	absoluteDatabasePath string
	con                  *sql.DB

	setupScripts  []Query
	deleteScripts []Query
	updateScripts []Query
	selectScripts map[string]Query
}

func NewView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View {
	view := &View{}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	view.connectionString = connectionString
	view.absoluteDatabasePath = absoluteDatabasePath

	view.name = viewName
	view.ddocID = ddoc.ID

	view.setupScripts = *new([]Query)
	view.deleteScripts = *new([]Query)
	view.updateScripts = *new([]Query)
	view.selectScripts = make(map[string]Query)
	designDocView := ddoc.Views[viewName]

	for _, x := range designDocView.Setup {
		text, params := viewManager.ParseQueryParams(x)
		view.setupScripts = append(view.setupScripts, Query{text: text, params: params})
	}
	for _, x := range designDocView.Delete {
		text, params := viewManager.ParseQueryParams(x)
		view.deleteScripts = append(view.deleteScripts, Query{text: text, params: params})
	}
	for _, x := range designDocView.Update {
		text, params := viewManager.ParseQueryParams(x)
		view.updateScripts = append(view.updateScripts, Query{text: text, params: params})
	}

	for k, v := range designDocView.Select {
		text, params := viewManager.ParseQueryParams(v)
		view.selectScripts[k] = Query{text: text, params: params}
	}

	return view
}

func (view *View) Open() error {
	db, err := sql.Open("sqlite3", view.connectionString)
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

	_, err = db.Exec("ATTACH DATABASE '" + view.absoluteDatabasePath + "' as docsdb;")
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
		panic(err)
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

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	return nil
}

var viewResultValidation = regexp.MustCompile("sql: expected (\\d+) destination arguments in Scan, not 1")

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
		o := viewResultValidation.FindAllStringSubmatch(err.Error(), -1)
		if len(o) > 0 {
			return nil, fmt.Errorf("%s: %w", fmt.Sprintf("select have %s, want 1 column", o[0][1]), ErrViewResult)
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
