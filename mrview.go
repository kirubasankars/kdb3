package main

import (
	"database/sql"
	"encoding/json"
	"errors"
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
	Initialize(dbName, dbPath, viewDirPath string, ddocs []*Document) error
	ListViewFiles() ([]string, error)
	OpenView(viewName string, ddoc *DesignDocument) error
	GetView(viewName string) (*View, bool)
	SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	Close() error
	Vacuum() error
	UpdateDesignDocument(doc *Document) error
	ValidateDesignDocument(doc *Document) error
	CalculateSignature(ddocv *DesignDocumentView) string
	ParseQueryParams(query string) (string, []string)
}

type DefaultViewManager struct {
	dbName               string
	viewDirPath          string
	absoluteDatabasePath string

	rwmux     sync.RWMutex
	views     map[string]*View
	ddocs     map[string]*DesignDocument
	viewFiles map[string]map[string]bool

	serviceLocator ServiceLocator
}

func (mgr *DefaultViewManager) Initialize(dbName, dbPath, viewDirPath string, ddocs []*Document) error {
	mgr.rwmux = sync.RWMutex{}
	mgr.dbName = dbName
	mgr.viewDirPath = viewDirPath
	absoluteDBPath, err := filepath.Abs(dbPath)
	if err != nil {
		panic(err)
	}
	mgr.absoluteDatabasePath = absoluteDBPath

	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()

	//load all design docs into memory
	for _, x := range ddocs {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(x.Data, ddoc)
		if err != nil {
			return err
		}
		mgr.ddocs[x.ID] = ddoc
	}

	//load all view files names for this database into memory
	viewFiles, _ := mgr.ListViewFiles()
	for _, x := range viewFiles {
		if _, ok := mgr.viewFiles[x]; !ok {
			mgr.viewFiles[x] = make(map[string]bool)
		}
	}

	// calculate view file with views reference counter
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

	// delete unused view files, if reference counter is zero
	for fileName, x := range mgr.viewFiles {
		if len(x) <= 0 {
			delete(mgr.viewFiles, fileName)
			os.Remove(filepath.Join(mgr.viewDirPath, fileName+dbExt))
		}
	}

	return nil
}

func (mgr *DefaultViewManager) ListViewFiles() ([]string, error) {
	list, err := ioutil.ReadDir(mgr.viewDirPath)
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

	viewPath := filepath.Join(mgr.viewDirPath, mgr.dbName+"$"+mgr.CalculateSignature(ddoc.Views[viewName])+dbExt)
	viewConnectionString := viewPath + "?_journal=MEMORY&cache=shared&_mutex=no"

	view := mgr.serviceLocator.GetView(viewName, viewConnectionString, mgr.absoluteDatabasePath, ddoc, mgr)
	if err := view.Open(); err != nil {
		return err
	}

	mgr.views[qualifiedViewName] = view

	return nil
}

func (mgr *DefaultViewManager) SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	ddocID := doc.ID
	qualifiedViewName := ddocID + "$" + viewName

	unlocked := false
	ReadUnlock := func() {
		if !unlocked {
			mgr.rwmux.RUnlock()
		}
		unlocked = true
	}
	ResetReadUnlock := func() {
		unlocked = false
		mgr.rwmux.RLock()
	}

	ResetReadUnlock()
	defer ReadUnlock()

	view, ok := mgr.views[qualifiedViewName]

	if !ok {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, ddoc)
		if err != nil {
			panic("invalid_design_document " + ddocID)
		}

		ReadUnlock()
		err = mgr.OpenView(viewName, ddoc)
		if err != nil {
			return nil, err
		}
		ResetReadUnlock()
		view = mgr.views[qualifiedViewName]
	}

	if view == nil {
		return nil, ErrViewNotFound
	}

	if !stale {
		ddoc, ok := mgr.ddocs[ddocID]

		if !ok || doc.Version != ddoc.Version {
			ReadUnlock()
			err := mgr.UpdateDesignDocument(doc)
			if err != nil {
				return nil, err
			}

			ddoc, ok = mgr.ddocs[ddocID]
			err = mgr.OpenView(viewName, ddoc)
			if err != nil {
				return nil, err
			}

			view = mgr.views[qualifiedViewName]

			ResetReadUnlock()
		}

		err := view.Build(updateSeqID)
		if err != nil {
			return nil, err
		}
	}

	return view.Select(selectName, values)
}

func (mgr *DefaultViewManager) Close() error {
	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()

	for k, v := range mgr.views {
		v.Close()
		delete(mgr.views, k)
	}

	return nil
}

func (mgr *DefaultViewManager) Vacuum() error {
	mgr.rwmux.Lock()
	defer mgr.rwmux.Unlock()
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

			if currentDDoc, ok := mgr.ddocs[ddocID]; ok {
				if cddv, _ := currentDDoc.Views[vname]; cddv != nil {
					currentViewFile = mgr.dbName + "$" + mgr.CalculateSignature(cddv)
				}
			}
			if newViewFile == currentViewFile {
				continue
			}

			if _, ok := mgr.viewFiles[newViewFile]; !ok {
				mgr.viewFiles[newViewFile] = make(map[string]bool)
			}
			mgr.viewFiles[newViewFile][qualifiedViewName] = true

			if _, ok := mgr.views[qualifiedViewName]; ok {
				mgr.views[qualifiedViewName].Close()
				delete(mgr.views, qualifiedViewName)
			}

			//To takecare old one
			if currentViewFile != "" && len(mgr.viewFiles[currentViewFile]) <= 0 {
				delete(mgr.viewFiles, currentViewFile)
				os.Remove(filepath.Join(mgr.viewDirPath, currentViewFile+dbExt))
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
					os.Remove(filepath.Join(mgr.viewDirPath, currentViewFile+dbExt))
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

func (mgr *DefaultViewManager) CalculateSignature(ddocv *DesignDocumentView) string {
	content := ""
	if ddocv != nil {
		crc32q := crc32.MakeTable(0xD5828281)
		if ddocv.Select != nil {
			for _, x := range ddocv.Setup {
				content += x
			}
		}
		if ddocv.Run != nil {
			for _, x := range ddocv.Run {
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

	_, err = tx.Exec("CREATE VIEW latest_changes (doc_id, deleted) AS select '', 0 as doc_id;")
	if err != nil {
		return err
	}
	_, err = tx.Exec("CREATE VIEW latest_documents (doc_id, version, deleted, data) AS select '' as doc_id, 1 as version, 0, '{}' as data ;")
	if err != nil {
		return err
	}
	_, err = tx.Exec("CREATE VIEW documents (doc_id, version, deleted, data) AS select '' as doc_id, 1 as version, 0, '{}' as data ;")
	if err != nil {
		return err
	}
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

		for _, x := range v.Run {
			_, err := tx.Exec(x)
			if err != nil {
				sqlErr += fmt.Sprintf("%s: %s ;", x, err.Error())
			}
		}

		if sqlErr != "" {
			break
		}
	}

	_, err = tx.Exec("SELECT * FROM latest_changes WHERE 1 = 2")
	if err != nil {
		return errors.New("your script can't drop latest_changes")
	}
	_, err = tx.Exec("SELECT * FROM latest_documents WHERE 1 = 2")
	if err != nil {
		return errors.New("your script can't drop latest_documents")
	}
	_, err = tx.Exec("SELECT * FROM documents WHERE 1 = 2")
	if err != nil {
		return errors.New("your script can't drop documents")
	}

	if sqlErr != "" {
		return fmt.Errorf("%s : %w", sqlErr, ErrInvalidSQLStmt)
	}

	return nil
}

func (mgr *DefaultViewManager) GetView(viewName string) (*View, bool) {
	if view, ok := mgr.views[viewName]; ok {
		return view, true
	}
	return nil, false
}

func NewViewManager(serviceLocator ServiceLocator) *DefaultViewManager {
	mgr := &DefaultViewManager{}
	mgr.views = make(map[string]*View)
	mgr.ddocs = make(map[string]*DesignDocument)
	mgr.viewFiles = make(map[string]map[string]bool)
	mgr.serviceLocator = serviceLocator
	return mgr
}

var viewResultValidation = regexp.MustCompile("sql: expected (\\d+) destination arguments in Scan, not 1")

type View struct {
	name                 string
	ddocID               string
	absoluteDatabasePath string

	currentSeqID string

	viewReaderPool ViewReaderPool
	viewWriter     ViewWriter

	mux sync.Mutex
}

func (view *View) Open() error {
	view.viewWriter.Open()
	view.viewReaderPool.Open()
	return nil
}

func (view *View) Close() error {
	view.viewReaderPool.Close()
	view.viewWriter.Close()
	return nil
}

func (view *View) Build(nextSeqID string) error {
	if view.currentSeqID >= nextSeqID {
		return nil
	}

	view.mux.Lock()
	defer view.mux.Unlock()

	if view.currentSeqID >= nextSeqID {
		return nil
	}

	err := view.viewWriter.Build(nextSeqID)
	if err != nil {
		return err
	}

	view.currentSeqID = nextSeqID

	return nil
}

func (view *View) Select(name string, values url.Values) ([]byte, error) {
	viewReader := view.viewReaderPool.Borrow()
	defer view.viewReaderPool.Return(viewReader)
	return viewReader.Select(name, values)
}

func (view *View) Vacuum() error {
	return nil
}

func setupDatabase(db *sql.DB, absoluteDatabasePath string) error {
	_, err := db.Exec("ATTACH DATABASE 'file://" + absoluteDatabasePath + "?mode=ro' as docsdb;")
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TEMP VIEW latest_changes AS SELECT doc_id, deleted FROM docsdb.documents INDEXED BY idx_changes WHERE seq_id > (SELECT current_seq_id FROM view_meta) AND seq_id <= (SELECT next_seq_id FROM view_meta);
		CREATE TEMP VIEW latest_documents AS SELECT doc_id, version, kind, deleted, JSON(data) as data FROM docsdb.documents WHERE seq_id > (SELECT current_seq_id FROM view_meta) AND seq_id <= (SELECT next_seq_id FROM view_meta);
		CREATE TEMP VIEW documents AS SELECT doc_id, version, kind, deleted, JSON(data) as data FROM docsdb.documents;
	`)

	if err != nil {
		return err
	}
	return nil
}

func NewView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager, serviceLocator ServiceLocator) *View {
	view := &View{}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	view.name = viewName
	view.ddocID = ddoc.ID
	view.absoluteDatabasePath = absoluteDatabasePath

	setupScripts := *new([]Query)
	scripts := *new([]Query)
	selectScripts := make(map[string]Query)
	designDocView := ddoc.Views[viewName]

	for _, text := range designDocView.Setup {
		setupScripts = append(setupScripts, Query{text: text})
	}
	for _, text := range designDocView.Run {
		scripts = append(scripts, Query{text: text})
	}
	for k, v := range designDocView.Select {
		text, params := viewManager.ParseQueryParams(v)
		selectScripts[k] = Query{text: text, params: params}
	}

	view.viewWriter = NewViewWriter(connectionString+"&mode=rwc", absoluteDatabasePath, setupScripts, scripts)
	view.viewReaderPool = NewViewReaderPool(connectionString+"&mode=ro", absoluteDatabasePath, 4, serviceLocator, selectScripts)

	return view
}
