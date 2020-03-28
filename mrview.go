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
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type ViewManager interface {
	Initialize(dbName, dbPath, viewDirPath string, ddocs []*Document) error
	OpenView(viewName string, ddoc *DesignDocument) error
	GetView(viewName string) (*View, bool)
	SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	Close() error
	Vacuum() error
	UpdateDesignDocument(doc *Document, qualifiedViewName string) error
	ValidateDesignDocument(doc *Document) error
	CalculateSignature(ddocv *DesignDocumentView) string
	ParseQueryParams(query string) (string, []string)
}

type DefaultViewManager struct {
	dbName               string
	viewDirPath          string
	absoluteDatabasePath string

	rwmux          sync.RWMutex
	views          map[string]*View
	ddocs          map[string]*DesignDocument
	localdb        LocalDB
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

	// cleanup unused files
	diskViewFiles, err := mgr.listViewFiles()
	viewFiles, err := mgr.localdb.ListViewFiles(mgr.dbName)
	for _, diskViewFile := range diskViewFiles {
		found := false
		for _, viewFile := range viewFiles {
			if diskViewFile == viewFile {
				found = true
			}
		}
		if !found {
			os.Remove(path.Join(mgr.viewDirPath, diskViewFile+dbExt))
		}
	}

	//load all design docs into memory
	for _, x := range ddocs {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(x.Data, ddoc)
		if err != nil {
			return err
		}
		mgr.ddocs[x.ID] = ddoc
	}

	return nil
}

func (mgr *DefaultViewManager) listViewFiles() ([]string, error) {
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
	qualifiedViewName := ddoc.ID + "$" + viewName
	if _, ok := mgr.views[qualifiedViewName]; ok {
		return nil
	}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	var currentViewHash, viewFileName string

	mgr.localdb.Begin()

	newHash := mgr.CalculateSignature(ddoc.Views[viewName])
	currentViewHash, viewFileName = mgr.localdb.GetViewFileName(mgr.dbName, qualifiedViewName)

	if currentViewHash != newHash {
		viewFileName = mgr.dbName + "$" + newHash
		mgr.localdb.UpdateView(mgr.dbName, qualifiedViewName, newHash, viewFileName)
	}

	viewPath := filepath.Join(mgr.viewDirPath, viewFileName+dbExt)
	viewConnectionString := viewPath + "?_journal=MEMORY&cache=shared&_mutex=no"
	view := mgr.serviceLocator.GetView(viewName, viewFileName, viewPath, viewConnectionString, mgr.absoluteDatabasePath, ddoc, mgr)
	if err := view.Open(); err != nil {
		return err
	}

	mgr.views[qualifiedViewName] = view

	mgr.localdb.Commit()

	return nil
}

func (mgr *DefaultViewManager) SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	ddocID := doc.ID
	qualifiedViewName := ddocID + "$" + viewName

	mgr.rwmux.RLock()
	defer mgr.rwmux.RUnlock()

	update := func() (*View, error) {
		mgr.rwmux.RUnlock()
		mgr.rwmux.Lock()

		defer mgr.rwmux.RLock()
		defer mgr.rwmux.Unlock()

		err := mgr.UpdateDesignDocument(doc, qualifiedViewName)
		if err != nil {
			return nil, err
		}
		ddoc, _ := mgr.ddocs[ddocID]
		err = mgr.OpenView(viewName, ddoc)
		if err != nil {
			return nil, err
		}
		view := mgr.views[qualifiedViewName]
		return view, nil
	}

	var err error
	view, ok := mgr.views[qualifiedViewName]
	if !ok {
		view, err = update()
		if err != nil {
			return nil, err
		}
	}

	if view == nil {
		return nil, ErrViewNotFound
	}

	if stale {
		return view.Select(selectName, values)
	}

	ddoc := mgr.ddocs[ddocID]
	if doc.Version != ddoc.Version {
		view, err = update()
		if err != nil {
			return nil, err
		}
	}

	if view == nil {
		return nil, ErrViewNotFound
	}

	err = view.Build(updateSeqID)
	if err != nil {
		return nil, err
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

func (mgr *DefaultViewManager) UpdateDesignDocument(doc *Document, viewName string) error {
	var views []string
	if viewName == "" {
		ddoc := mgr.ddocs[doc.ID]
		for viewName := range ddoc.Views {
			views = append(views, doc.ID+"$"+viewName)
		}
	} else {
		views = append(views, viewName)
	}

	mgr.localdb.Begin()
	defer mgr.localdb.Rollback()

	for _, qualifiedViewName := range views {
		if view, ok := mgr.views[qualifiedViewName]; ok {
			view.Close()
		}
		delete(mgr.views, qualifiedViewName)

		files, _ := mgr.localdb.ListViewFiles(mgr.dbName)
		_, viewFileName := mgr.localdb.GetViewFileName(mgr.dbName, qualifiedViewName)

		refCount := 0
		for _, vFile := range files {
			if vFile == viewFileName {
				refCount++
			}
		}
		if refCount == 1 {
			os.Remove(path.Join(mgr.viewDirPath, viewFileName+dbExt))
		}
		mgr.localdb.DeleteView(mgr.dbName, qualifiedViewName)
	}

	mgr.localdb.Commit()

	if doc.Deleted {
		delete(mgr.ddocs, doc.ID)
	} else {
		newDDoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, newDDoc)
		if err != nil {
			panic("invalid_design_document " + doc.ID)
		}
		mgr.ddocs[doc.ID] = newDDoc
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
	mgr.localdb = serviceLocator.GetLocalDB()
	mgr.serviceLocator = serviceLocator
	return mgr
}

var viewResultValidation = regexp.MustCompile("sql: expected (\\d+) destination arguments in Scan, not 1")

type View struct {
	name                 string
	ddocID               string
	viewFileName         string
	viewFilePath         string
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

func NewView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager, serviceLocator ServiceLocator) *View {
	view := &View{}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	view.name = viewName
	view.ddocID = ddoc.ID
	view.viewFileName = viewFileName
	view.viewFilePath = viewFilePath
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
