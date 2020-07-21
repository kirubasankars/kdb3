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
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type ViewManager interface {
	Initialize(designDocs []*Document) error
	OpenView(viewName string, designDoc *DesignDocument) error
	GetView(viewName string) (*View, bool)
	SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)

	UpdateDesignDocument(doc *Document, qualifiedViewName string) error
	ValidateDesignDocument(doc *Document) error
	CalculateSignature(designView *DesignDocumentView) string
	ParseQueryParams(query string) (string, []string)

	Close() error
	Vacuum() error
}

type DefaultViewManager struct {
	DBName      string
	DBPath      string
	viewDirPath string

	rwMutex        sync.RWMutex
	views          map[string]*View
	designDocs     map[string]*DesignDocument
	localDB        LocalDB
	serviceLocator ServiceLocator
}

func (mgr *DefaultViewManager) Initialize(designDocs []*Document) error {
	mgr.rwMutex = sync.RWMutex{}

	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	// cleanup unused files
	diskViewFiles, err := mgr.listViewFiles()
	if err != nil {
		panic(err)
	}
	viewFiles, err := mgr.localDB.ListViewFiles(mgr.DBName)
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
	for _, x := range designDocs {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(x.Data, ddoc)
		if err != nil {
			return err
		}
		mgr.designDocs[x.ID] = ddoc
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
		if strings.HasPrefix(name, mgr.DBName+"$") && strings.HasSuffix(name, dbExt) {
			viewFiles = append(viewFiles, strings.ReplaceAll(name, dbExt, ""))
		}
	}
	return viewFiles, nil
}

func (mgr *DefaultViewManager) OpenView(viewName string, designDoc *DesignDocument) error {
	qualifiedViewName := designDoc.ID + "$" + viewName
	if _, ok := mgr.views[qualifiedViewName]; ok {
		return nil
	}

	if _, ok := designDoc.Views[viewName]; !ok {
		return nil
	}

	var currentViewHash, viewFileName string

	newHash := mgr.CalculateSignature(designDoc.Views[viewName])
	currentViewHash, viewFileName = mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)

	if currentViewHash != newHash {
		viewFileName = mgr.DBName + "$" + newHash
		mgr.localDB.UpdateView(mgr.DBName, qualifiedViewName, newHash, viewFileName)
	}

	view := NewView(mgr.DBName, viewName, designDoc, mgr, mgr.serviceLocator)
	if err := view.Open(); err != nil {
		return err
	}

	mgr.views[qualifiedViewName] = view

	return nil
}

func (mgr *DefaultViewManager) SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	ddocID := doc.ID
	qualifiedViewName := ddocID + "$" + viewName

	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	update := func() (*View, error) {
		mgr.rwMutex.RUnlock()
		mgr.rwMutex.Lock()

		defer mgr.rwMutex.RLock()
		defer mgr.rwMutex.Unlock()

		err := mgr.UpdateDesignDocument(doc, qualifiedViewName)
		if err != nil {
			return nil, err
		}
		ddoc, _ := mgr.designDocs[ddocID]
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

	ddoc := mgr.designDocs[ddocID]
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
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	for k, v := range mgr.views {
		v.Close()
		delete(mgr.views, k)
	}

	return nil
}

func (mgr *DefaultViewManager) Vacuum() error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	for _, v := range mgr.views {
		v.Vacuum()
	}
	return nil
}

func (mgr *DefaultViewManager) UpdateDesignDocument(doc *Document, viewName string) error {
	var views []string
	if viewName == "" {
		ddoc := mgr.designDocs[doc.ID]
		for viewName := range ddoc.Views {
			views = append(views, doc.ID+"$"+viewName)
		}
	} else {
		views = append(views, viewName)
	}

	for _, qualifiedViewName := range views {
		if view, ok := mgr.views[qualifiedViewName]; ok {
			view.Close()
		}
		delete(mgr.views, qualifiedViewName)

		files, _ := mgr.localDB.ListViewFiles(mgr.DBName)
		_, viewFileName := mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)

		refCount := 0
		for _, vFile := range files {
			if vFile == viewFileName {
				refCount++
			}
		}
		if refCount == 1 {
			os.Remove(path.Join(mgr.viewDirPath, viewFileName+dbExt))
		}
		mgr.localDB.DeleteView(mgr.DBName, qualifiedViewName)
	}

	if doc.Deleted {
		delete(mgr.designDocs, doc.ID)
	} else {
		newDDoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, newDDoc)
		if err != nil {
			panic("invalid_design_document " + doc.ID)
		}
		mgr.designDocs[doc.ID] = newDDoc
	}

	return nil
}

func (mgr *DefaultViewManager) CalculateSignature(designView *DesignDocumentView) string {
	content := ""
	if designView != nil {
		crc32q := crc32.MakeTable(0xD5828281)
		if designView.Select != nil {
			for _, x := range designView.Setup {
				content += x
			}
		}
		if designView.Run != nil {
			for _, x := range designView.Run {
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

func NewViewManager(DBName, DBPath, viewDirPath string, serviceLocator ServiceLocator) *DefaultViewManager {
	mgr := &DefaultViewManager{}
	mgr.DBName = DBName
	mgr.DBPath = DBPath
	mgr.viewDirPath = viewDirPath
	mgr.views = make(map[string]*View)
	mgr.designDocs = make(map[string]*DesignDocument)
	mgr.localDB = serviceLocator.GetLocalDB()
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

	viewReader ViewReader
	viewWriter ViewWriter

	mutex sync.Mutex
}

func (view *View) Open() error {
	view.viewWriter.Open()
	view.viewReader.Open()
	return nil
}

func (view *View) Close() error {
	view.viewReader.Close()
	view.viewWriter.Close()

	return nil
}

func (view *View) Build(nextSeqID string) error {
	if view.currentSeqID >= nextSeqID {
		return nil
	}

	view.mutex.Lock()
	defer view.mutex.Unlock()

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
	return view.viewReader.Select(name, values)
}

func (view *View) Vacuum() error {
	return nil
}

func setupViewDatabase(db *sql.DB, absoluteDatabasePath string) error {
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

func NewView(DBName, viewName string, ddoc *DesignDocument, viewManager ViewManager, serviceLocator ServiceLocator) *View {
	view := &View{}

	if _, ok := ddoc.Views[viewName]; !ok {
		return nil
	}

	view.name = viewName
	view.ddocID = ddoc.ID

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

	view.viewWriter = serviceLocator.GetViewWriter(DBName, viewName, *ddoc, setupScripts, scripts)
	view.viewReader = serviceLocator.GetViewReader(DBName, viewName, *ddoc, selectScripts)

	return view
}
