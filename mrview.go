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
	Initialize(designDocs []Document) error
	OpenView(docID, viewName string, designDocumentView DesignDocumentView) error
	GetView(viewName string) (*View, bool)
	SelectView(updateSeqID string, designDoc Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)

	OnDesignDocumentChange(doc Document, qualifiedViewName string) error
	ValidateDesignDocument(doc Document) error
	CalculateSignature(designView DesignDocumentView) string
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

func (mgr *DefaultViewManager) Initialize(designDocs []Document) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	diskViewFiles, err := mgr.ListViewFiles()
	if err != nil {
		panic(err)
	}
	localDBViewFiles, err := mgr.localDB.ListViewFiles(mgr.DBName)
	if err != nil {
		panic(err)
	}

	// cleanup unused files
	for _, diskViewFile := range diskViewFiles {
		found := false
		for _, viewFile := range localDBViewFiles {
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
		designDoc := &DesignDocument{}
		err := json.Unmarshal(x.Data, designDoc)
		if err != nil {
			return err
		}
		mgr.designDocs[x.ID] = designDoc
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
		if strings.HasPrefix(name, mgr.DBName+"$") && strings.HasSuffix(name, dbExt) {
			viewFiles = append(viewFiles, strings.ReplaceAll(name, dbExt, ""))
		}
	}

	return viewFiles, nil
}

func (mgr *DefaultViewManager) OpenView(docID, viewName string, designDocumentView DesignDocumentView) error {
	qualifiedViewName := docID + "$" + viewName
	if _, ok := mgr.views[qualifiedViewName]; ok {
		// view is exists
		return nil
	}

	var currentViewHash, viewFileName string

	currentViewHash, viewFileName = mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)
	newViewHash := mgr.CalculateSignature(designDocumentView)

	if currentViewHash != newViewHash {
		// view content changed
		viewFileName = mgr.DBName + "$" + newViewHash
		mgr.localDB.UpdateView(mgr.DBName, qualifiedViewName, newViewHash, viewFileName)
	}

	view := NewView(mgr.DBName, docID, viewName, &designDocumentView, mgr, mgr.serviceLocator)
	if err := view.Open(); err != nil {
		return err
	}

	mgr.views[qualifiedViewName] = view

	return nil
}

func (mgr *DefaultViewManager) SelectView(updateSeqID string, designDoc Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	designDocID := designDoc.ID
	qualifiedViewName := designDocID + "$" + viewName

	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	update := func() (*View, error) {
		mgr.rwMutex.RUnlock() // remove read lock, if any
		mgr.rwMutex.Lock()    // put write lock

		// in the end
		defer mgr.rwMutex.RLock()  // put read lock back on.
		defer mgr.rwMutex.Unlock() // remove write lock
		// in the end

		err := mgr.OnDesignDocumentChange(designDoc, qualifiedViewName)
		if err != nil {
			return nil, err
		}

		designDoc, _ := mgr.designDocs[designDocID]
		designDocView := designDoc.Views[viewName]
		err = mgr.OpenView(designDoc.ID, viewName, *designDocView)
		if err != nil {
			return nil, err
		}

		view := mgr.views[qualifiedViewName]
		return view, nil
	}

	var err error
	view, ok := mgr.views[qualifiedViewName]
	if !ok {
		// if view not found. try to find and open
		view, err = update()
		if err != nil {
			return nil, err
		}
	}

	if view == nil {
		// no view found
		return nil, ErrViewNotFound
	}

	if stale {
		return view.Select(selectName, values)
	}

	currentDesignDoc := mgr.designDocs[designDocID]
	if designDoc.Version != currentDesignDoc.Version {
		view, err = update()
		if err != nil {
			return nil, err
		}
	}

	if view == nil {
		// no view found
		return nil, ErrViewNotFound
	}

	// refresh view data
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

func (mgr *DefaultViewManager) OnDesignDocumentChange(doc Document, qualifiedViewName string) error {
	var views []string
	if qualifiedViewName == "" {
		designDoc := mgr.designDocs[doc.ID]
		for viewName := range designDoc.Views {
			views = append(views, doc.ID+"$"+viewName)
		}
	} else {
		views = append(views, qualifiedViewName)
	}

	for _, qualifiedViewName := range views {
		// delete current view and it's data file
		if view, ok := mgr.views[qualifiedViewName]; ok {
			view.Close()
		}
		delete(mgr.views, qualifiedViewName)

		localDBViewFileNames, _ := mgr.localDB.ListViewFiles(mgr.DBName)
		_, viewFileName := mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)

		referenceCount := 0
		for _, vFile := range localDBViewFileNames {
			if vFile == viewFileName {
				referenceCount++
			}
		}
		if referenceCount == 1 {
			// delete data file, only if its used by one view
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

func (mgr *DefaultViewManager) CalculateSignature(designDocumentView DesignDocumentView) string {
	content := ""
	crc32q := crc32.MakeTable(0xD5828281)
	if designDocumentView.Select != nil {
		for _, x := range designDocumentView.Setup {
			content += x
		}
	}
	if designDocumentView.Run != nil {
		for _, x := range designDocumentView.Run {
			content += x
		}
	}
	v := crc32.Checksum([]byte(content), crc32q)
	return strconv.Itoa(int(v))
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

func (mgr *DefaultViewManager) ValidateDesignDocument(doc Document) error {
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
	mgr.rwMutex = sync.RWMutex{}

	mgr.serviceLocator = serviceLocator
	mgr.localDB = serviceLocator.GetLocalDB()

	return mgr
}

var viewResultValidation = regexp.MustCompile("sql: expected (\\d+) destination arguments in Scan, not 1")

type View struct {
	name         string
	designDocID  string
	mutex        sync.Mutex
	currentSeqID string

	viewReader chan ViewReader
	viewWriter chan ViewWriter
}

func (view *View) Open() error {
	viewWriter := <- view.viewWriter
	defer func() {
		view.viewWriter <- viewWriter
	}()
	viewWriter.Open()

	// open all readers
	func() {
		readersCount := cap(view.viewReader)
		readers := make([]ViewReader, readersCount)
		for i := 0; i < readersCount; i++ {
			viewReader := <-view.viewReader
			err := viewReader.Open()
			if err != nil {
				viewReader.Close()
				continue
			}
			readers[i] = viewReader
		}
		for _, reader := range readers {
			view.viewReader <- reader
		}
	}()

	return nil
}

func (view *View) Close() error {
	viewWriter := <- view.viewWriter
	viewWriter.Close()

	// close all readers
	func() {
		readersCount := cap(view.viewReader)
		for i := 0; i < readersCount; i++ {
			viewReader := <-view.viewReader
			viewReader.Close()
		}
	}()

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

	viewWriter := <- view.viewWriter
	defer func() {
		view.viewWriter <- viewWriter
	}()

	err := viewWriter.Build(nextSeqID)
	if err != nil {
		return err
	}

	view.currentSeqID = nextSeqID

	return nil
}

func (view *View) Select(name string, values url.Values) ([]byte, error) {
	viewReader := <- view.viewReader
	defer func () {
		view.viewReader <- viewReader
	}()
	return viewReader.Select(name, values)
}

func (view *View) Vacuum() error {
	return nil
}

func NewView(DBName, viewName, docID string, designDocumentView *DesignDocumentView, viewManager ViewManager, serviceLocator ServiceLocator) *View {
	view := &View{}

	view.name = viewName
	view.designDocID = docID

	setupScripts := *new([]Query)
	scripts := *new([]Query)
	selectScripts := make(map[string]Query)
	designDocView := designDocumentView

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

	view.viewReader = make(chan ViewReader, 1)
	view.viewWriter = make(chan ViewWriter, 1)

	view.viewWriter <- serviceLocator.GetViewWriter(DBName, viewName, docID, setupScripts, scripts)
	readersCount := cap(view.viewReader)
	for i := 0; i < readersCount; i++ {
		view.viewReader <- serviceLocator.GetViewReader(DBName, viewName, docID, selectScripts)
	}

	return view
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