package main

import (
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

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type ViewManager interface {
	Initialize(designDocs []Document) error
	OpenView(docID, viewName string, designDocumentView DesignDocumentView) error
	GetView(viewName string) (*View, bool)
	SelectView(updateSeqID string, designDoc Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	SQL(updateSeqID string, doc Document, viewName string) ([]byte, error)

	DeleteViewsIfRemoved(doc Document)
	ValidateDesignDocument(doc Document) error
	CalculateSignature(designView DesignDocumentView) string
	ParseQueryParams(query string) (string, []string)

	Close(closeChannel bool) error
	ReinitializeViews() error
	Vacuum() error
}

type DefaultViewManager struct {
	DBName      string
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
			err = os.Remove(path.Join(mgr.viewDirPath, diskViewFile+dbExt))
			if err != nil {
				panic(err)
			}
		}
	}

	//load all design docs into memory
	for _, x := range designDocs {
		designDoc := &DesignDocument{}
		doc, _ := ParseDocument(x.Data)
		err := json.Unmarshal(doc.Data, designDoc)
		designDoc.Hash = doc.Hash
		designDoc.Version = doc.Version
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
	var currentViewHash, viewFileName, currentViewFileName string
	var view *View
	var ok bool

	qualifiedViewName := docID + "$" + viewName
	currentViewHash, currentViewFileName = mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)
	newViewHash := mgr.CalculateSignature(designDocumentView)

	if currentViewHash != newViewHash {
		// view content changed
		viewFileName = mgr.DBName + "$" + newViewHash
		mgr.localDB.UpdateView(mgr.DBName, qualifiedViewName, newViewHash, viewFileName)
	}

	if view, ok = mgr.views[qualifiedViewName]; ok {
		if currentViewHash != newViewHash {
			view.Close(false) // safe close readers and writer

			setupScripts := *new([]Query)
			runScripts := *new([]Query)
			selectScripts := make(map[string]Query)
			designDocView := designDocumentView

			for _, text := range designDocView.Setup {
				setupScripts = append(setupScripts, Query{text: text})
			}
			for _, text := range designDocView.Run {
				runScripts = append(runScripts, Query{text: text})
			}
			for k, v := range designDocView.Select {
				text, params := mgr.ParseQueryParams(v)
				selectScripts[k] = Query{text: text, params: params}
			}

			view.setupScripts = setupScripts
			view.runScripts = runScripts
			view.selectScripts = selectScripts

			view.ReInitialize() // safe initialize to writer and readers
			mgr.deleteViewFileIfNoReference(currentViewFileName)
		}
	} else {
		view = NewView(mgr.DBName, docID, viewName, &designDocumentView, mgr, mgr.serviceLocator)
		if err := view.Open(); err != nil {
			return err
		}
		mgr.views[qualifiedViewName] = view
	}
	return nil
}

func (mgr *DefaultViewManager) SelectView(updateSeqID string, doc Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	designDocID := doc.ID
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

		designDoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, designDoc)
		version, hash, _ := SplitRev(designDoc.Rev)
		designDoc.Version = version
		designDoc.Hash = hash
		if err != nil {
			panic("invalid_design_document " + doc.ID)
		}
		if _, ok := mgr.designDocs[doc.ID]; !ok {
			// document is new
			mgr.designDocs[doc.ID] = designDoc
		}

		designDocView := designDoc.Views[viewName]
		if designDocView == nil {
			return nil, ErrViewNotFound
		}
		err = mgr.OpenView(designDoc.ID, viewName, *designDocView)
		mgr.designDocs[doc.ID] = designDoc

		if err != nil {
			return nil, err
		}

		view := mgr.views[qualifiedViewName]

		//TODO: duplicate code
		selectScripts := make(map[string]Query)
		for k, v := range designDocView.Select {
			text, params := mgr.ParseQueryParams(v)
			selectScripts[k] = Query{text: text, params: params}
		}
		view.selectScripts = selectScripts

		return view, nil
	}

	var err error
	view, ok := mgr.views[qualifiedViewName]
	if !ok {
		// if view not found. try to find and open
		// new doc handled here
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
	if doc.Version != currentDesignDoc.Version {
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

func (mgr *DefaultViewManager) SQL(fromSeqID string, doc Document, viewName string) ([]byte, error) {
	designDocID := doc.ID
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

		designDoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, designDoc)
		if err != nil {
			panic("invalid_design_document " + doc.ID)
		}
		if _, ok := mgr.designDocs[doc.ID]; !ok {
			// document is new
			mgr.designDocs[doc.ID] = designDoc
		}

		designDocView := designDoc.Views[viewName]
		err = mgr.OpenView(designDoc.ID, viewName, *designDocView)
		mgr.designDocs[doc.ID] = designDoc
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
		// new doc handled here
		view, err = update()
		if err != nil {
			return nil, err
		}
	}

	if view == nil {
		// no view found
		return nil, ErrViewNotFound
	}

	return view.SQL(fromSeqID)
}

func (mgr *DefaultViewManager) Close(closeChannel bool) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	for k, v := range mgr.views {
		err := v.Close(closeChannel)
		if err != nil {
			return err
		}
		if closeChannel {
			delete(mgr.views, k)
		}
	}

	return nil
}

func (mgr *DefaultViewManager) ReinitializeViews() error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	for _, v := range mgr.views {
		v.ReInitialize()
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

func (mgr *DefaultViewManager) deleteViews(qualifiedViewNames []string) {

	for _, qualifiedViewName := range qualifiedViewNames {
		// delete current view and it's data file

		if view, ok := mgr.views[qualifiedViewName]; ok {
			// safe close all readers and writer
			view.Close(true)
		}
		delete(mgr.views, qualifiedViewName)

		_, viewFileName := mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)
		mgr.localDB.DeleteView(mgr.DBName, qualifiedViewName)

		mgr.deleteViewFileIfNoReference(viewFileName)
	}
}

func (mgr *DefaultViewManager) deleteViewFileIfNoReference(viewFileName string) {
	if viewFileName == "" {
		return
	}
	localDBViewFileNames, _ := mgr.localDB.ListViewFiles(mgr.DBName)
	referenceCount := 0
	for _, vFile := range localDBViewFileNames {
		if vFile == viewFileName {
			referenceCount++
		}
	}
	if referenceCount <= 0 {
		// delete data file, only if its used by one view
		os.Remove(path.Join(mgr.viewDirPath, viewFileName+dbExt))
	}
}

func (mgr *DefaultViewManager) DeleteViewsIfRemoved(doc Document) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	if doc.Deleted {
		if designDoc, ok := mgr.designDocs[doc.ID]; ok {
			var views []string
			for viewName := range designDoc.Views {
				qualifiedViewName := doc.ID + "$" + viewName
				views = append(views, qualifiedViewName)
			}
			mgr.deleteViews(views)
			delete(mgr.designDocs, doc.ID)
		}
	} else {
		// on document update, find any deleted views and clean them
		currentDesignDoc := mgr.designDocs[doc.ID]
		newDesignDoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, newDesignDoc)
		if err != nil {
			panic("invalid_design_document " + doc.ID)
		}

		var deletedViews []string
		for viewName := range currentDesignDoc.Views {
			if _, ok := newDesignDoc.Views[viewName]; !ok {
				qualifiedViewName := doc.ID + "$" + viewName
				deletedViews = append(deletedViews, qualifiedViewName)
			}
		}
		if len(deletedViews) > 0 {
			mgr.deleteViews(deletedViews)
		}
	}
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
	re := regexp.MustCompile(`\${(.*?)}`)
	o := re.FindAllStringSubmatch(query, -1)
	var params []string
	for _, x := range o {
		params = append(params, x[1])
	}
	text := re.ReplaceAllString(query, "?")
	return text, params
}

func (mgr *DefaultViewManager) ValidateDesignDocument(doc Document) error {
	var invalidKeywords = []string{
		"PRAGMA", "ALTER", "ATTACH", "TRANSACTION", "DETACH", "DROP", "EXPLAIN", "REINDEX", "SAVEPOINT", "CONFLICT", "UPDATE", "VACUUM",
	}
	newDDoc := &DesignDocument{}
	err := json.Unmarshal(doc.Data, newDDoc)
	if err != nil {
		panic("invalid_design_document " + doc.ID)
	}

	db, err := sqlite3.Open(":memory:")
	if err != nil {
		return err
	}

	err = db.WithTx(func() error {
		err = db.Exec("CREATE VIEW latest_changes (doc_id, deleted) AS select '', 0 as doc_id;")
		if err != nil {
			return err
		}
		err = db.Exec("CREATE VIEW latest_documents (doc_id, rev, deleted, data) AS select '' as doc_id, '1-xxxxxxxxxxxxxx' as rev, 0, '{}' as data;")
		if err != nil {
			return err
		}
		err = db.Exec("CREATE VIEW documents (doc_id, rev, deleted, data) AS select '' as doc_id, '1-xxxxxxxxxxxxxx' as rev, 0, '{}' as data;")
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	var sqlErr = ""
	for _, v := range newDDoc.Views {
		for _, x := range v.Setup {
			for _, invalidKeyword := range invalidKeywords {
				query := strings.ToLower(x)
				if strings.Contains(query, strings.ToLower(invalidKeyword)) {
					sqlErr += fmt.Sprintf("%s: %s; ", invalidKeyword, "invalid keyword")
				}
			}

			if sqlErr != "" {
				return errors.New(sqlErr)
			}

			err := db.Exec(x)
			if err != nil {
				sqlErr += fmt.Sprintf("%s: %s; ", x, err.Error())
			}
		}

		if sqlErr != "" {
			break
		}

		for _, x := range v.Run {
			for _, invalidKeyword := range invalidKeywords {
				query := strings.ToLower(x)
				if strings.Contains(query, " "+strings.ToLower(invalidKeyword)+" ") {
					sqlErr += fmt.Sprintf("%s: %s; ", invalidKeyword, "invalid keyword")
				}
			}

			if sqlErr != "" {
				return errors.New(sqlErr)
			}

			err := db.Exec(x)
			if err != nil {
				sqlErr += fmt.Sprintf("%s: %s; ", x, err.Error())
			}
		}

		if sqlErr != "" {
			break
		}
	}

	err = db.Exec("SELECT * FROM latest_changes WHERE 1 = 2")
	if err != nil {
		return errors.New("your script can't drop latest_changes")
	}
	err = db.Exec("SELECT * FROM latest_documents WHERE 1 = 2")
	if err != nil {
		return errors.New("your script can't drop latest_documents")
	}
	err = db.Exec("SELECT * FROM documents WHERE 1 = 2")
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

func NewViewManager(DBName, viewDirPath string, serviceLocator ServiceLocator) *DefaultViewManager {
	mgr := &DefaultViewManager{}

	mgr.DBName = DBName
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
	DBName       string
	designDocID  string
	currentSeqID string

	viewReader chan ViewReader
	viewWriter chan ViewWriter

	serviceLocator ServiceLocator

	setupScripts  []Query
	runScripts    []Query
	selectScripts map[string]Query
}

func (view *View) ReInitialize() error {
	viewWriter := view.serviceLocator.GetViewWriter(view.DBName, view.name, view.designDocID, view.setupScripts, view.runScripts)
	viewWriter.Open()
	view.viewWriter <- viewWriter

	readersCount := cap(view.viewReader)
	for i := 0; i < readersCount; i++ {
		viewReader := view.serviceLocator.GetViewReader(view.DBName, view.name, view.designDocID, view.setupScripts, view.selectScripts)
		viewReader.Open()
		view.viewReader <- viewReader
	}

	return nil
}

func (view *View) Open() error {
	viewWriter := <-view.viewWriter
	defer func() {
		view.viewWriter <- viewWriter
	}()
	if err := viewWriter.Open(); err != nil {
		return err
	}

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

func (view *View) Close(closeChannel bool) error {
	viewWriter := <-view.viewWriter
	err := viewWriter.Close()
	if err != nil {
		return err
	}

	var readerError error
	// safe close all readers
	func() {
		readersCount := cap(view.viewReader)
		for i := 0; i < readersCount; i++ {
			viewReader := <-view.viewReader
			err = viewReader.Close()
			if err != nil {
				readerError = err
			}
		}
	}()

	if readerError != nil {
		return readerError
	}

	if closeChannel {
		close(view.viewWriter)
		close(view.viewReader)
	}

	return nil
}

func (view *View) Build(nextSeqID string) error {
	if view.currentSeqID >= nextSeqID {
		return nil
	}

	viewWriter, ok := <-view.viewWriter
	if !ok {
		return ErrViewNotFound
	}
	defer func() {
		view.viewWriter <- viewWriter
	}()

	if view.currentSeqID >= nextSeqID {
		return nil
	}

	err := viewWriter.Build(nextSeqID)
	if err != nil {
		return err
	}

	view.currentSeqID = nextSeqID

	return nil
}

func (view *View) Select(name string, values url.Values) ([]byte, error) {
	viewReader, ok := <-view.viewReader
	if !ok {
		return nil, ErrViewNotFound
	}
	defer func() {
		view.viewReader <- viewReader
	}()
	return viewReader.Select(name, values)
}

func (view *View) SQL(fromSeqID string) ([]byte, error) {
	vs := view.serviceLocator.GetViewSQLBuilder(view.DBName, view.designDocID, view.name, view.setupScripts, view.runScripts)
	vs.Open()
	return vs.SQL(fromSeqID)
}

func (view *View) Vacuum() error {
	return nil
}

func NewView(DBName, viewName, docID string, designDocumentView *DesignDocumentView, viewManager ViewManager, serviceLocator ServiceLocator) *View {
	view := &View{}

	view.name = viewName
	view.designDocID = docID
	view.DBName = DBName
	view.serviceLocator = serviceLocator
	setupScripts := *new([]Query)
	runScripts := *new([]Query)
	selectScripts := make(map[string]Query)
	designDocView := designDocumentView

	for _, text := range designDocView.Setup {
		setupScripts = append(setupScripts, Query{text: text})
	}
	for _, text := range designDocView.Run {
		runScripts = append(runScripts, Query{text: text})
	}
	for k, v := range designDocView.Select {
		text, params := viewManager.ParseQueryParams(v)
		selectScripts[k] = Query{text: text, params: params}
	}

	view.setupScripts = setupScripts
	view.runScripts = runScripts
	view.selectScripts = selectScripts

	view.viewReader = make(chan ViewReader, 1)
	view.viewWriter = make(chan ViewWriter, 1)

	view.viewWriter <- view.serviceLocator.GetViewWriter(view.DBName, view.name, view.designDocID, view.setupScripts, view.runScripts)
	readersCount := cap(view.viewReader)
	for i := 0; i < readersCount; i++ {
		view.viewReader <- view.serviceLocator.GetViewReader(view.DBName, view.name, view.designDocID, view.setupScripts, view.selectScripts)
	}

	return view
}

func setupViewDatabase(db *sqlite3.Conn, absoluteDatabasePath string) error {
	err := db.Exec("ATTACH DATABASE 'file:" + absoluteDatabasePath + "?cache=shared&mode=ro' as docsdb;")
	if err != nil {
		return err
	}

	err = db.Exec(`
		CREATE TEMP VIEW latest_changes AS SELECT doc_id, deleted, seq_id FROM docsdb.documents INDEXED BY idx_changes WHERE seq_id > (SELECT current_seq_id FROM view_meta) AND seq_id <= (SELECT next_seq_id FROM view_meta);
		CREATE TEMP VIEW latest_documents AS SELECT doc_id, printf('%d-%s', version, hash) as rev, deleted, data, seq_id FROM docsdb.documents WHERE seq_id > (SELECT current_seq_id FROM view_meta) AND seq_id <= (SELECT next_seq_id FROM view_meta);
		CREATE TEMP VIEW documents AS SELECT doc_id, printf('%d-%s', version, hash) as rev, deleted, data, seq_id FROM docsdb.documents
	`)

	return err
}
