package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"kdb3/sqlite3"
)

type ViewManager interface {
	Initialize(designDocs []Document) error
	OpenView(docID, viewName string, designDocumentView DesignDocumentView) error
	GetView(viewName string) (*View, bool)
	SelectView(updateSeq int64, designDoc Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	SQL(updateSeq int64, doc Document, viewName string) ([]byte, error)
	DryRun(req ViewAtelierDryRunRequest) (*ViewAtelierDryRunResult, error)
	GetViewStatus(designDocID, viewName string, dbUpdateSeq int64) (*ViewStatus, error)

	DeleteViewsIfRemoved(doc Document)
	ValidateDesignDocument(doc Document) error
	CalculateSignature(designView DesignDocumentView) string
	ParseQueryParams(query string) (string, []string)

	Close(closeChannel bool) error
	ReinitializeViews() error
	RebuildAfterVacuum(updateSeq int64) error
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
		return err
	}
	localDBViewFiles, err := mgr.localDB.ListViewFiles(mgr.DBName)
	if err != nil {
		return err
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
			removeSQLiteFiles(path.Join(mgr.viewDirPath, diskViewFile+dbExt))
		}
	}

	//load all design docs into memory
	for _, x := range designDocs {
		designDoc := &DesignDocument{}
		doc, _ := ParseDocument(x.Data)
		err := json.Unmarshal(doc.Data, designDoc)
		designDoc.Version = doc.Version
		if err != nil {
			return err
		}
		mgr.designDocs[x.ID] = designDoc
	}

	return nil
}

func (mgr *DefaultViewManager) ListViewFiles() ([]string, error) {
	list, err := os.ReadDir(mgr.viewDirPath)
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

			if err := view.ReInitialize(); err != nil {
				return err
			}
			mgr.deleteViewFileIfNoReference(currentViewFileName)
		}
	} else {
		view = NewView(mgr.DBName, viewName, docID, &designDocumentView, mgr, mgr.serviceLocator)
		if err := view.Open(); err != nil {
			return err
		}
		mgr.views[qualifiedViewName] = view
		syncViewPoolGaugesLocked(mgr)
	}
	return nil
}

func (mgr *DefaultViewManager) openOrUpdateView(doc Document, viewName, qualifiedViewName string) (*View, error) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	designDoc := &DesignDocument{}
	err := json.Unmarshal(doc.Data, designDoc)
	designDoc.Version = designDoc.Rev
	if err != nil {
		return nil, fmt.Errorf("%w: invalid design document %s", ErrDocumentInvalidInput, doc.ID)
	}
	if _, ok := mgr.designDocs[doc.ID]; !ok {
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
	if view == nil {
		return nil, ErrViewNotFound
	}

	selectScripts := make(map[string]Query)
	for k, v := range designDocView.Select {
		text, params := mgr.ParseQueryParams(v)
		selectScripts[k] = Query{text: text, params: params}
	}
	view.selectScripts = selectScripts
	return view, nil
}

func (mgr *DefaultViewManager) SelectView(updateSeq int64, doc Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	designDocID := doc.ID
	qualifiedViewName := designDocID + "$" + viewName

	// Lookup under RLock only — never hold mgr locks while blocking on view channels
	// (Build/Select), or Vacuum's ReinitializeViews can deadlock.
	mgr.rwMutex.RLock()
	view, ok := mgr.views[qualifiedViewName]
	var designVer int
	if dd := mgr.designDocs[designDocID]; dd != nil {
		designVer = dd.Version
	}
	mgr.rwMutex.RUnlock()

	var (
		err error
		rs  []byte
	)
	defer func() {
		viewQueriesTotal.WithLabelValues(mgr.DBName, metricsResult(err)).Inc()
		syncViewPoolGauges(mgr)
	}()

	if !ok {
		view, err = mgr.openOrUpdateView(doc, viewName, qualifiedViewName)
		if err != nil {
			return nil, err
		}
	}

	if view == nil {
		err = ErrViewNotFound
		return nil, err
	}

	if stale {
		rs, err = view.Select(selectName, values)
		return rs, err
	}

	if doc.Version != designVer {
		view, err = mgr.openOrUpdateView(doc, viewName, qualifiedViewName)
		if err != nil {
			return nil, err
		}
	}
	if view == nil {
		err = ErrViewNotFound
		return nil, err
	}

	if err = view.Build(updateSeq); err != nil {
		return nil, err
	}
	rs, err = view.Select(selectName, values)
	return rs, err
}

func (mgr *DefaultViewManager) SQL(fromSeq int64, doc Document, viewName string) ([]byte, error) {
	designDocID := doc.ID
	qualifiedViewName := designDocID + "$" + viewName

	mgr.rwMutex.RLock()
	view, ok := mgr.views[qualifiedViewName]
	mgr.rwMutex.RUnlock()

	var err error
	if !ok {
		view, err = mgr.openOrUpdateView(doc, viewName, qualifiedViewName)
		if err != nil {
			return nil, err
		}
	}
	if view == nil {
		return nil, ErrViewNotFound
	}
	return view.SQL(fromSeq)
}

func (mgr *DefaultViewManager) Close(closeChannel bool) error {
	mgr.rwMutex.Lock()
	views := make([]*View, 0, len(mgr.views))
	keys := make([]string, 0, len(mgr.views))
	for k, v := range mgr.views {
		views = append(views, v)
		keys = append(keys, k)
	}
	if closeChannel {
		for _, k := range keys {
			delete(mgr.views, k)
		}
	}
	mgr.rwMutex.Unlock()

	// Close without holding rwMutex so SelectView (RLock + channel ops) cannot deadlock.
	for _, v := range views {
		if err := v.Close(closeChannel); err != nil {
			return err
		}
	}
	syncViewPoolGauges(mgr)
	return nil
}

func (mgr *DefaultViewManager) ReinitializeViews() error {
	mgr.rwMutex.Lock()
	views := make([]*View, 0, len(mgr.views))
	for _, v := range mgr.views {
		views = append(views, v)
	}
	mgr.rwMutex.Unlock()

	for _, v := range views {
		if err := v.ReInitialize(); err != nil {
			return err
		}
	}
	return nil
}

// RebuildAfterVacuum wipes persistent view indexes and rebuilds them from live docs.
// Call only after views were Close(false)'d (channels drained); do not Close again.
func (mgr *DefaultViewManager) RebuildAfterVacuum(updateSeq int64) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	mgr.views = make(map[string]*View)

	viewFiles, err := mgr.localDB.ListViewFiles(mgr.DBName)
	if err != nil {
		return err
	}
	if err := mgr.localDB.DeleteViews(mgr.DBName); err != nil {
		return err
	}
	for _, vf := range viewFiles {
		removeSQLiteFiles(path.Join(mgr.viewDirPath, vf+dbExt))
	}
	diskFiles, err := mgr.ListViewFiles()
	if err != nil {
		return err
	}
	for _, vf := range diskFiles {
		removeSQLiteFiles(path.Join(mgr.viewDirPath, vf+dbExt))
	}

	for docID, dd := range mgr.designDocs {
		if dd == nil {
			continue
		}
		id := dd.ID
		if id == "" {
			id = docID
		}
		for viewName, viewDef := range dd.Views {
			if viewDef == nil {
				continue
			}
			if err := mgr.OpenView(id, viewName, *viewDef); err != nil {
				return err
			}
			qualified := id + "$" + viewName
			view := mgr.views[qualified]
			if view == nil {
				return ErrViewNotFound
			}
			if err := view.Build(updateSeq); err != nil {
				return err
			}
		}
	}
	syncViewPoolGaugesLocked(mgr)
	return nil
}

func (mgr *DefaultViewManager) Vacuum() error {
	mgr.rwMutex.Lock()
	views := make([]*View, 0, len(mgr.views))
	for _, v := range mgr.views {
		views = append(views, v)
	}
	mgr.rwMutex.Unlock()

	for _, v := range views {
		_ = v.Vacuum()
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
	syncViewPoolGaugesLocked(mgr)
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
		removeSQLiteFiles(path.Join(mgr.viewDirPath, viewFileName+dbExt))
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
		if currentDesignDoc == nil {
			return
		}
		newDesignDoc := &DesignDocument{}
		err := json.Unmarshal(doc.Data, newDesignDoc)
		if err != nil {
			return
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

func containsInvalidSQLKeyword(query string, invalidKeywords []string) string {
	q := strings.ToLower(query)
	for _, invalidKeyword := range invalidKeywords {
		kw := strings.ToLower(invalidKeyword)
		if strings.Contains(q, kw) {
			return invalidKeyword
		}
	}
	return ""
}

func (mgr *DefaultViewManager) ValidateDesignDocument(doc Document) error {
	newDDoc := &DesignDocument{}
	err := json.Unmarshal(doc.Data, newDDoc)
	if err != nil {
		return fmt.Errorf("%w: invalid design document %s", ErrDocumentInvalidInput, doc.ID)
	}

	db, err := sqlite3.Open(":memory:")
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.WithTx(func() error {
		if err = db.Exec("CREATE VIEW latest_changes (doc_id, deleted) AS select '', 0 as doc_id;"); err != nil {
			return err
		}
		if err = db.Exec("CREATE VIEW latest_documents (doc_id, rev, deleted, data) AS select '' as doc_id, '1-xxxxxxxxxxxxxx' as rev, 0, '{}' as data;"); err != nil {
			return err
		}
		if err = db.Exec("CREATE VIEW documents (doc_id, rev, deleted, data) AS select '' as doc_id, '1-xxxxxxxxxxxxxx' as rev, 0, '{}' as data;"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, v := range newDDoc.Views {
		if v == nil {
			continue
		}
		if kwErr := checkViewSQLKeywords(v.Setup, v.Run, v.Select); kwErr != nil {
			return fmt.Errorf("%s: %w", sqlErrorReason(*kwErr), ErrInvalidSQLStmt)
		}
		for i, x := range v.Setup {
			if err := db.Exec(x); err != nil {
				return fmt.Errorf("%s: %w", formatSQLStmtError("setup", i, err.Error()), ErrInvalidSQLStmt)
			}
		}
		for i, x := range v.Run {
			if err := db.Exec(x); err != nil {
				return fmt.Errorf("%s: %w", formatSQLStmtError("run", i, err.Error()), ErrInvalidSQLStmt)
			}
		}
	}

	if err = db.Exec("SELECT * FROM latest_changes WHERE 1 = 2"); err != nil {
		return errors.New("your script can't drop latest_changes")
	}
	if err = db.Exec("SELECT * FROM latest_documents WHERE 1 = 2"); err != nil {
		return errors.New("your script can't drop latest_documents")
	}
	if err = db.Exec("SELECT * FROM documents WHERE 1 = 2"); err != nil {
		return errors.New("your script can't drop documents")
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
	name        string
	DBName      string
	designDocID string
	currentSeq  int64

	viewReader chan ViewReader
	viewWriter chan ViewWriter

	serviceLocator ServiceLocator

	setupScripts  []Query
	runScripts    []Query
	selectScripts map[string]Query
}

func (view *View) ReInitialize() error {
	viewWriter := view.serviceLocator.GetViewWriter(view.DBName, view.designDocID, view.name, view.setupScripts, view.runScripts)
	if err := viewWriter.Open(); err != nil {
		return err
	}
	view.viewWriter <- viewWriter

	readersCount := cap(view.viewReader)
	for i := 0; i < readersCount; i++ {
		viewReader := view.serviceLocator.GetViewReader(view.DBName, view.designDocID, view.name, view.setupScripts, view.selectScripts)
		if err := viewReader.Open(); err != nil {
			_ = viewReader.Close()
			return err
		}
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

	readersCount := cap(view.viewReader)
	pending := make([]ViewReader, 0, readersCount)
	for i := 0; i < readersCount; i++ {
		pending = append(pending, <-view.viewReader)
	}
	opened := make([]ViewReader, 0, readersCount)
	var openErr error
	for _, viewReader := range pending {
		if openErr != nil {
			view.viewReader <- viewReader
			continue
		}
		if err := viewReader.Open(); err != nil {
			_ = viewReader.Close()
			openErr = err
			view.viewReader <- view.serviceLocator.GetViewReader(view.DBName, view.designDocID, view.name, view.setupScripts, view.selectScripts)
			continue
		}
		opened = append(opened, viewReader)
	}
	if openErr != nil {
		for _, r := range opened {
			_ = r.Close()
			view.viewReader <- view.serviceLocator.GetViewReader(view.DBName, view.designDocID, view.name, view.setupScripts, view.selectScripts)
		}
		return openErr
	}
	for _, reader := range opened {
		view.viewReader <- reader
	}

	return nil
}

func (view *View) Close(closeChannel bool) error {
	viewWriter := <-view.viewWriter
	err := viewWriter.Close()
	if err != nil {
		view.viewWriter <- viewWriter
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

func (view *View) Build(nextSeq int64) error {
	if view.currentSeq >= nextSeq {
		return nil
	}

	viewWriter, ok := <-view.viewWriter
	if !ok {
		return ErrViewNotFound
	}
	defer func() {
		view.viewWriter <- viewWriter
	}()

	if view.currentSeq >= nextSeq {
		return nil
	}

	start := time.Now()
	err := viewWriter.Build(nextSeq)
	viewBuildDuration.WithLabelValues(view.DBName).Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}

	view.currentSeq = nextSeq

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

func (view *View) SQL(fromSeq int64) ([]byte, error) {
	vs := view.serviceLocator.GetViewSQLBuilder(view.DBName, view.designDocID, view.name, view.setupScripts, view.runScripts)
	vs.Open()
	return vs.SQL(fromSeq)
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

	view.viewReader = make(chan ViewReader, viewReaderPoolSize)
	view.viewWriter = make(chan ViewWriter, 1)

	view.viewWriter <- view.serviceLocator.GetViewWriter(view.DBName, view.designDocID, view.name, view.setupScripts, view.runScripts)
	readersCount := cap(view.viewReader)
	for i := 0; i < readersCount; i++ {
		view.viewReader <- view.serviceLocator.GetViewReader(view.DBName, view.designDocID, view.name, view.setupScripts, view.selectScripts)
	}

	return view
}

func setupViewDatabase(db *sqlite3.Conn, absoluteDatabasePath string) error {
	// Attach without cache=shared: a shared-cache readonly attach can mark the
	// main DB readonly for writers and surface as SQLITE_READONLY / DBMOVED.
	err := db.Exec("ATTACH DATABASE 'file:" + absoluteDatabasePath + "?mode=ro' as docsdb;")
	if err != nil {
		return err
	}

	err = db.Exec(`
		CREATE TEMP VIEW latest_changes AS SELECT doc_id, deleted, update_seq FROM docsdb.documents INDEXED BY idx_changes WHERE update_seq > (SELECT current_update_seq FROM view_meta) AND update_seq <= (SELECT next_update_seq FROM view_meta);
		CREATE TEMP VIEW latest_documents AS SELECT doc_id, version as rev, deleted, data, update_seq FROM docsdb.documents WHERE update_seq > (SELECT current_update_seq FROM view_meta) AND update_seq <= (SELECT next_update_seq FROM view_meta);
		CREATE TEMP VIEW documents AS SELECT doc_id, version as rev, deleted, data, update_seq FROM docsdb.documents
	`)

	return err
}
