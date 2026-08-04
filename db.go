package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Database interface
type Database interface {
	Initialize() error
	ReInitialize() error
	Open(createIfNotExists bool) error
	Close(closeChannel bool) error

	PutDocument(doc *Document) (*Document, error)
	DeleteDocument(doc *Document) (*Document, error)
	GetDocument(doc *Document, includeData bool) (*Document, error)
	GetAllDesignDocuments() ([]Document, error)
	GetLastUpdateSequence() int64
	GetChanges(since int64, limit int, order bool) ([]byte, error)
	GetChangesRows(since int64, limit int, desc bool) ([]Change, error)
	ChangesNotifyChan() <-chan struct{}
	GetDocumentCount() (int, int)

	GetStat() *DatabaseStat
	SelectView(designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	SQL(fromSeq int64, designDocID, viewName string) ([]byte, error)
	DryRunView(viewName string, req ViewAtelierDryRunRequest) (*ViewAtelierDryRunResult, error)
	GetViewStatus(designDocID, viewName string) (*ViewStatus, error)
	ValidateDesignDocument(doc Document) error
	SetupAllDocsViews() error
	Vacuum() error

	GetViewManager() ViewManager
}

// DefaultDatabase default implementation of database
type DefaultDatabase struct {
	Name string

	updateSeq    atomic.Int64
	docCount     atomic.Int64
	deletedCount atomic.Int64

	mutex     sync.RWMutex
	changeSeq *ChangeSequenceGenarator
	idSeq     *SequenceUUIDGenarator

	reader chan DatabaseReader
	writer chan DatabaseWriter

	viewManager   ViewManager
	vacuumManager chan VacuumManager

	serviceLocator ServiceLocator

	notifyMu      sync.Mutex
	changesNotify chan struct{}
}

// legacy field accessors used by tests expecting struct fields — keep via methods
func (db *DefaultDatabase) UpdateSequence() int64 { return db.updateSeq.Load() }
func (db *DefaultDatabase) DocumentCount() int    { return int(db.docCount.Load()) }
func (db *DefaultDatabase) DeletedDocumentCount() int {
	return int(db.deletedCount.Load())
}

func (db *DefaultDatabase) openReaders() error {
	readersCount := cap(db.reader)
	pending := make([]DatabaseReader, 0, readersCount)
	for i := 0; i < readersCount; i++ {
		pending = append(pending, <-db.reader)
	}

	opened := make([]DatabaseReader, 0, readersCount)
	var openErr error
	for _, reader := range pending {
		if openErr != nil {
			db.reader <- reader
			continue
		}
		if err := reader.Open(); err != nil {
			_ = reader.Close()
			openErr = err
			db.reader <- db.serviceLocator.GetDatabaseReader(db.Name)
			continue
		}
		opened = append(opened, reader)
	}
	if openErr != nil {
		for _, r := range opened {
			_ = r.Close()
			db.reader <- db.serviceLocator.GetDatabaseReader(db.Name)
		}
		return openErr
	}
	for _, reader := range opened {
		db.reader <- reader
	}
	return nil
}

// Open open kdb database
func (db *DefaultDatabase) Open(createIfNotExists bool) error {
	writer := <-db.writer
	err := writer.Open(createIfNotExists)
	if err != nil {
		db.writer <- writer
		return err
	}
	db.writer <- writer

	if err := db.openReaders(); err != nil {
		return err
	}

	docs, deleted := db.getDocumentCountUnlocked()
	db.docCount.Store(int64(docs))
	db.deletedCount.Store(int64(deleted))
	db.updateSeq.Store(db.getLastUpdateSequenceUnlocked())
	db.changeSeq = NewChangeSequenceGenarator(db.updateSeq.Load())

	if createIfNotExists {
		if err = db.SetupAllDocsViews(); err != nil {
			return err
		}
	}

	designDocs, err := db.getAllDesignDocumentsUnlocked()
	if err != nil {
		return err
	}

	if err := db.viewManager.Initialize(designDocs); err != nil {
		return err
	}
	syncDatabaseStatGauges(db)
	syncDBPoolGauges(db)
	return nil
}

func (db *DefaultDatabase) closePoolsUnlocked(closeChannel bool) error {
	err := db.viewManager.Close(closeChannel)
	if err != nil {
		return err
	}

	writer := <-db.writer
	werr := writer.Close()
	if werr != nil {
		db.writer <- writer
		return werr
	}

	var foundError error
	readersCount := cap(db.reader)
	for i := 0; i < readersCount; i++ {
		reader := <-db.reader
		if err = reader.Close(); err != nil {
			foundError = err
		}
	}

	if foundError != nil {
		return foundError
	}

	if closeChannel {
		close(db.writer)
		close(db.reader)
	}

	return nil
}

// Close close the kdb database
func (db *DefaultDatabase) Close(closeChannel bool) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.closePoolsUnlocked(closeChannel)
}

func (db *DefaultDatabase) applyCountDelta(currentDoc *Document, doc *Document) {
	wasDeleted := currentDoc != nil && currentDoc.Deleted
	isNew := currentDoc == nil

	if doc.Deleted {
		if !wasDeleted && !isNew {
			db.docCount.Add(-1)
			db.deletedCount.Add(1)
		}
		return
	}
	if isNew {
		db.docCount.Add(1)
		return
	}
	if wasDeleted {
		db.docCount.Add(1)
		db.deletedCount.Add(-1)
	}
}

// PutDocument put a document
func (db *DefaultDatabase) PutDocument(doc *Document) (*Document, error) {
	start := time.Now()
	defer func() {
		documentWriteDuration.WithLabelValues(db.Name).Observe(time.Since(start).Seconds())
		syncDBPoolGauges(db)
	}()

	writer, ok := <-db.writer
	if !ok {
		documentsWrittenTotal.WithLabelValues(db.Name, metricsResult(ErrDatabaseNotFound)).Inc()
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.writer <- writer
	}()

	defer writer.Rollback()
	if err := writer.Begin(); err != nil {
		documentsWrittenTotal.WithLabelValues(db.Name, metricsResult(err)).Inc()
		return nil, err
	}

	out, currentDoc, err := db.putDocumentWithWriter(writer, doc)
	if err != nil {
		documentsWrittenTotal.WithLabelValues(db.Name, metricsResult(err)).Inc()
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		documentsWrittenTotal.WithLabelValues(db.Name, metricsResult(err)).Inc()
		return nil, err
	}

	db.updateSeq.Store(out.updateSeq)
	db.applyCountDelta(currentDoc, out.doc)
	syncDatabaseStatGauges(db)
	documentsWrittenTotal.WithLabelValues(db.Name, "ok").Inc()
	db.notifyChanges()

	if currentDoc != nil && strings.HasPrefix(out.doc.ID, "_design/") {
		db.viewManager.DeleteViewsIfRemoved(*out.doc)
	}

	return out.doc, nil
}

type putResult struct {
	doc       *Document
	updateSeq int64
}

func (db *DefaultDatabase) putDocumentWithWriter(writer DatabaseWriter, doc *Document) (*putResult, *Document, error) {
	if doc.ID == "" {
		doc.ID = db.idSeq.Next()
	}

	currentDoc, err := writer.GetDocumentMetadataByID(doc.ID)
	if err != nil && err != ErrDocumentNotFound {
		return nil, nil, fmt.Errorf("%s: %w", err.Error(), ErrInternalError)
	}

	if currentDoc != nil {
		if currentDoc.Deleted {
			if doc.Version == 0 || currentDoc.Version != doc.Version {
				return nil, nil, ErrDocumentConflict
			}
			doc.Version = currentDoc.Version
		} else {
			if doc.Version == 0 || currentDoc.Version != doc.Version {
				return nil, nil, ErrDocumentConflict
			}
		}
	} else {
		if doc.Version > 0 {
			return nil, nil, ErrDocumentConflict
		}
	}

	doc.CalculateNextVersion()
	updateSeq := db.changeSeq.Next()

	if err = writer.PutDocument(updateSeq, doc); err != nil {
		return nil, nil, err
	}

	return &putResult{doc: doc, updateSeq: updateSeq}, currentDoc, nil
}

// BulkPutDocuments put many documents in a single transaction
func (db *DefaultDatabase) BulkPutDocuments(docs []*Document) ([]*Document, []error) {
	start := time.Now()
	outs := make([]*Document, len(docs))
	errs := make([]error, len(docs))
	defer func() {
		documentWriteDuration.WithLabelValues(db.Name).Observe(time.Since(start).Seconds())
		for _, err := range errs {
			if err != nil {
				documentsWrittenTotal.WithLabelValues(db.Name, metricsResult(err)).Inc()
			}
		}
		for _, out := range outs {
			if out != nil {
				documentsWrittenTotal.WithLabelValues(db.Name, "ok").Inc()
			}
		}
		syncDBPoolGauges(db)
	}()

	writer, ok := <-db.writer
	if !ok {
		for i := range docs {
			errs[i] = ErrDatabaseNotFound
		}
		return outs, errs
	}
	defer func() { db.writer <- writer }()

	defer writer.Rollback()
	if err := writer.Begin(); err != nil {
		for i := range docs {
			errs[i] = err
		}
		return outs, errs
	}

	type pending struct {
		out     *Document
		current *Document
		seq     int64
	}
	pendings := make([]*pending, len(docs))
	var lastSeq int64
	for i, doc := range docs {
		if doc == nil {
			errs[i] = ErrDocumentInvalidInput
			continue
		}
		res, currentDoc, err := db.putDocumentWithWriter(writer, doc)
		if err != nil {
			errs[i] = err
			continue
		}
		pendings[i] = &pending{out: res.doc, current: currentDoc, seq: res.updateSeq}
		lastSeq = res.updateSeq
	}

	if err := writer.Commit(); err != nil {
		for i := range docs {
			if errs[i] == nil && pendings[i] != nil {
				errs[i] = err
				pendings[i] = nil
			}
		}
		return outs, errs
	}
	if lastSeq > 0 {
		db.updateSeq.Store(lastSeq)
	}
	notified := false
	for i, p := range pendings {
		if p == nil {
			continue
		}
		outs[i] = p.out
		notified = true
		db.applyCountDelta(p.current, p.out)
		if p.current != nil && strings.HasPrefix(p.out.ID, "_design/") {
			db.viewManager.DeleteViewsIfRemoved(*p.out)
		}
	}
	syncDatabaseStatGauges(db)
	if notified {
		db.notifyChanges()
	}
	return outs, errs
}

// DeleteDocument delete a document
func (db *DefaultDatabase) DeleteDocument(doc *Document) (*Document, error) {
	doc.Deleted = true
	return db.PutDocument(doc)
}

// GetDocument get a document
func (db *DefaultDatabase) GetDocument(doc *Document, includeData bool) (*Document, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.getDocumentUnlocked(doc, includeData)
}

func (db *DefaultDatabase) getDocumentUnlocked(doc *Document, includeData bool) (*Document, error) {
	start := time.Now()
	defer func() {
		documentReadDuration.WithLabelValues(db.Name).Observe(time.Since(start).Seconds())
		syncDBPoolGauges(db)
	}()

	reader, ok := <-db.reader
	if !ok {
		documentsReadTotal.WithLabelValues(db.Name, metricsResult(ErrDatabaseNotFound)).Inc()
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	var (
		out *Document
		err error
	)
	if includeData {
		if doc.Version > 0 {
			out, err = reader.GetDocumentByIDandVersion(doc.ID, doc.Version)
		} else {
			out, err = reader.GetDocumentByID(doc.ID)
		}
	} else if doc.Version > 0 {
		out, err = reader.GetDocumentMetadataByIDandVersion(doc.ID, doc.Version)
	} else {
		out, err = reader.GetDocumentMetadataByID(doc.ID)
	}
	documentsReadTotal.WithLabelValues(db.Name, metricsResult(err)).Inc()
	return out, err
}

// GetAllDesignDocuments get all design document
func (db *DefaultDatabase) GetAllDesignDocuments() ([]Document, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.getAllDesignDocumentsUnlocked()
}

func (db *DefaultDatabase) getAllDesignDocumentsUnlocked() ([]Document, error) {
	reader, ok := <-db.reader
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	reader.Begin()
	defer reader.Commit()

	return reader.GetAllDesignDocuments()
}

// GetLastUpdateSequence get last sequence number
func (db *DefaultDatabase) GetLastUpdateSequence() int64 {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.getLastUpdateSequenceUnlocked()
}

func (db *DefaultDatabase) getLastUpdateSequenceUnlocked() int64 {
	reader, ok := <-db.reader
	if !ok {
		return 0
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	return reader.GetLastUpdateSequence()
}

// GetChanges get changes
func (db *DefaultDatabase) GetChanges(since int64, limit int, desc bool) ([]byte, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	reader, ok := <-db.reader
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	return reader.GetChanges(since, limit, desc)
}

// GetChangesRows returns typed change rows.
func (db *DefaultDatabase) GetChangesRows(since int64, limit int, desc bool) ([]Change, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	reader, ok := <-db.reader
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	return reader.GetChangesRows(since, limit, desc)
}

func (db *DefaultDatabase) notifyChanges() {
	db.notifyMu.Lock()
	defer db.notifyMu.Unlock()
	if db.changesNotify != nil {
		close(db.changesNotify)
	}
	db.changesNotify = make(chan struct{})
}

// ChangesNotifyChan returns the current notify channel. Callers should
// snapshot it before reading changes so writes during the read are not missed.
func (db *DefaultDatabase) ChangesNotifyChan() <-chan struct{} {
	db.notifyMu.Lock()
	ch := db.changesNotify
	db.notifyMu.Unlock()
	return ch
}

// GetDocumentCount get document count
func (db *DefaultDatabase) GetDocumentCount() (int, int) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.getDocumentCountUnlocked()
}

func (db *DefaultDatabase) getDocumentCountUnlocked() (int, int) {
	reader, ok := <-db.reader
	if !ok {
		return 0, 0
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	return reader.GetDocumentCount()
}

// GetStat get database stat
func (db *DefaultDatabase) GetStat() *DatabaseStat {
	return &DatabaseStat{
		DBName:          db.Name,
		UpdateSeq:       db.updateSeq.Load(),
		DocCount:        int(db.docCount.Load()),
		DeletedDocCount: int(db.deletedCount.Load()),
	}
}

// Vacuum vacuum — quiesces writer, copies live docs, swaps file; errors abort without rename/delete.
func (db *DefaultDatabase) Vacuum() error {
	start := time.Now()
	vacuumInProgress.WithLabelValues(db.Name).Set(1)
	var vacuumErr error
	defer func() {
		vacuumInProgress.WithLabelValues(db.Name).Set(0)
		vacuumDuration.WithLabelValues(db.Name).Observe(time.Since(start).Seconds())
		vacuumTotal.WithLabelValues(db.Name, metricsResult(vacuumErr)).Inc()
		syncDBPoolGauges(db)
	}()

	vacuumManager := <-db.vacuumManager
	defer func() {
		db.vacuumManager <- vacuumManager
	}()

	db.mutex.Lock()
	defer db.mutex.Unlock()

	currentFileName := db.serviceLocator.GetLocalDB().GetDatabaseFileName(db.Name)
	currentDBPath := filepath.Join(db.serviceLocator.GetDBDirPath(), currentFileName+dbExt)

	id := NewSequenceUUIDGenarator().Next()
	newFileName := db.Name + "_" + id
	newConnectionString := filepath.Join(db.serviceLocator.GetDBDirPath(), newFileName+dbExt)

	vacuumManager.SetNewConnectionString(newConnectionString)
	vacuumManager.SetCurrentConnectionString(currentDBPath, currentDBPath)

	if err := vacuumManager.SetupDatabase(); err != nil {
		removeSQLiteFiles(newConnectionString)
		vacuumErr = err
		return err
	}

	// Quiesce writes for the entire compact.
	writer := <-db.writer
	maxUpdateSequence := db.updateSeq.Load()

	restorePools := func() {
		_ = db.reinitializeUnlocked()
		_ = db.viewManager.ReinitializeViews()
	}

	if err := vacuumManager.CopyData(0, maxUpdateSequence); err != nil {
		db.writer <- writer
		removeSQLiteFiles(newConnectionString)
		vacuumErr = err
		return err
	}

	if err := vacuumManager.Vacuum(); err != nil {
		db.writer <- writer
		removeSQLiteFiles(newConnectionString)
		vacuumErr = err
		return err
	}

	if err := writer.Close(); err != nil {
		db.writer <- writer
		removeSQLiteFiles(newConnectionString)
		vacuumErr = err
		return err
	}

	var readerErr error
	readersCount := cap(db.reader)
	for i := 0; i < readersCount; i++ {
		reader := <-db.reader
		if err := reader.Close(); err != nil {
			readerErr = err
		}
	}
	if err := db.viewManager.Close(false); err != nil {
		removeSQLiteFiles(newConnectionString)
		restorePools()
		vacuumErr = err
		return err
	}
	if readerErr != nil {
		removeSQLiteFiles(newConnectionString)
		restorePools()
		vacuumErr = readerErr
		return readerErr
	}

	localDB := db.serviceLocator.GetLocalDB()
	if err := localDB.UpdateDatabaseFileName(db.Name, newFileName); err != nil {
		removeSQLiteFiles(newConnectionString)
		restorePools()
		vacuumErr = err
		return err
	}

	if err := db.reinitializeUnlocked(); err != nil {
		vacuumErr = err
		return err
	}

	docs, deleted := db.getDocumentCountUnlocked()
	db.docCount.Store(int64(docs))
	db.deletedCount.Store(int64(deleted))
	// Never rewind update_seq: tombstone purge can lower MAX(seq) of live docs.
	liveSeq := db.getLastUpdateSequenceUnlocked()
	hwm := maxUpdateSequence
	if liveSeq > hwm {
		hwm = liveSeq
	}
	db.updateSeq.Store(hwm)
	db.changeSeq = NewChangeSequenceGenarator(hwm)
	syncDatabaseStatGauges(db)

	// Wipe stale view indexes (tombstones are gone; incremental deletes can't run) and rebuild.
	if err := db.viewManager.RebuildAfterVacuum(hwm); err != nil {
		vacuumErr = err
		return err
	}

	// Remove old DB + WAL/SHM. Orphan sidecars make SQLite report READONLY_DBMOVED (1032).
	removeSQLiteFiles(filepath.Join(db.serviceLocator.GetDBDirPath(), currentFileName+dbExt))
	return nil
}

// SelectView select view
func (db *DefaultDatabase) SelectView(designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	// Hold RLock for the whole select so Vacuum (Lock + pool drain) cannot interleave
	// and deadlock against GetDocument/view channel ops.
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	inputDoc := &Document{ID: designDocID}
	outputDoc, err := db.getDocumentUnlocked(inputDoc, true)
	if err != nil {
		return nil, err
	}

	if !values.Has("limit") {
		values.Set("limit", "10")
	}

	if !values.Has("offset") {
		values.Set("offset", "0")
	}

	return db.viewManager.SelectView(db.updateSeq.Load(), *outputDoc, viewName, selectName, values, stale)
}

// SQL build sql
func (db *DefaultDatabase) SQL(fromSeq int64, designDocID, viewName string) ([]byte, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	inputDoc := &Document{ID: designDocID}
	outputDoc, err := db.getDocumentUnlocked(inputDoc, true)
	if err != nil {
		return nil, err
	}
	if fromSeq == db.updateSeq.Load() {
		return nil, nil
	}
	return db.viewManager.SQL(fromSeq, *outputDoc, viewName)
}

// DryRunView dry-runs draft view SQL against a docs sequence window.
func (db *DefaultDatabase) DryRunView(viewName string, req ViewAtelierDryRunRequest) (*ViewAtelierDryRunResult, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	_ = viewName
	return db.viewManager.DryRun(req)
}

// GetViewStatus returns view lag vs the database update_seq.
func (db *DefaultDatabase) GetViewStatus(designDocID, viewName string) (*ViewStatus, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.viewManager.GetViewStatus(designDocID, viewName, db.updateSeq.Load())
}

// ValidateDesignDocument validate design document
func (db *DefaultDatabase) ValidateDesignDocument(doc Document) error {
	return db.viewManager.ValidateDesignDocument(doc)
}

// GetViewManager get a view manager
func (db *DefaultDatabase) GetViewManager() ViewManager {
	return db.viewManager
}

// SetupAllDocsViews setup default views
func (db *DefaultDatabase) SetupAllDocsViews() error {
	doc := `
		{
			"_id" : "_design/_views",
			"views" : {
				"_all_docs" : {
					"setup" : [
						"CREATE TABLE IF NOT EXISTS all_docs (key, rev, doc_id, PRIMARY KEY(doc_id)) WITHOUT ROWID",
						"CREATE TABLE IF NOT EXISTS all_docs_meta (id INTEGER PRIMARY KEY, total_rows INTEGER) WITHOUT ROWID",
						"INSERT OR IGNORE INTO all_docs_meta (id, total_rows) VALUES (1, 0)"
					],
					"run" : [
						"DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
						"INSERT OR REPLACE INTO all_docs (key, rev, doc_id) SELECT doc_id, rev, doc_id FROM latest_documents WHERE deleted = 0",
						"UPDATE all_docs_meta SET total_rows = (SELECT COUNT(1) FROM all_docs) WHERE id = 1"
					],
					"select" : {
						"default" : "SELECT JSON_OBJECT('offset', ifnull(min(offset) + 1, 0),'rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', doc_id, 'id', doc_id, 'rev', rev)),'total_rows', (SELECT total_rows FROM all_docs_meta WHERE id = 1)) as data FROM (SELECT (ROW_NUMBER() OVER(ORDER BY doc_id) - 1) as offset, * FROM all_docs WHERE (${startkey} IS NULL OR doc_id >= ${startkey}) AND (${endkey} IS NULL OR doc_id <= ${endkey}) ORDER BY doc_id LIMIT CAST(${limit} AS INT) OFFSET CAST(${offset} AS INT))",
						"with_docs" : "SELECT JSON_OBJECT('offset', ifnull(min(offset) + 1, 0),'rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', doc_id, 'id', doc_id, 'rev', rev, 'doc', JSON((SELECT data FROM documents WHERE doc_id = o.doc_id)))),'total_rows', (SELECT total_rows FROM all_docs_meta WHERE id = 1)) as data FROM (SELECT (ROW_NUMBER() OVER(ORDER BY doc_id) - 1) as offset, * FROM all_docs WHERE (${startkey} IS NULL OR doc_id >= ${startkey}) AND (${endkey} IS NULL OR doc_id <= ${endkey}) ORDER BY doc_id LIMIT CAST(${limit} AS INT) OFFSET CAST(${offset} AS INT)) o"
					}
				}
			}
		}
	`

	designDoc, err := ParseDocument([]byte(doc))
	if err != nil {
		return err
	}

	_, err = db.PutDocument(designDoc)
	return err
}

func (db *DefaultDatabase) Initialize() error {
	db.writer <- db.serviceLocator.GetDatabaseWriter(db.Name)
	readersCount := cap(db.reader)
	for i := 0; i < readersCount; i++ {
		db.reader <- db.serviceLocator.GetDatabaseReader(db.Name)
	}
	return nil
}

func (db *DefaultDatabase) reinitializeUnlocked() error {
	writer := db.serviceLocator.GetDatabaseWriter(db.Name)
	if err := writer.Open(false); err != nil {
		return err
	}
	db.writer <- writer

	readersCount := cap(db.reader)
	for i := 0; i < readersCount; i++ {
		reader := db.serviceLocator.GetDatabaseReader(db.Name)
		if err := reader.Open(); err != nil {
			return err
		}
		db.reader <- reader
	}
	return nil
}

func (db *DefaultDatabase) ReInitialize() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.reinitializeUnlocked()
}

// NewDatabase create database instance
func NewDatabase(name string, createIfNotExists bool, serviceLocator ServiceLocator) (Database, error) {
	db := &DefaultDatabase{Name: name}
	db.idSeq = NewSequenceUUIDGenarator()
	db.serviceLocator = serviceLocator
	db.changesNotify = make(chan struct{})

	db.writer = make(chan DatabaseWriter, 1)
	db.reader = make(chan DatabaseReader, dbReaderPoolSize)
	db.vacuumManager = make(chan VacuumManager, 1)
	db.vacuumManager <- serviceLocator.GetVacuumManager(name)

	db.viewManager = serviceLocator.GetViewManager(name)

	db.Initialize()

	err := db.Open(createIfNotExists)
	if err != nil {
		return nil, err
	}

	return db, nil
}
