package main

import (
	"errors"
	"net/url"
	"testing"
)

type FakeDatabaseReaderPool struct {
	reader   *FakeDatabaseReader
	borrowed bool
	returned bool
}

func NewTestFakeDatabaseReaderPool(reader *FakeDatabaseReader) *FakeDatabaseReaderPool {
	p := new(FakeDatabaseReaderPool)
	p.reader = reader
	return p
}

func (p *FakeDatabaseReaderPool) Borrow() DatabaseReader {
	p.borrowed = true
	return p.reader
}

func (p *FakeDatabaseReaderPool) Return(r DatabaseReader) {
	p.returned = true
}

func (p *FakeDatabaseReaderPool) Reset() {
	p.borrowed = false
	p.returned = false
}

func (p *FakeDatabaseReaderPool) Close() error {
	return nil
}

type FakeDatabaseReader struct {
	begin  bool
	commit bool
}

func (reader *FakeDatabaseReader) Reset() {
	reader.begin = false
	reader.commit = false
}

func (reader *FakeDatabaseReader) Open() error {
	return nil
}

func (reader *FakeDatabaseReader) Begin() error {
	reader.begin = true
	return nil
}

func (reader *FakeDatabaseReader) Commit() error {
	reader.commit = true
	return nil
}

type FakeDatabaseWriter struct {
	begin    bool
	commit   bool
	rollback bool

	beginerr    bool
	commiterr   bool
	roolbackerr bool
	putdocerror bool
	getdocerror bool
}

func (writer *FakeDatabaseWriter) Open() error {
	return nil
}

func (writer *FakeDatabaseWriter) Close() error {
	return nil
}

func (writer *FakeDatabaseWriter) Begin() error {
	if writer.beginerr {
		return ErrInternalError
	}
	writer.begin = true
	return nil
}

func (writer *FakeDatabaseWriter) Commit() error {
	if writer.commiterr {
		return ErrInternalError
	}
	writer.commit = true
	return nil
}

func (writer *FakeDatabaseWriter) Rollback() error {
	if writer.roolbackerr {
		return ErrInternalError
	}
	writer.rollback = true
	return nil
}

func (writer *FakeDatabaseWriter) Reset() error {
	writer.rollback = false
	writer.begin = false
	writer.commit = false
	return nil
}

func (writer *FakeDatabaseWriter) ExecBuildScript() error {
	return nil
}

func (writer *FakeDatabaseWriter) Vacuum() error {
	return nil
}

func (writer *FakeDatabaseWriter) GetDocumentRevisionByID(docID string) (*Document, error) {
	if writer.getdocerror {
		return nil, ErrInternalError
	}
	if docID == "1" {
		return ParseDocument([]byte(`{"_id":1, "_version" :1}`))
	}
	if docID == "2" {
		return ParseDocument([]byte(`{"_id":1, "_version" :2, "_deleted":true}`))
	}
	if docID == "3" {
		return nil, ErrInternalError
	}
	return nil, nil
}

func (writer *FakeDatabaseWriter) PutDocument(updateSeqID string, newDoc *Document, currentDoc *Document) error {
	if writer.putdocerror {
		return ErrInternalError
	}
	return nil
}

func (reader *FakeDatabaseReader) GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error) {
	return ParseDocument([]byte(`{"_id":2, "_version" :1}`))
}

func (reader *FakeDatabaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {
	return ParseDocument([]byte(`{"_id":1, "_version" :1}`))
}

func (reader *FakeDatabaseReader) GetDocumentByID(ID string) (*Document, error) {
	return ParseDocument([]byte(`{"_id":3, "_version" :1, "test": "test"}`))
}

func (reader *FakeDatabaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {
	return ParseDocument([]byte(`{"_id":4, "_version" :1, "test": "test"}`))
}

func (reader *FakeDatabaseReader) GetAllDesignDocuments() ([]*Document, error) {
	return nil, nil
}

func (db *FakeDatabaseReader) GetChanges(since string, limit int) ([]byte, error) {
	return nil, nil
}

func (db *FakeDatabaseReader) GetLastUpdateSequence() string {
	return "GiJYxpHX92iFe_tvtuAICAkmdnOMXEm1erk_0RkfgCC7JHvbN64M2bv5CxtZrfSrrA1b48HGNvV57GbHuqVJrRv9L_1NuceGQQt0OGUs7BskxKjW51aylNDA5Zjqzir44wrUMm6x5W"
}

func (db *FakeDatabaseReader) GetDocumentCount() int {
	return 3
}

func (reader *FakeDatabaseReader) Close() error {
	return nil
}

func TestDBLoadUpdateSeqID(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.Open()

	l := reader.GetLastUpdateSequence()

	if db.UpdateSeq != l {
		t.Errorf("failed to load last update seq id.")
	}
	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}

	pool.Reset()
	reader.Reset()

	if db.GetLastUpdateSequence() != l {
		t.Errorf("failed to load last update seq id.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}
}

func TestDBDocumentCount(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	v := db.GetDocumentCount()

	if v != 3 {
		t.Errorf("expected %d, got %d", 3, v)
	}

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}
}

func TestDBGetChanges(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	_, _ = db.GetChanges("", 0)

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}
}

func TestDBGetDesignDocuments(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	_, _ = db.GetAllDesignDocuments()

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}
}

func TestDBStat(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool

	db.Open()

	stat := db.Stat()

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}

	if stat.DocCount != 3 {
		t.Errorf("expected doc count %d, got %d", 3, stat.DocCount)
	}

	if stat.UpdateSeq != reader.GetLastUpdateSequence() {
		t.Errorf("expected to load last update seqid. failed")
	}
}

func TestDBGetDocumentRevisionByID(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	odoc, err := db.GetDocument(doc, false)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if odoc.ID != "1" {
		t.Errorf("expected doc id %s, got %s", "1", odoc.ID)
	}

	if odoc.Version != 1 {
		t.Errorf("expected doc version %d, got %d", 1, odoc.Version)
	}

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}

	if string(odoc.Data) != (`{}`) {
		t.Errorf("data mismatch.")
	}
}

func TestDBGetDocumentRevisionByIDandVersion(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool

	doc, _ := ParseDocument([]byte(`{"_id":2, "_version":1}`))
	odoc, err := db.GetDocument(doc, false)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if odoc.ID != "2" {
		t.Errorf("expected doc id %s, got %s", "2", odoc.ID)
	}

	if odoc.Version != 1 {
		t.Errorf("expected doc version %d, got %d", 1, odoc.Version)
	}

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}

	if string(odoc.Data) != (`{}`) {
		t.Errorf("data mismatch.")
	}
}

func TestDBGetDocumentByID(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool

	doc, _ := ParseDocument([]byte(`{"_id":3}`))
	odoc, err := db.GetDocument(doc, true)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if odoc.ID != "3" {
		t.Errorf("expected doc id %s, got %s", "3", odoc.ID)
	}

	if odoc.Version != 1 {
		t.Errorf("expected doc version %d, got %d", 1, odoc.Version)
	}

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}

	if string(odoc.Data) != (`{"test":"test"}`) {
		t.Errorf("data mismatch.")
	}
}

func TestDBGetDocumentByIDandVersion(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool

	doc, _ := ParseDocument([]byte(`{"_id":4, "_version":1}`))
	odoc, err := db.GetDocument(doc, true)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if odoc.ID != "4" {
		t.Errorf("expected doc id %s, got %s", "4", odoc.ID)
	}

	if odoc.Version != 1 {
		t.Errorf("expected doc version %d, got %d", 1, odoc.Version)
	}

	if !reader.begin || !reader.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if !pool.borrowed || !pool.returned {
		t.Errorf("expected to call borrow and return, failed.")
	}

	if string(odoc.Data) != (`{"test":"test"}`) {
		t.Errorf("data mismatch.")
	}
}

func TestDBPutDocumentNewDocID(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{}`))
	odoc, err := db.PutDocument(doc)
	if err != nil {
		t.Errorf("unable put document")
	}

	if !writer.begin || !writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc.ID == "" || odoc.Version != 1 {
		t.Errorf("expected to have id and version, failed.")
	}

	if db.UpdateSeq == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
	}
}

func TestDBPutDocumentNewDocWithID(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "4"}`))
	odoc, err := db.PutDocument(doc)
	if err != nil {
		t.Errorf("unable put document")
	}

	if !writer.begin || !writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc.ID == "" || odoc.Version != 1 {
		t.Errorf("expected to have id and version, failed.")
	}

	if db.UpdateSeq == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
	}
}

func TestDBPutDocumentConflict(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	odoc, err := db.PutDocument(doc)
	if err == nil {
		t.Errorf("expected fail put document. ")
	}

	if err != nil && err != ErrDocConflict {
		t.Errorf("expected fail put document with %s ", ErrDocConflict)
	}

	if !writer.begin || !writer.rollback || writer.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("expected to have nil, failed.")
	}

	if db.UpdateSeq != db.GetLastUpdateSequence() {
		t.Errorf("unexpected to have new seq id, failed.")
	}
}

func TestDBPutDocumentConflict1(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id":1, "_version":2}`))
	odoc, err := db.PutDocument(doc)
	if err == nil {
		t.Errorf("expected fail put document. ")
	}

	if err != nil && err != ErrDocConflict {
		t.Errorf("expected fail put document with %s ", ErrDocConflict)
	}

	if !writer.begin || !writer.rollback || writer.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("expected to have nil, failed.")
	}

	if db.UpdateSeq != db.GetLastUpdateSequence() {
		t.Errorf("unexpected to have new seq id, failed.")
	}
}

func TestDBPutDocumentUpdateDoc(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "1", "_version":1}`))
	odoc, err := db.PutDocument(doc)
	if err != nil {
		t.Errorf("unable put document")
	}

	if !writer.begin || !writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc.ID == "" || odoc.Version != 2 {
		t.Errorf("expected to have id and version, failed.")
	}

	if db.UpdateSeq == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
	}
}

func TestDBPutDocumentUpdateDocNoDocExists(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "151", "_version":4}`))
	_, err := db.PutDocument(doc)
	if err != nil {
		t.Errorf("unexpected err %s", ErrDocConflict)
	}

	if !writer.begin || !writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and rollback, failed.")
	}

	if db.UpdateSeq == db.GetLastUpdateSequence() {
		t.Errorf("expected to have same seq id, failed.")
	}
}

func TestDBPutDocumentBeginError(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	writer.beginerr = true
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "12"}`))
	odoc, err := db.PutDocument(doc)
	if err == nil {
		t.Errorf("unable put document")
	}

	if err != nil && err != ErrInternalError {
		t.Errorf("expected to fail with %s, failed", ErrInternalError)
	}

	if writer.begin || writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("unexpected to have return doc, failed.")
	}

	if db.UpdateSeq != db.GetLastUpdateSequence() {
		t.Errorf("unexpected to have new seq id, failed.")
	}
}

func TestDBPutDocumentCommitError(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	writer.commiterr = true
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "12"}`))
	odoc, err := db.PutDocument(doc)
	if err == nil {
		t.Errorf("unable put document")
	}

	if err != nil && err != ErrInternalError {
		t.Errorf("expected to fail with %s, failed", ErrInternalError)
	}

	if !writer.begin || writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("unexpected to have return doc, failed.")
	}

	if db.UpdateSeq != db.GetLastUpdateSequence() {
		t.Errorf("unexpected to have new seq id, failed.")
	}
}

func TestDBPutDocumentRollbackError(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	writer.roolbackerr = true
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "12"}`))
	_, _ = db.PutDocument(doc)

	if !writer.begin || !writer.commit || writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}
}

func TestDBPutDocumentWriterPutDocumentError(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	writer.putdocerror = true
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "12"}`))
	odoc, err := db.PutDocument(doc)
	if err == nil {
		t.Errorf("unable put document")
	}

	if err != nil && err != ErrInternalError {
		t.Errorf("expected to fail with %s, failed", ErrInternalError)
	}

	if !writer.begin || writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("unexpected to have return doc, failed.")
	}

	if db.UpdateSeq != db.GetLastUpdateSequence() {
		t.Errorf("unexpected to have new seq id, failed.")
	}
}

func TestDBPutDocumentWriterGetDocumentError(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	writer.getdocerror = true
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "12"}`))
	odoc, err := db.PutDocument(doc)
	if err == nil {
		t.Errorf("expected fail put document. ")
	}

	if err != nil && !errors.Is(err, ErrInternalError) {
		t.Errorf("expected fail put document with %s, got %s", ErrInternalError, err.Error())
	}

	if !writer.begin || !writer.rollback || writer.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("expected to have nil, failed.")
	}

	if db.UpdateSeq != db.GetLastUpdateSequence() {
		t.Errorf("unexpected to have new seq id, failed.")
	}
}

func TestDBPutDocumentUpdateDeletedDoc(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "2"}`))
	odoc, err := db.PutDocument(doc)
	if err != nil {
		t.Errorf("unable put document")
	}

	if !writer.begin || !writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc.ID == "" || odoc.Version != 3 {
		t.Errorf("expected to have id and version, failed.")
	}

	if db.UpdateSeq == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
	}

	doc, _ = ParseDocument([]byte(`{"_id": "2", "_version": 1}`))
	odoc, err = db.PutDocument(doc)
	if err == nil {
		t.Errorf("expected to fail, when you update deleted doc with old verison")
	}
}

func TestDBDeleteDocument(t *testing.T) {
	db := &Database{}
	reader := new(FakeDatabaseReader)
	writer := new(FakeDatabaseWriter)
	db.idSeq = NewSequenceUUIDGenarator()
	pool := NewTestFakeDatabaseReaderPool(reader)
	db.readers = pool
	db.writer = writer
	db.Open()

	doc, _ := ParseDocument([]byte(`{"_id": "1", "_version":1}`))
	odoc, err := db.DeleteDocument(doc)
	if err != nil {
		t.Errorf("unable delete document")
	}

	if !writer.begin || !writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc.ID == "" || odoc.Version != 2 || !odoc.Deleted {
		t.Errorf("expected to have id and version, failed.")
	}

	if db.UpdateSeq == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
	}
}

type FakeViewManager struct {
}

func (sl *FakeViewManager) SetupViews(db *Database) error {
	return nil
}

func (sl *FakeViewManager) Initialize(db *Database) error {
	return nil
}

func (sl *FakeViewManager) ListViewFiles() ([]string, error) {
	return nil, nil
}

func (sl *FakeViewManager) OpenView(viewName string, ddoc *DesignDocument) error {
	return nil
}

func (sl *FakeViewManager) SelectView(updateSeqID string, doc *Document, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	return nil, nil
}

func (sl *FakeViewManager) Close() error {
	return nil
}

func (sl *FakeViewManager) Vacuum() error {
	return nil
}

func (sl *FakeViewManager) UpdateDesignDocument(doc *Document) error {
	return nil
}

func (sl *FakeViewManager) ValidateDesignDocument(doc *Document) error {
	return nil
}

func (sl *FakeViewManager) CalculateSignature(ddocv *DesignDocumentView) string {
	return ""
}

func (sl *FakeViewManager) ParseQueryParams(query string) (string, []string) {
	return "", nil
}

type FakeFileHandler struct {
}

func (sl *FakeFileHandler) IsFileExists(path string) bool {
	return false
}

func (sl *FakeFileHandler) MkdirAll(path string) error {
	return nil
}

type FakeServiceLocator struct {
}

func (sl *FakeServiceLocator) GetFileHandler() FileHandler {
	return &FakeFileHandler{}
}

func (sl *FakeServiceLocator) GetDatabaseWriter(connectionString string) DatabaseWriter {
	return &FakeDatabaseWriter{}
}

func (sl *FakeServiceLocator) GetDatabaseReader(connectionString string) DatabaseReader {
	return &FakeDatabaseReader{}
}

func (sl *FakeServiceLocator) GetDatabaseReaderPool(connectionString string, limit int) DatabaseReaderPool {
	reader := &FakeDatabaseReader{}
	return NewTestFakeDatabaseReaderPool(reader)
}

func (sl *FakeServiceLocator) GetViewManager(dbName, absoluteDatabasePath, viewPath string) ViewManager {
	return &FakeViewManager{}
}

func (sl *FakeServiceLocator) GetView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View {
	return nil
}

func TestNewDatabase(t *testing.T) {
	db, _ := NewDatabase("testdb", "./data/dbs", "./data/mrviews", true, &FakeServiceLocator{})

	_ = db
}
