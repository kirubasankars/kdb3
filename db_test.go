package main

import (
	"errors"
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

func (reader *FakeDatabaseReader) Open(path string) error {
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

func (writer *FakeDatabaseWriter) Open(path string) error {
	return nil
}

func (writer *FakeDatabaseWriter) Close() error {
	return nil
}

func (writer *FakeDatabaseWriter) Begin() error {
	if writer.beginerr {
		return errors.New(INTERAL_ERROR)
	}
	writer.begin = true
	return nil
}

func (writer *FakeDatabaseWriter) Commit() error {
	if writer.commiterr {
		return errors.New(INTERAL_ERROR)
	}
	writer.commit = true
	return nil
}

func (writer *FakeDatabaseWriter) Rollback() error {
	if writer.roolbackerr {
		return errors.New(INTERAL_ERROR)
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
		return nil, errors.New(INTERAL_ERROR)
	}
	if docID == "1" {
		return ParseDocument([]byte(`{"_id":1, "_version" :1}`))
	}
	if docID == "2" {
		return ParseDocument([]byte(`{"_id":1, "_version" :2, "_deleted":true}`))
	}
	if docID == "3" {
		return nil, errors.New(INTERAL_ERROR)
	}
	return nil, nil
}

func (writer *FakeDatabaseWriter) PutDocument(updateSeqID string, newDoc *Document, currentDoc *Document) error {
	if writer.putdocerror {
		return errors.New(INTERAL_ERROR)
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

func (db *FakeDatabaseReader) GetChanges(since string) []byte {
	return nil
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

	if db.updateSeqID != l {
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
	_ = db.GetChanges("")

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

	if db.updateSeqID == db.GetLastUpdateSequence() {
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

	if db.updateSeqID == db.GetLastUpdateSequence() {
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

	if err != nil && err.Error() != DOC_CONFLICT {
		t.Errorf("expected fail put document with %s ", DOC_CONFLICT)
	}

	if !writer.begin || !writer.rollback || writer.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("expected to have nil, failed.")
	}

	if db.updateSeqID != db.GetLastUpdateSequence() {
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

	if err != nil && err.Error() != DOC_CONFLICT {
		t.Errorf("expected fail put document with %s ", DOC_CONFLICT)
	}

	if !writer.begin || !writer.rollback || writer.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("expected to have nil, failed.")
	}

	if db.updateSeqID != db.GetLastUpdateSequence() {
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

	if db.updateSeqID == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
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

	if err != nil && err.Error() != INTERAL_ERROR {
		t.Errorf("expected to fail with %s, failed", INTERAL_ERROR)
	}

	if writer.begin || writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("unexpected to have return doc, failed.")
	}

	if db.updateSeqID != db.GetLastUpdateSequence() {
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

	if err != nil && err.Error() != INTERAL_ERROR {
		t.Errorf("expected to fail with %s, failed", INTERAL_ERROR)
	}

	if !writer.begin || writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("unexpected to have return doc, failed.")
	}

	if db.updateSeqID != db.GetLastUpdateSequence() {
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

	if err != nil && err.Error() != INTERAL_ERROR {
		t.Errorf("expected to fail with %s, failed", INTERAL_ERROR)
	}

	if !writer.begin || writer.commit || !writer.rollback {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("unexpected to have return doc, failed.")
	}

	if db.updateSeqID != db.GetLastUpdateSequence() {
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

	if err != nil && err.Error() != INTERAL_ERROR {
		t.Errorf("expected fail put document with %s, got %s", DOC_CONFLICT, err.Error())
	}

	if !writer.begin || !writer.rollback || writer.commit {
		t.Errorf("expected to call begin and commit, failed.")
	}

	if odoc != nil {
		t.Errorf("expected to have nil, failed.")
	}

	if db.updateSeqID != db.GetLastUpdateSequence() {
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

	if db.updateSeqID == db.GetLastUpdateSequence() {
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

	if db.updateSeqID == db.GetLastUpdateSequence() {
		t.Errorf("expected to have new seq id, failed.")
	}
}
