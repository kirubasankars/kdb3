package main

import (
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
