package main

import (
	"os"
	"testing"
)

var testConnectionString string = "./data/dbs/testdb.db"

func setupTestWriterDatabase() error {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	if err := writer.ExecBuildScript(); err != nil {
		return err
	}

	writer.Close()

	return nil
}

func deleteTestWriterDatabase() {
	os.Remove(testConnectionString)
}

func TestWriterPutDocument(t *testing.T) {
	if err := setupTestWriterDatabase(); err != nil {
		t.Errorf("unable to setup test database for writer %s", err)
	}

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	doc, _ := ParseDocument([]byte(`{"_id":1,"name":"name"}`))
	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	currentDoc, _ := writer.GetDocumentRevisionByID("1")
	newDoc, _ := ParseDocument([]byte(`{"_id":1,"_version":1,"name":"name"}`))
	if err := writer.PutDocument("seqID2", newDoc, currentDoc); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	writer.Close()

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	doc, err := reader.GetDocumentByID("1")
	if err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}
	if len(doc.Data) < 2 {
		t.Errorf("unable to load document data, error %s", err.Error())
	}
	reader.Close()

	deleteTestWriterDatabase()
}

func TestWriterPutDocumentWithConflict(t *testing.T) {
	if err := setupTestWriterDatabase(); err != nil {
		t.Errorf("unable to setup test database for writer %s", err)
	}

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	doc, _ := ParseDocument([]byte(`{"_id":1}`))

	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	if err := writer.PutDocument("seqID2", doc, nil); err == nil {
		t.Errorf("expected err %s, failed.", ErrDocConflict)
	}

	if err := writer.PutDocument("seqID3", doc, nil); err == nil {
		t.Errorf("expected err %s, failed.", ErrDocConflict)
	}

	writer.Close()
	deleteTestWriterDatabase()
}

func TestWriterPutDocumentWithDeplicateSeqID(t *testing.T) {
	if err := setupTestWriterDatabase(); err != nil {
		t.Errorf("unable to setup test database for writer %s", err)
	}

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	doc1, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc1, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	doc2, _ := ParseDocument([]byte(`{"_id":2}`))
	err := writer.PutDocument("seqID", doc2, nil)
	if err == nil {
		t.Errorf("expected err %s, failed.", ErrInternalError)
	}
	if err != nil && err != ErrInternalError {
		t.Errorf("expected err %s, got %s", ErrInternalError, err.Error())
	}

	err = writer.PutDocument("seqID", doc2, nil)
	if err == nil {
		t.Errorf("expected err %s, failed.", ErrInternalError)
	}
	if err != nil && err != ErrInternalError {
		t.Errorf("expected err %s, got %s", ErrInternalError, err.Error())
	}

	writer.Close()
	deleteTestWriterDatabase()
}

func TestWriterDeleteDocument(t *testing.T) {
	if err := setupTestWriterDatabase(); err != nil {
		t.Errorf("unable to setup test database for writer %s", err)
	}

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	doc, _ = ParseDocument([]byte(`{"_id":1, "_version":1, "_deleted":true}`))
	if err := writer.PutDocument("seqID2", doc, nil); err != nil {
		t.Errorf("unable to delete document, error %s", err.Error())
	}

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocNotFound {
		t.Errorf("expected err %s, got err %s", ErrDocNotFound, err)
	}

	writer.Close()
	deleteTestWriterDatabase()
}

func TestWriterDocNotFound(t *testing.T) {
	if err := setupTestWriterDatabase(); err != nil {
		t.Errorf("unable to setup test database for writer %s", err)
	}

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocNotFound {
		t.Errorf("expected err %s, got %s", ErrDocNotFound, err)
	}

	writer.Close()

	deleteTestWriterDatabase()
}

func TestWriterVaccum(t *testing.T) {
	if err := setupTestWriterDatabase(); err != nil {
		t.Errorf("unable to setup test database for writer %s", err)
	}

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	writer.Vacuum()

	writer.Close()

	deleteTestWriterDatabase()
}
