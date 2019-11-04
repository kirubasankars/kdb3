package main

import (
	"os"
	"testing"
)

var testConnectionString string = "./data/dbs/testdb.db"

func TestWriterPutDocument(t *testing.T) {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}

func TestWriterPutDocumentWithConflict(t *testing.T) {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	doc, _ = ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err == nil {
		t.Errorf("expected %s, failed.", DOC_CONFLICT)
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err == nil {
		t.Errorf("expected %s, failed.", DOC_CONFLICT)
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}

func TestWriterPutDocumentWithDeplicateSeqID(t *testing.T) {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	doc, _ = ParseDocument([]byte(`{"_id":2}`))
	err := writer.PutDocument("seqID", doc, nil)
	if err == nil {
		t.Errorf("expected %s, failed.", INTERAL_ERROR)
	}
	if err != nil && err.Error() != INTERAL_ERROR {
		t.Errorf("expected %s, got %s", INTERAL_ERROR, err.Error())
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":2}`))
	err = writer.PutDocument("seqID", doc, nil)
	if err == nil {
		t.Errorf("expected %s, failed.", INTERAL_ERROR)
	}
	if err != nil && err.Error() != INTERAL_ERROR {
		t.Errorf("expected %s, got %s", INTERAL_ERROR, err.Error())
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}

func TestWriterDeleteDocument(t *testing.T) {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":1, "_version":1, "_deleted":true}`))
	if err := writer.PutDocument("seqID2", doc, nil); err != nil {
		t.Errorf("unable to delete document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err.Error() != DOC_NOT_FOUND {
		t.Errorf("expected %s, got doc or err %s", DOC_NOT_FOUND, err)
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}

func TestWriterDocNotFound(t *testing.T) {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err.Error() != DOC_NOT_FOUND {
		t.Errorf("expected %s, got doc or err %s", DOC_NOT_FOUND, err)
	}

	writer.Commit()
	writer.Close()

	os.Remove(testConnectionString)
}
