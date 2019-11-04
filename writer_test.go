package main

import (
	"os"
	"testing"
)

var testConnectionString string = "./data/dbs/testdb.db"

func TestNewWriter(t *testing.T) {
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

	writer.Close()

	os.Remove(testConnectionString)
}

func TestNewWriter1(t *testing.T) {
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

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	writer.Commit()
	writer.Close()

	os.Remove(testConnectionString)
}

func TestNewWriterDocNotFound(t *testing.T) {
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
