package main

import (
	"os"
	"testing"
)

var testConnectionString string = "./data/dbs/testdb.db"

func TestWriterPutDocument(t *testing.T) {
	os.Remove(testConnectionString)

	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
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

/*func TestWriterPutDocumentWithConflict(t *testing.T) {
	os.Remove(testConnectionString)

	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter(testConnectionString)
	writer.Open()

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	doc, _ = ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		t.Errorf("expected %s, failed.", ErrDocConflict)
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		t.Errorf("expected %s, failed.", ErrDocConflict)
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}

func TestWriterPutDocumentWithDeplicateSeqID(t *testing.T) {
	os.Remove(testConnectionString)

	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter(testConnectionString)
	writer.Open()

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	err := writer.PutDocument("seqID", doc, nil)
	if err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	doc, _ = ParseDocument([]byte(`{"_id":2}`))
	err = writer.PutDocument("seqID", doc, nil)
	if err == nil {
		t.Errorf("expected %s, failed.", ErrInternalError)
	}
	if err != nil && err != ErrInternalError {
		t.Errorf("expected %s, got %s", ErrInternalError, err.Error())
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":2}`))
	err = writer.PutDocument("seqID", doc, nil)
	if err == nil {
		t.Errorf("expected %s, failed.", ErrInternalError)
	}
	if err != nil && err != ErrInternalError {
		t.Errorf("expected %s, got %s", ErrInternalError, err.Error())
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}
*/

func TestWriterDeleteDocument(t *testing.T) {
	os.Remove(testConnectionString)

	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
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

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocNotFound {
		t.Errorf("expected %s, got doc or err %s", ErrDocNotFound, err)
	}

	writer.Commit()

	writer.Close()

	os.Remove(testConnectionString)
}

func TestWriterDocNotFound(t *testing.T) {
	os.Remove(testConnectionString)

	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
	writer.Open(testConnectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		t.Errorf("unable to setup database")
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocNotFound {
		t.Errorf("expected %s, got doc or err %s", ErrDocNotFound, err)
	}

	writer.Commit()
	writer.Close()

	os.Remove(testConnectionString)
}
