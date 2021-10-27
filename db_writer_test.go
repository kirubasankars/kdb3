package main

import (
	"testing"
)

var writerTestConnectionString string = "file:test.db?mode=memory&cache=shared"

func openTestDatabaseForWriter() func() {
	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)

	writer.Open(true)

	writer.Begin()

	doc, _ := ParseDocument([]byte(`{"_id":1, "_version":1}`))
	writer.PutDocument("seqID1", doc)

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":1}`))
	writer.PutDocument("seqID2", doc)

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":2, "_deleted":true}`))
	writer.PutDocument("seqID3", doc)

	doc, _ = ParseDocument([]byte(`{"_id":"invalid", "_version":1}`))
	writer.PutDocument("seqID3", doc)

	doc, _ = ParseDocument([]byte(`{"_id":"_design/_views", "_version":1, "test":"test"}`))
	writer.PutDocument("seqID4", doc)

	writer.Commit()

	return func() {
		writer.Close()
	}
}

func TestWriterInvalidConnectionString(t *testing.T) {
	var writer DefaultDatabaseWriter
	writer.connectionString = "."
	err := writer.Open(false)
	if err == nil {
		t.Errorf("expected error invalid db name")
	}
}

func TestWriterPutDocument(t *testing.T) {
	dbHandle := openTestDatabaseForWriter()
	defer dbHandle()

	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	writer.Open(true)

	writer.Begin()

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID", doc); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	if _, err := writer.GetDocumentMetadataByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentMetadataByID("1"); err != nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":"new"}`))
	if err := writer.PutDocument("seqID", doc); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	writer.Rollback()

	writer.Begin()

	if _, err := writer.GetDocumentMetadataByID("new"); err == nil {
		t.Errorf("unable to get document, error %s", err.Error())
	}

	writer.Commit()

	writer.Close()
}

func TestWriterDeleteDocument(t *testing.T) {
	dbHandle := openTestDatabaseForWriter()
	defer dbHandle()

	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	writer.Open(true)

	writer.Begin()

	doc, _ := ParseDocument([]byte(`{"_id":1}`))
	if err := writer.PutDocument("seqID1", doc); err != nil {
		t.Errorf("unable to put document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	doc, _ = ParseDocument([]byte(`{"_id":1, "_version":1, "_deleted":true}`))
	if err := writer.PutDocument("seqID2", doc); err != nil {
		t.Errorf("unable to delete document, error %s", err.Error())
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentMetadataByID("1"); err == nil || err != ErrDocumentNotFound {
		t.Errorf("expected %s, got doc or err %s", ErrDocumentNotFound, err)
	}

	writer.Commit()

	writer.Close()
}

func TestWriterDocNotFound(t *testing.T) {
	dbHandle := openTestDatabaseForWriter()
	defer dbHandle()

	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	writer.Open(true)

	writer.Begin()

	if _, err := writer.GetDocumentMetadataByID("4"); err == nil || err != ErrDocumentNotFound {
		t.Errorf("expected %s, got doc or err %s", ErrDocumentNotFound, err)
	}

	writer.Commit()
	writer.Close()
}
