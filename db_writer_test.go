package main

import (
	"database/sql"
	"fmt"
	"testing"
)

var writerTestConnectionString string = "file:test.db?mode=memory&cache=shared"

func openTestDatabaseForWriter() func() {
	con, _ := sql.Open("sqlite3", writerTestConnectionString)
	tx, _ := con.Begin()
	tx.Exec("CREATE TABLE temp(a)")
	tx.Commit()
	return func() {
		con.Close()
	}
}

func setupTestDatabaseForWriter() {
	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	err := writer.Open()
	if err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	err = writer.Begin()
	if err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	if err := writer.ExecBuildScript(); err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	writer.Commit()

	writer.Close()
}

func TestWriterPutDocument(t *testing.T) {
	dbHandle := openTestDatabaseForWriter()
	defer dbHandle()
	setupTestDatabaseForWriter()

	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	writer.Open()

	writer.Begin()

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
}

func TestWriterDeleteDocument(t *testing.T) {
	dbHandle := openTestDatabaseForWriter()
	defer dbHandle()
	setupTestDatabaseForWriter()

	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	writer.Open()

	writer.Begin()

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

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocumentNotFound {
		t.Errorf("expected %s, got doc or err %s", ErrDocumentNotFound, err)
	}

	writer.Commit()

	writer.Close()
}

func TestWriterDocNotFound(t *testing.T) {
	dbHandle := openTestDatabaseForWriter()
	defer dbHandle()
	setupTestDatabaseForWriter()

	var writer DefaultDatabaseWriter
	writer.connectionString = writerTestConnectionString
	writer.reader = new(DefaultDatabaseReader)
	writer.Open()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocumentNotFound {
		t.Errorf("expected %s, got doc or err %s", ErrDocumentNotFound, err)
		fmt.Println(err)
	}

	writer.Commit()
	writer.Close()
}
