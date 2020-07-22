package main

import (
	"database/sql"
	"fmt"
	"testing"
)

var readerTestConnectionString string = "file:test.db?mode=memory&cache=shared"

func openTestDatabaseForReader() func() {
	con, _ := sql.Open("sqlite3", readerTestConnectionString)
	tx, _ := con.Begin()
	tx.Exec("CREATE TABLE temp(a)")
	tx.Commit()
	return func() {
		con.Close()
	}
}

func setupTestDatabaseForReader() {
	var writer DefaultDatabaseWriter
	writer.reader = new(DefaultDatabaseReader)
	writer.connectionString = readerTestConnectionString

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

	doc, _ := ParseDocument([]byte(`{"_id":1, "_version":1}`))
	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":1}`))
	if err := writer.PutDocument("seqID2", doc, nil); err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":2, "_deleted":true}`))
	if err := writer.PutDocument("seqID3", doc, nil); err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	doc, _ = ParseDocument([]byte(`{"_id":"_design/_views", "_version":1, "test":"test"}`))
	if err := writer.PutDocument("seqID4", doc, nil); err != nil {
		fmt.Println("unable to setup test database")
		return
	}

	writer.Commit()

	writer.Close()
}

func TestReaderGetDocumentByID(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()
	reader.Begin()

	doc, err := reader.GetDocumentByID("1")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}
	if doc.Version != 1 {
		t.Errorf("missing doc version")
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentRevisionByID(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	doc, err := reader.GetDocumentRevisionByID("1")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}
	if doc.Version != 1 {
		t.Errorf("missing doc version")
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentByIDandVersion(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentRevisionByIDandVersion(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentRevisionByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentCount(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	docCount, deletedDocCount := reader.GetDocumentCount()
	if docCount != 2 && deletedDocCount != 1 {
		t.Errorf("expected %d rows, got %d", 2, docCount)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetLastUpdateSequence(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	seqID := reader.GetLastUpdateSequence()
	if seqID != "seqID4" {
		t.Errorf("expected last seqID as %s, got %s", "seqID4", seqID)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetChanges(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()
	expected := `{"results":[{"seq":"seqID4","version":1,"id":"_design/_views"},{"seq":"seqID3","version":2,"id":"2","deleted":true},{"seq":"seqID1","version":1,"id":"1"}]}`
	changes, _ := reader.GetChanges("", 999)
	if string(changes) != expected {
		t.Errorf("expected changes as  \n %s \n, got \n %s \n", expected, string(changes))
	}
	reader.Commit()
	reader.Close()
}

func TestReaderGetAllDesignDocuments(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()
	setupTestDatabaseForReader()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	docs, err := reader.GetAllDesignDocuments()
	if err != nil {
		t.Errorf("unable to get design docs, %s", err)
	}
	if len(docs) != 1 {
		t.Errorf("expected %d docs, got %d ", 1, len(docs))
	}

	if len(docs) > 0 && docs[0].ID != "_design/_views" {
		t.Errorf("expected %s, got %s", "_design/_views", docs[0].ID)
	}

	reader.Commit()

	reader.Close()
}
