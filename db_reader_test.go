package main

import (
	"testing"
)

var readerTestConnectionString = "file:test.db?mode=memory&cache=shared"

func openTestDatabaseForReader() func() {
	var writer DefaultDatabaseWriter
	writer.reader = new(DefaultDatabaseReader)
	writer.connectionString = readerTestConnectionString

	writer.Open()

	writer.Begin()
	writer.ExecBuildScript()

	doc, _ := ParseDocument([]byte(`{"_id":1, "_version":1}`))
	writer.PutDocument("seqID1", doc)

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":1}`))
	writer.PutDocument("seqID2", doc)

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":2, "_deleted":true}`))
	writer.PutDocument("seqID3", doc)

	doc, _ = ParseDocument([]byte(`{"_id":"invalid", "_version":1}`))
	writer.PutDocument("seqID3", doc)

	doc, _ = ParseDocument([]byte(`{"_id":"_design/_views", "_version":1, "_kind": "design", "test":"test"}`))
	writer.PutDocument("seqID4", doc)

	writer.Commit()

	writer.conn.Exec("UPDATE documents SET deleted = 'aa' WHERE doc_id = 'invalid'")

	return func() {
		writer.Close()
	}
}

func TestReaderInvalidConnectionString(t *testing.T) {
	var reader DefaultDatabaseReader
	reader.connectionString = "."
	err := reader.Open()
	if err == nil {
		t.Errorf("expected error invalid db name")
	}
}

func TestReaderGetDocumentByID(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()
	reader.Begin()

	doc, err := reader.GetDocumentByID("1")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByID("2")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByID("_design/_views")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "design") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByID("invalid")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	doc, err = reader.GetDocumentByID("nothing")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentRevisionByID(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	doc, err := reader.GetDocumentRevisionByID("1")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentRevisionByID("2")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentRevisionByID("_design/_views")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "design") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentRevisionByID("invalid")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	doc, err = reader.GetDocumentRevisionByID("nothing")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentByIDandVersion(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	doc, err := reader.GetDocumentByIDandVersion("1", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByIDandVersion("2", 2)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByIDandVersion("_design/_views", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "design") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByIDandVersion("invalid", 1)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	doc, err = reader.GetDocumentByIDandVersion("nothing", 1)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentRevisionByIDandVersion(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentRevisionByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	doc, err := reader.GetDocumentRevisionByIDandVersion("1", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentRevisionByIDandVersion("2", 2)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true && doc.Kind == "") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentRevisionByIDandVersion("_design/_views", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false && doc.Kind == "design") {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentRevisionByIDandVersion("invalid", 1)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	doc, err = reader.GetDocumentRevisionByIDandVersion("nothing", 1)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentCount(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

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

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()
	expected := `{"results":[{"seq":"seqID4","version":1,"id":"_design/_views"},{"seq":"seqID3","version":2,"id":"2","deleted":true},{"seq":"seqID3","version":1,"id":"invalid"},{"seq":"seqID1","version":1,"id":"1"}]}`
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
