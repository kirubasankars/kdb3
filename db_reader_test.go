package main

import (
	"testing"
)

var readerTestConnectionString = "file:test.db?mode=memory&cache=shared"

func openTestDatabaseForReader() func() {
	var writer DefaultDatabaseWriter
	writer.reader = new(DefaultDatabaseReader)
	writer.connectionString = readerTestConnectionString

	writer.Open(true)

	writer.Begin()
	doc, _ := ParseDocument([]byte(`{"_id":"_design/_views", "_rev": 1, "test":"test"}`))
	writer.PutDocument(1, doc)

	doc, _ = ParseDocument([]byte(`{"_id":1, "_rev": 1}`))
	writer.PutDocument(2, doc)

	doc, _ = ParseDocument([]byte(`{"_id":2, "_rev": 1}`))
	writer.PutDocument(3, doc)

	doc, _ = ParseDocument([]byte(`{"_id":2, "_rev": 2, "_deleted":true}`))
	writer.PutDocument(4, doc)

	doc, _ = ParseDocument([]byte(`{"_id":"invalid", "_rev": 1}`))
	writer.PutDocument(5, doc)

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

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByID("2")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByID("_design/_views")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	//doc, err = reader.GetDocumentByID("invalid")
	//if err == nil {
	//	t.Errorf("expected error %s", ErrDocumentNotFound)
	//}

	_, err = reader.GetDocumentByID("nothing")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentMetadataByID(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	doc, err := reader.GetDocumentMetadataByID("1")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentMetadataByID("2")
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentMetadataByID("_design/_views")
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	//doc, err = reader.GetDocumentRevisionByID("invalid")
	//if err == nil {
	//	t.Errorf("expected error %s", ErrDocumentNotFound)
	//}

	_, err = reader.GetDocumentMetadataByID("nothing")
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

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByIDandVersion("2", 2)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentByIDandVersion("_design/_views", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	//doc, err = reader.GetDocumentByIDandVersion("invalid", 1)
	//if err == nil {
	//	t.Errorf("expected error %s", ErrDocumentNotFound)
	//}

	_, err = reader.GetDocumentByIDandVersion("nothing", 1)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentMetadataByIDandVersion(t *testing.T) {
	dbHandle := openTestDatabaseForReader()
	defer dbHandle()

	var reader DefaultDatabaseReader
	reader.connectionString = readerTestConnectionString
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentMetadataByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	doc, err := reader.GetDocumentMetadataByIDandVersion("1", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "1" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentMetadataByIDandVersion("2", 2)
	if err == nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	if !(doc.ID == "2" && doc.Version == 2 && doc.Deleted == true) {
		t.Errorf("unexpected doc values")
	}

	doc, err = reader.GetDocumentMetadataByIDandVersion("_design/_views", 1)
	if err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	if !(doc.ID == "_design/_views" && doc.Version == 1 && doc.Deleted == false) {
		t.Errorf("unexpected doc values")
	}

	_, err = reader.GetDocumentMetadataByIDandVersion("invalid", 1)
	if err != nil {
		t.Errorf("expected error %s", ErrDocumentNotFound)
	}

	_, err = reader.GetDocumentMetadataByIDandVersion("nothing", 1)
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

	update_seq := reader.GetLastUpdateSequence()
	if update_seq != 5 {
		t.Errorf("expected last seqID as %d, got %d", 5, update_seq)
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
	expected := `{"results":[{"update_seq":1,"id":"_design/_views","rev":1},{"update_seq":2,"id":"1","rev":1},{"update_seq":4,"id":"2","rev":2,"deleted":true},{"update_seq":5,"id":"invalid","rev":1}]}`
	changes, _ := reader.GetChanges(0, 999, false)
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
