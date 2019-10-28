package main

import (
	"log"
	"os"
	"testing"
)

func TestNewWriter(t *testing.T) {
	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	connectionString := "testdb"
	writer.Open(connectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		log.Fatal(err)
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))

	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		log.Fatal(err)
	}

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		log.Fatal(err)
	}

	writer.Commit()

	writer.Close()

	os.Remove(connectionString)
}

func TestNewWriter1(t *testing.T) {
	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	connectionString := "testdb"
	writer.Open(connectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		log.Fatal(err)
	}

	doc, _ := ParseDocument([]byte(`{"_id":1}`))

	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		log.Fatal(err)
	}

	writer.Commit()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
		log.Fatal(err)
	}

	writer.Close()

	os.Remove(connectionString)
}

func TestNewWriterNoDoc(t *testing.T) {
	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	connectionString := "testdb"
	writer.Open(connectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		log.Fatal(err)
	}

	writer.Commit()

	writer.Begin()

	doc, _ := ParseDocument([]byte(`{"_id":1}`))

	if err := writer.PutDocument("seqID", doc, nil); err != nil {
		log.Fatal(err)
	}

	writer.Rollback()

	writer.Begin()

	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err.Error() != "doc_not_found" {
		log.Fatal(err)
	}

	writer.Close()

	os.Remove(connectionString)
}
