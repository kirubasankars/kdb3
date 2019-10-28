package main

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestNewReader(t *testing.T) {
	connectionString := "testdb.db"
	os.Remove(connectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)

	writer.Open(connectionString)

	writer.Begin()

	if err := writer.ExecBuildScript(); err != nil {
		log.Fatal(err)
	}

	doc, _ := ParseDocument([]byte(`{"_id":1, "_version":1}`))

	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		log.Fatal(err)
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":1}`))

	if err := writer.PutDocument("seqID2", doc, nil); err != nil {
		log.Fatal(err)
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":1, "_deleted":true}`))
	if err := writer.PutDocument("seqID3", doc, nil); err.Error() != "doc_conflict" {
		fmt.Println("deplicate docID, version shouldn't allowed", err)
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":2, "_deleted":true}`))
	if err := writer.PutDocument("seqID3", doc, nil); err != nil {
		log.Fatal(err)
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":3, "_deleted":true}`))
	if err := writer.PutDocument("seqID3", doc, nil); err.Error() != "internal_error" {
		log.Fatal(err)
	}

	doc, _ = ParseDocument([]byte(`{"_id":"_design/1", "_version":1, "test":"test"}`))
	if err := writer.PutDocument("seqID4", doc, nil); err != nil {
		log.Fatal(err)
	}

	writer.Commit()

	writer.Close()

	var reader DatabaseReader = new(DefaultDatabaseReader)

	reader.Open(connectionString)

	reader.Begin()

	if _, err := reader.GetDocumentRevisionByID("1"); err != nil {
		fmt.Println(err)
	}

	if _, err := reader.GetDocumentRevisionByIDandVersion("1", 1); err != nil {
		fmt.Println(err)
	}

	if _, err := reader.GetDocumentByIDandVersion("1", 1); err != nil {
		fmt.Println(err)
	}

	if _, err := reader.GetDocumentByID("1"); err != nil {
		fmt.Println(err)
	}

	if string(reader.GetChanges()) != `{"results":[{"seq":"seqID4","version":1,"id":"_design/1"},{"seq":"seqID3","version":2,"id":"2","deleted":1},{"seq":"seqID2","version":1,"id":"2"},{"seq":"seqID1","version":1,"id":"1"}]}` {
		log.Fatal("changes api mismatch")
	}

	if reader.GetDocumentCount() != 2 {
		log.Fatal("count mismatch")
	}

	if reader.GetLastUpdateSequence() != "seqID4" {
		log.Fatal("update seq mismatch")
	}

	reader.Commit()

	reader.Close()

	os.Remove(connectionString)
}
