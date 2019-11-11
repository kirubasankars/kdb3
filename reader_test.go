package main

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupTestReaderDatabase() error {
	os.Remove(testConnectionString)

	var writer DatabaseWriter = new(DefaultDatabaseWriter)
	writer.Open(testConnectionString)

	if err := writer.ExecBuildScript(); err != nil {
		return err
	}

	doc, _ := ParseDocument([]byte(`{"_id":1, "_version":1}`))
	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
		return err
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":1}`))
	if err := writer.PutDocument("seqID2", doc, nil); err != nil {
		return err
	}

	doc, _ = ParseDocument([]byte(`{"_id":2, "_version":2, "_deleted":true}`))
	if err := writer.PutDocument("seqID3", doc, nil); err != nil {
		return err
	}

	doc, _ = ParseDocument([]byte(`{"_id":"_design/_views", "_version":1, "test":"test"}`))
	if err := writer.PutDocument("seqID4", doc, nil); err != nil {
		return err
	}

	writer.Close()

	return nil
}

func deleteTestReaderDatabase() {
	os.Remove(testConnectionString)
}

func TestReaderGetDocumentByID(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	if _, err := reader.GetDocumentByID("1"); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Close()
	deleteTestReaderDatabase()
}

func TestReaderGetDocumentRevisionByID(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	if _, err := reader.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Close()

	deleteTestReaderDatabase()
}

func TestReaderGetDocumentByIDandVersion(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	if _, err := reader.GetDocumentByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Close()

	deleteTestReaderDatabase()
}

func TestReaderGetDocumentRevisionByIDandVersion(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	if _, err := reader.GetDocumentRevisionByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Close()

	deleteTestReaderDatabase()
}

func TestReaderGetDocumentCount(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	count, _ := reader.GetDocumentCount()
	if count != 2 {
		t.Errorf("expected %d rows, got %d", 2, count)
	}

	reader.Close()
	deleteTestReaderDatabase()
}

func TestReaderGetLastUpdateSequence(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	seqID, _ := reader.GetLastUpdateSequence()
	if seqID != "seqID4" {
		t.Errorf("expected last update seq as %s, got %s", "seqID4", seqID)
	}

	reader.Close()
	deleteTestReaderDatabase()
}

func TestReaderGetChanges(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

	expected := `{"results":[{"seq":"seqID4","version":1,"id":"_design/_views"},{"seq":"seqID3","version":2,"id":"2","deleted":1},{"seq":"seqID2","version":1,"id":"2"},{"seq":"seqID1","version":1,"id":"1"}]}`
	changes, _ := reader.GetChanges("", 999)
	if string(changes) != expected {
		t.Errorf("expected changes as  \n %s \n, got \n %s \n", expected, string(changes))
	}

	expected = `{"results":[{"seq":"seqID2","version":1,"id":"2"},{"seq":"seqID1","version":1,"id":"1"}]}`
	changes, _ = reader.GetChanges("", 2)
	if string(changes) != expected {
		t.Errorf("changes, limit : expected changes as  \n %s \n, got \n %s \n", expected, string(changes))
	}

	expected = `{"results":[{"seq":"seqID4","version":1,"id":"_design/_views"}]}`
	changes, _ = reader.GetChanges("seqID3", 2)
	if string(changes) != expected {
		t.Errorf("changes, since : expected changes as  \n %s \n, got \n %s \n", expected, string(changes))
	}

	reader.Close()

	deleteTestReaderDatabase()
}

func TestReaderGetAllDesignDocuments(t *testing.T) {
	if err := setupTestReaderDatabase(); err != nil {
		t.Errorf("unable to setup test database. %s", err.Error())
	}

	var reader DatabaseReader = new(DefaultDatabaseReader)
	reader.Open(testConnectionString)

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

	reader.Close()
	deleteTestReaderDatabase()
}

func TestReaderPool(t *testing.T) {
	readers := NewDatabaseReaderPool(testConnectionString, 1)
	r1 := readers.Borrow()

	var wg sync.WaitGroup
	wg.Add(1)

	var counter uint64
	go func() {
		r2 := readers.Borrow()
		if counter <= 0 {
			t.Errorf("expected reader borrow has to wait. failed.")
		}
		readers.Return(r2)
		wg.Done()
	}()

	time.Sleep(5 * 1000)
	atomic.AddUint64(&counter, 1)
	readers.Return(r1)
	wg.Wait()
	_ = r1
}
