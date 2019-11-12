package main

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupTestDatabaseWithWriter() error {
	os.Remove(testConnectionString)

	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter(testConnectionString)
	writer.Open()

	writer.Begin()

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

	writer.Commit()

	writer.Close()

	return nil
}

func deleteTestDatabaseWithWriter() {
	os.Remove(testConnectionString)
}

func TestReaderGetDocumentByID(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentByID("1"); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetDocumentRevisionByID(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentRevisionByID("1"); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetDocumentByIDandVersion(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetDocumentRevisionByIDandVersion(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()

	if _, err := reader.GetDocumentRevisionByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetDocumentCount(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()

	count := reader.GetDocumentCount()
	if count != 2 {
		t.Errorf("expected %d rows, got %d", 2, count)
	}

	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetLastUpdateSequence(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()

	seqID := reader.GetLastUpdateSequence()
	if seqID != "seqID4" {
		t.Errorf("expected last seqID as %s, got %s", "seqID4", seqID)
	}

	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetChanges(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
	reader.Open()

	reader.Begin()
	expected := `{"results":[{"seq":"seqID4","version":1,"id":"_design/_views"},{"seq":"seqID3","version":2,"id":"2","deleted":1},{"seq":"seqID2","version":1,"id":"2"},{"seq":"seqID1","version":1,"id":"1"}]}`
	changes, _ := reader.GetChanges("", 999)
	if string(changes) != expected {
		t.Errorf("expected changes as  \n %s \n, got \n %s \n", expected, string(changes))
	}
	reader.Commit()
	reader.Close()

	deleteTestDatabaseWithWriter()
}

func TestReaderGetAllDesignDocuments(t *testing.T) {

	if err := setupTestDatabaseWithWriter(); err != nil {
		t.Errorf("unable to setup a database. %s", err.Error())
	}

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader(testConnectionString)
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

	deleteTestDatabaseWithWriter()
}

func TestReaderPool(t *testing.T) {
	serviceLocator := new(DefaultServiceLocator)
	readers := serviceLocator.GetDatabaseReaderPool(testConnectionString, 1)
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
