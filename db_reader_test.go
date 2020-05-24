package main

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testConnectionString string = "file:testdb.db?mode=memory&cache=shared"

func openTestDatabase() func() {
	con, _ := sql.Open("sqlite3", testConnectionString)
	tx, _ := con.Begin()
	tx.Exec("CREATE TABLE a(b)")
	tx.Commit()
	return func() {
		con.Close()
	}
}

func setupTestDatabase1() {
	serviceLocator := new(DefaultServiceLocator)
	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
	err := writer.Open(testConnectionString)
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
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

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
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

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
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

	reader.Begin()

	if _, err := reader.GetDocumentByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentRevisionByIDandVersion(t *testing.T) {
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

	reader.Begin()

	if _, err := reader.GetDocumentRevisionByIDandVersion("1", 1); err != nil {
		t.Errorf("unexpected error %s", err.Error())
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetDocumentCount(t *testing.T) {
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

	reader.Begin()

	docCount, deletedDocCount := reader.GetDocumentCount()
	if docCount != 2 && deletedDocCount != 1 {
		t.Errorf("expected %d rows, got %d", 2, docCount)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetLastUpdateSequence(t *testing.T) {
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

	reader.Begin()

	seqID := reader.GetLastUpdateSequence()
	if seqID != "seqID4" {
		t.Errorf("expected last seqID as %s, got %s", "seqID4", seqID)
	}

	reader.Commit()
	reader.Close()
}

func TestReaderGetChanges(t *testing.T) {
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

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
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()
	setupTestDatabase1()

	serviceLocator := new(DefaultServiceLocator)
	var reader DatabaseReader = serviceLocator.GetDatabaseReader()
	reader.Open(testConnectionString)

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

func TestReaderPool(t *testing.T) {
	dbCloseHandle := openTestDatabase()
	defer dbCloseHandle()

	serviceLocator := new(DefaultServiceLocator)
	readers := NewDatabaseReaderPool(1, serviceLocator)
	readers.Open(testConnectionString)
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
