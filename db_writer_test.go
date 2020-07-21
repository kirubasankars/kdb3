package main

// import (
// 	"fmt"
// 	"testing"
// )

// func setupTestDatabase2() {
// 	serviceLocator := new(DefaultServiceLocator)
// 	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
// 	err := writer.Open(testConnectionString)
// 	if err != nil {
// 		fmt.Println("unable to setup test database")
// 		return
// 	}

// 	err = writer.Begin()
// 	if err != nil {
// 		fmt.Println("unable to setup test database")
// 		return
// 	}

// 	if err := writer.ExecBuildScript(); err != nil {
// 		fmt.Println("unable to setup test database")
// 		return
// 	}

// 	writer.Commit()

// 	writer.Close()
// }

// func TestWriterPutDocument(t *testing.T) {

// 	dbCloseHandle := openTestDatabase()
// 	defer dbCloseHandle()
// 	setupTestDatabase2()

// 	serviceLocator := new(DefaultServiceLocator)
// 	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
// 	writer.Open(testConnectionString)

// 	writer.Begin()

// 	doc, _ := ParseDocument([]byte(`{"_id":1}`))
// 	if err := writer.PutDocument("seqID", doc, nil); err != nil {
// 		t.Errorf("unable to put document, error %s", err.Error())
// 	}

// 	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
// 		t.Errorf("unable to get document, error %s", err.Error())
// 	}

// 	writer.Commit()

// 	writer.Begin()

// 	if _, err := writer.GetDocumentRevisionByID("1"); err != nil {
// 		t.Errorf("unable to get document, error %s", err.Error())
// 	}

// 	writer.Commit()

// 	writer.Close()
// }

// func TestWriterDeleteDocument(t *testing.T) {
// 	dbCloseHandle := openTestDatabase()
// 	defer dbCloseHandle()
// 	setupTestDatabase2()

// 	serviceLocator := new(DefaultServiceLocator)
// 	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
// 	writer.Open(testConnectionString)

// 	writer.Begin()

// 	doc, _ := ParseDocument([]byte(`{"_id":1}`))
// 	if err := writer.PutDocument("seqID1", doc, nil); err != nil {
// 		t.Errorf("unable to put document, error %s", err.Error())
// 	}

// 	writer.Commit()

// 	writer.Begin()

// 	doc, _ = ParseDocument([]byte(`{"_id":1, "_version":1, "_deleted":true}`))
// 	if err := writer.PutDocument("seqID2", doc, nil); err != nil {
// 		t.Errorf("unable to delete document, error %s", err.Error())
// 	}

// 	writer.Commit()

// 	writer.Begin()

// 	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocumentNotFound {
// 		t.Errorf("expected %s, got doc or err %s", ErrDocumentNotFound, err)
// 	}

// 	writer.Commit()

// 	writer.Close()
// }

// func TestWriterDocNotFound(t *testing.T) {
// 	dbCloseHandle := openTestDatabase()
// 	defer dbCloseHandle()
// 	setupTestDatabase2()

// 	serviceLocator := new(DefaultServiceLocator)
// 	var writer DatabaseWriter = serviceLocator.GetDatabaseWriter()
// 	writer.Open(testConnectionString)

// 	writer.Begin()

// 	if _, err := writer.GetDocumentRevisionByID("1"); err == nil || err != ErrDocumentNotFound {
// 		t.Errorf("expected %s, got doc or err %s", ErrDocumentNotFound, err)
// 		fmt.Println(err)
// 	}

// 	writer.Commit()
// 	writer.Close()
// }
