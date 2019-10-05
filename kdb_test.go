package main

import (
	"testing"
)

func TestNewKDBEngine(t *testing.T) {
	kdb, _ := NewKDB()
	if kdb.dbs == nil {
		t.Failed()
	}

	if kdb.viewPath == "" || kdb.dbPath == "" {
		t.Failed()
	}
}

func TestCreateDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}
	err = kdb.Open("testdb", true)
	if err.Error() != "db_exists" {
		t.Error("db not found")
	}

	if _, ok := kdb.dbs["testdb"]; !ok {
		t.Error("db instance not found")
	}

	err = kdb.Open("t$estdb", true)
	if err.Error() != "invalid_db_name" {
		t.Error("db name should not accept $")
	}

	err = kdb.Delete("testdb")
	if err != nil {
		t.Error(err)
	}

	if _, ok := kdb.dbs["testdb"]; ok {
		t.Error("db instance not deleted")
	}
}

func TestListDatabases(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb1", true)
	if err != nil {
		t.Error(err)
	}
	err = kdb.Open("testdb2", true)
	if err != nil {
		t.Error(err)
	}

	dbs, err := kdb.ListDataBases()
	if err != nil {
		t.Error(err)
	}
	var dbCount = 0
	for _, x := range dbs {
		if x == "testdb1" {
			dbCount++
		}
		if x == "testdb2" {
			dbCount++
		}
	}

	if dbCount != 2 {
		t.Error("list databases failed")
	}

	err = kdb.Delete("testdb1")
	if err != nil {
		t.Error(err)
	}
	err = kdb.Delete("testdb2")
	if err != nil {
		t.Error(err)
	}
}

func TestPutDocument(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ := ParseDocument([]byte(`{"_id":"1","test":1}`))
	_, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	kdb.Delete("testdb")
}

func TestGetDocument(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ := ParseDocument([]byte(`{"_id":"1","test":1}`))
	_, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2","test":1}`))
	_, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1"}`))
	outputDoc, err := kdb.GetDocument("testdb", inputDoc, true)
	if err != nil {
		t.Error(err)
	}

	if outputDoc == nil {
		t.Error("doc not found")
	}

	if outputDoc.ID != "1" {
		t.Error("doc not found")
	}

	rev1 := formatRev(outputDoc.RevNumber, outputDoc.RevID)

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2"}`))
	outputDoc, err = kdb.GetDocument("testdb", inputDoc, true)
	if err != nil {
		t.Error(err)
	}

	if outputDoc == nil {
		t.Error("doc not found")
	}

	if outputDoc.ID != "2" {
		t.Error("wrong doc")
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1", "_rev":"` + rev1 + `"}`))
	outputDoc, err = kdb.GetDocument("testdb", inputDoc, true)
	if err != nil {
		t.Error(err)
	}

	if outputDoc == nil {
		t.Error("doc not found")
	}

	if outputDoc.ID != "1" {
		t.Error("wrong doc")
	}

	stat, _ := kdb.DBStat("testdb")
	if stat.DocCount != 3 {
		t.Error("doc count failed")
	}

	kdb.Delete("testdb")
}

func TestDeleteDocument(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ := ParseDocument([]byte(`{"_id":"1","test":1}`))
	doc, err := kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	rev := formatRev(doc.RevNumber, doc.RevID)

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2","test":1}`))
	doc, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1", "_rev":"` + rev + `"}`))
	doc, err = kdb.DeleteDocument("testdb", inputDoc)
	if err != nil {
		t.Error("unable to delete doc", err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1"}`))
	doc, err = kdb.GetDocument("testdb", inputDoc, true)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1","test":2}`))
	doc, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2","test":2}`))
	doc, err = kdb.PutDocument("testdb", inputDoc)
	if err.Error() != "mismatched_rev" {
		t.Error("doc missing")
	}

	stat, _ := kdb.DBStat("testdb")
	if stat.DocCount != 3 {
		t.Error("doc count failed")
	}

	kdb.Delete("testdb")
}

func TestDatabaseVaccum(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	err = kdb.Vacuum("testdb")
	if err != nil {
		t.Error(err)
	}
	err = kdb.Delete("testdb")
	if err != nil {
		t.Error(err)
	}
}

func TestDatabaseStat(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	stat, err := kdb.DBStat("testdb")
	if err != nil {
		t.Error(err)
	}

	if stat.DocCount != 1 || stat.DBName != "testdb" || stat.UpdateSeq == "" {
		t.Error("db stat err")
	}

	err = kdb.Delete("testdb")
	if err != nil {
		t.Error(err)
	}
}

func TestGetDesignDocumentAllViews(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	stat, err := kdb.DBStat("testdb")
	if err != nil {
		t.Error(err)
	}

	if stat.DocCount != 1 {
		t.Error("db creation err")
	}

	doc, _ := ParseDocument([]byte(`{"_id":"_design/_views"}`))
	ddoc, _ := kdb.GetDocument("testdb", doc, true)

	if ddoc.ID != "_design/_views" {
		t.Error("build in view missing")
	}

	err = kdb.Delete("testdb")
	if err != nil {
		t.Error(err)
	}
}
