package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

func TestNewKDBEngine(t *testing.T) {
	kdb, err := NewKDB()
	if kdb.dbs == nil {
		fmt.Println(err)
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
	doc, err := kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	if doc.Version != 1 {
		t.Error("verison missing")
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

	ver := strconv.Itoa(outputDoc.Version)

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

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1", "_version":"` + ver + `"}`))
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

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2","test":1}`))
	doc, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1", "_version":1}`))
	doc, err = kdb.DeleteDocument("testdb", inputDoc)
	if err != nil {
		t.Error("unable to delete doc", err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1"}`))
	doc, err = kdb.GetDocument("testdb", inputDoc, true)
	if err == nil || doc.Deleted == false {
		t.Error("revision missing for deleted doc")
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"1","test":2}`))
	doc, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2","test":2}`))
	doc, err = kdb.PutDocument("testdb", inputDoc)
	if err.Error() != "doc_conflict" {
		t.Error("doc missing")
	}

	stat, _ := kdb.DBStat("testdb")
	if stat.DocCount != 2 {
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

type AllDocsViewResult struct {
	Offset int `json:"offset"`
	Rows   []struct {
		Key   string `json:"key"`
		Value struct {
			Rev string
		} `json:"value"`
		DocID string `json:"doc_id"`
	} `json:"rows"`
	TotalRows int `json:"total_rows"`
}

func TestBuildView(t *testing.T) {
	kdb, _ := NewKDB()
	err := kdb.Open("testdb", true)
	if err != nil {
		t.Error(err)
	}

	if _, ok := kdb.dbs["testdb"].viewManager.GetView("_design/_views$_all_docs"); ok {
		t.Error("view failed")
	}

	rs, _ := kdb.SelectView("testdb", "_design/_views", "_all_docs", "default", nil, false)
	r := AllDocsViewResult{}
	json.Unmarshal(rs, &r)

	if _, ok := kdb.dbs["testdb"].viewManager.GetView("_design/_views$_all_docs"); !ok {
		t.Error("view failed")
	}

	if len(r.Rows) != 1 {
		t.Error("row count failed", r.Rows)
	}

	inputDoc, _ := ParseDocument([]byte(`{"_id":"1","test":1}`))
	_, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	rs, _ = kdb.SelectView("testdb", "_design/_views", "_all_docs", "default", nil, false)
	r = AllDocsViewResult{}
	json.Unmarshal(rs, &r)

	if len(r.Rows) != 2 {
		t.Error("row count failed")
	}

	inputDoc, _ = ParseDocument([]byte(`{"_id":"2","test":1}`))
	_, err = kdb.PutDocument("testdb", inputDoc)
	if err != nil {
		t.Error(err)
	}

	rs, _ = kdb.SelectView("testdb", "_design/_views", "_all_docs", "default", nil, false)
	r = AllDocsViewResult{}
	json.Unmarshal(rs, &r)

	if len(r.Rows) != 3 {
		t.Error("row count failed")
	}

	kdb.Delete("testdb")
}

func BenchmarkPutDocument(b *testing.B) {
	kdb, _ := NewKDB()
	kdb.Open("testdb", true)
	inputDoc, _ := ParseDocument([]byte(`{"test":1}`))

	for x := 0; x < b.N; x++ {
		kdb.PutDocument("testdb", inputDoc)
	}

	kdb.Delete("testdb")
}

func BenchmarkParseDocument(b *testing.B) {
	for x := 0; x < b.N; x++ {
		ParseDocument([]byte(`{"test":1}`))
	}
}
