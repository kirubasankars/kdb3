package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/valyala/fastjson"
)

// https://blog.questionable.services/article/testing-http-handlers-go/
func TestGetUUID(t *testing.T) {
	kdb, _ := NewKDB()
	var parser fastjson.Parser
	req, _ := http.NewRequest("GET", "/_uuids?count=10", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter(kdb)
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	v, _ := parser.Parse(rr.Body.String())
	uuids := v.GetArray()

	if len(uuids) != 10 {
		t.Errorf("expected 10 items, got %d", len(uuids))
	}

	testExpectJSONContentType(t, rr)

	req, _ = http.NewRequest("GET", "/_uuids?count=-1", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	v, _ = parser.Parse(rr.Body.String())
	uuids = v.GetArray()

	if len(uuids) != 1 {
		t.Errorf("expected 1 items, got %d", len(uuids))
	}

	testExpectJSONContentType(t, rr)
}

func testExpectJSONContentType(t *testing.T, rr *httptest.ResponseRecorder) {
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf(`expected json content type`)
	}
}

func testExpect200(t *testing.T, rr *httptest.ResponseRecorder) {
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
}

func testExpect201(t *testing.T, rr *httptest.ResponseRecorder) {
	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusCreated)
	}
}

func testExpect404(t *testing.T, rr *httptest.ResponseRecorder) {
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
}

func testExpect409(t *testing.T, rr *httptest.ResponseRecorder) {
	if status := rr.Code; status != http.StatusConflict {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusConflict)
	}
}

func TestGetInfo(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/", nil)
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)
}

func TestHandlerPutDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	testExpect201(t, rr)
	testExpectJSONContentType(t, rr)

	expected := `{"ok":true}`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have ok %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerPutDocument(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/testdb", nil)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString("{}")
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())
	if doc.ID == "" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}
	if doc.Version != 1 {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	testExpectJSONContentType(t, rr)

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerPutDocument1(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	body := bytes.NewBufferString(`{"_id":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())
	if doc.Version != 1 || doc.ID != "1" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	body = bytes.NewBufferString(formatDocumentString(doc.ID, doc.Version, false))
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpectJSONContentType(t, rr)
	testExpect200(t, rr)

	doc, _ = ParseDocument(rr.Body.Bytes())
	if doc.Version != 2 || doc.ID != "1" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	body = bytes.NewBufferString(formatDocumentString(doc.ID, doc.Version-1, false))
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect409(t, rr)

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerDeleteDocument(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	body := bytes.NewBufferString(`{"_id":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("/testdb/1?rev=%d", doc.Version), nil)
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ = ParseDocument(rr.Body.Bytes())
	if doc.ID != "1" || doc.Version != 2 || doc.Deleted != true {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerPutDeleteDocument(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	body := bytes.NewBufferString(`{"_id":1, "_rev":2}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect409(t, rr)
	testExpectJSONContentType(t, rr)

	body = bytes.NewBufferString(`{"_id":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())

	if doc.ID != "1" || doc.Version != 1 || doc.Deleted != false {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerBulkDocuments(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	expected := `[{"_id":"3","_rev":1},{"_id":"4","_rev":1}]`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have %s, got %s`, expected, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerBulkGetDocuments(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_gets", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	expected := `[{"_id":"3","_rev":1},{"_id":"4","_rev":1}]`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have %s, got %s`, expected, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

type testChanges struct {
	Results []testChange `json:"results"`
}

type testChange struct {
	ID  string `json:"id"`
	Rev int    `json:"rev"`
	Seq int    `json:"seq"`
}

func TestHandlerGetChanges(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("GET", "/testdb/_changes", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	a := testChanges{}
	json.Unmarshal(rr.Body.Bytes(), &a)

	a0 := a.Results[0]
	if a0.ID != "_design/_views" || a0.Rev != 1 {
		t.Errorf(`failed`)
	}

	a1 := a.Results[1]
	if a1.ID != "3" || a1.Rev != 1 {
		t.Errorf(`failed`)
	}

	a4 := a.Results[2]
	if a4.ID != "4" || a4.Rev != 1 {
		t.Errorf(`failed`)
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerGetDocument(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("GET", "/testdb/1", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect404(t, rr)
	testExpectJSONContentType(t, rr)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"_id":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	doc, _ := ParseDocument(rr.Body.Bytes())
	if doc.Version != 1 || doc.ID != "1" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("GET", "/testdb/1?version=1", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)
	doc, _ = ParseDocument(rr.Body.Bytes())
	if doc.Version != 1 || doc.ID != "1" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerGetDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)
	stat := &DatabaseStat{}
	json.Unmarshal(rr.Body.Bytes(), stat)

	if stat.DBName != "testdb" || stat.DocCount != 1 {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)
	stat = &DatabaseStat{}
	json.Unmarshal(rr.Body.Bytes(), stat)

	if stat.DBName != "testdb" || stat.DocCount != 3 {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerGetDDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/testdb", nil)
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("GET", "/testdb/_design/_views", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())

	if doc.ID != "_design/_views" {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

type testAllDocsRows struct {
	Rows []testEmpty
}

type testEmpty struct {
	ID string `json:"id"`
}

func TestHandlerPutDDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("GET", "/testdb/_design/_views", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	req, _ = http.NewRequest("PUT", "/testdb/_design/_views", rr.Body)
	rr = httptest.NewRecorder()
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())
	if doc.ID != "_design/_views" || doc.Version != 2 {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("GET", "/testdb/_design/_views", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	viewDoc := rr.Body.Bytes()
	doc, _ = ParseDocument(viewDoc)
	req, _ = http.NewRequest("POST", "/testdb/_design/_views1", bytes.NewBuffer(doc.Data))
	req.Header.Add("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ = ParseDocument(rr.Body.Bytes())
	if doc.ID != "_design/_views1" || doc.Version != 1 {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("GET", "/testdb/_all_docs", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("GET", "/testdb/_design/_views1/_all_docs", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	rows := testAllDocsRows{}
	json.Unmarshal(rr.Body.Bytes(), &rows)

	if len(rows.Rows) != 4 {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("GET", "/testdb/_all_docs?key=3", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	json.Unmarshal(rr.Body.Bytes(), &rows)

	if rows.Rows[0].ID != "3" {
		t.Errorf(`failed, got %s`, rr.Body.String())
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestDeleteDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb)

	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("PUT", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	expected := `{"ok":true}`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have ok`)
	}

	testExpectJSONContentType(t, rr)
}
