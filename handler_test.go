package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/valyala/fastjson"
)

//https://blog.questionable.services/article/testing-http-handlers-go/
func TestGetUUID(t *testing.T) {
	var parser fastjson.Parser
	req, _ := http.NewRequest("GET", "/_uuids?count=10", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter()
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
	if rr.HeaderMap.Get("Content-Type") != "application/json" {
		t.Errorf(`expected json content type`)
	}
}

func testExpect200(t *testing.T, rr *httptest.ResponseRecorder) {
	if status := rr.Code; status != http.StatusOK {
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
	var parser fastjson.Parser
	req, _ := http.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	v, _ := parser.Parse(rr.Body.String())

	version := v.GetObject("version").Get("sqlite_version").String()
	if version != `"3.29.0"` {
		t.Errorf(`expected version "3.29.0", got %s`, version)
	}

	testExpectJSONContentType(t, rr)
}

func TestPutDatabase(t *testing.T) {
	kdb, _ = NewKDB()
	req, _ := http.NewRequest("PUT", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	expected := `{"ok":true}`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have ok %s`, rr.Body.String())
	}

	testExpectJSONContentType(t, rr)
}

func TestHandlerPutDocument(t *testing.T) {
	body := bytes.NewBufferString("{}")
	req, _ := http.NewRequest("POST", "/testdb", body)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	doc, _ := ParseDocument(rr.Body.Bytes())
	if doc.Version != 1 || doc.ID == "" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	testExpectJSONContentType(t, rr)
}

func TestHandlerPutDocument1(t *testing.T) {
	handler := NewRouter()

	body := bytes.NewBufferString(`{"_id":1}`)
	req, _ := http.NewRequest("POST", "/testdb", body)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())
	if doc.Version != 1 || doc.ID != "1" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	body = bytes.NewBufferString(`{"_id":1, "_version":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpectJSONContentType(t, rr)
	testExpect200(t, rr)

	doc, _ = ParseDocument(rr.Body.Bytes())
	if doc.Version != 2 || doc.ID != "1" {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}

	body = bytes.NewBufferString(`{"_id":1, "_version":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect409(t, rr)
}

func TestHandlerDeleteDocument(t *testing.T) {
	handler := NewRouter()

	req, _ := http.NewRequest("DELETE", "/testdb/1?version=2", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())

	if doc.ID != "1" || doc.Version != 3 || doc.Deleted != true {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}
}

func TestHandlerPutDeletedDocument(t *testing.T) {
	handler := NewRouter()

	body := bytes.NewBufferString(`{"_id":1, "_version":2}`)
	req, _ := http.NewRequest("POST", "/testdb", body)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect409(t, rr)
	testExpectJSONContentType(t, rr)

	body = bytes.NewBufferString(`{"_id":1}`)
	req, _ = http.NewRequest("POST", "/testdb", body)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	doc, _ := ParseDocument(rr.Body.Bytes())

	if doc.ID != "1" || doc.Version != 4 || doc.Deleted != false {
		t.Errorf(`expected to have ok, got %s`, rr.Body.String())
	}
}

func TestHandlerBulkDocuments(t *testing.T) {
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ := http.NewRequest("POST", "/testdb/_bulk_docs", body)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)
	expected := `[{"_id":"3","_version":1},{"_id":"4","_version":1}]`

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	if expected != rr.Body.String() {
		t.Errorf(`expected to have %s, got %s`, expected, rr.Body.String())
	}
}

func TestHandlerBulkGetDocuments(t *testing.T) {
	body := bytes.NewBufferString(`{"_docs":[{"_id":3},{"_id":4}]}`)
	req, _ := http.NewRequest("POST", "/testdb/_bulk_gets", body)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)
	expected := `[{"_id":"3","_version":1},{"_id":"4","_version":1}]`

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	if expected != rr.Body.String() {
		t.Errorf(`expected to have %s, got %s`, expected, rr.Body.String())
	}
}

type testChanges struct {
	Results []testChange `json:"results"`
}

type testChange struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	Seq     string `json:"seq"`
}

func TestHandlerGetChanges(t *testing.T) {
	req, _ := http.NewRequest("GET", "/testdb/_changes", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	a := testChanges{}
	json.Unmarshal(rr.Body.Bytes(), &a)

	a0 := a.Results[0]
	if a0.ID != "4" || a0.Version != 1 {
		t.Errorf(`failed`)
	}

	a1 := a.Results[1]
	if a1.ID != "3" || a1.Version != 1 {
		t.Errorf(`failed`)
	}

	a2 := a.Results[2]
	if a2.ID != "1" || a2.Version != 4 {
		t.Errorf(`failed`)
	}

	a4 := a.Results[4]
	if a4.ID != "_design/_views" || a4.Version != 1 {
		t.Errorf(`failed`)
	}
}
func TestDeleteDatabase(t *testing.T) {
	req, _ := http.NewRequest("DELETE", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter()
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)

	expected := `{"ok":true}`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have ok`)
	}

	testExpectJSONContentType(t, rr)
}
