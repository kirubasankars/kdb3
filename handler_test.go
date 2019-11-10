package main

import (
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
