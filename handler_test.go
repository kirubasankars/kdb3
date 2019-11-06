package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

//https://blog.questionable.services/article/testing-http-handlers-go/
func TestGetUUID(t *testing.T) {
	req, err := http.NewRequest("GET", "/_uuids?count=10", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := NewRouter()

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	v, _ := parser.Parse(rr.Body.String())
	uuids := v.GetArray()

	if len(uuids) != 10 {
		t.Errorf("expected 10 items, got %d", len(uuids))
	}

	testExpectJSONContentType(t, rr)
}

func testExpectJSONContentType(t *testing.T, rr *httptest.ResponseRecorder) {
	if rr.HeaderMap.Get("Content-Type") != "application/json" {
		t.Errorf(`expected json content type`)
	}
}
func TestGetInfo(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := NewRouter()

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	v, _ := parser.Parse(rr.Body.String())

	version := v.GetObject("version").Get("sqlite_version").String()
	if version != `"3.29.0"` {
		t.Errorf(`expected version "3.29.0", got %s`, version)
	}

	testExpectJSONContentType(t, rr)
}

func TestPutDatabase(t *testing.T) {
	req, err := http.NewRequest("PUT", "/testdb", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := NewRouter()

	handler.ServeHTTP(rr, req)

	expected := `{"ok":true}`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have ok`)
	}
}

func TestDeleteDatabase(t *testing.T) {
	req, err := http.NewRequest("DELETE", "/testdb", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := NewRouter()

	handler.ServeHTTP(rr, req)

	expected := `{"ok":true}`
	if expected != rr.Body.String() {
		t.Errorf(`expected to have ok`)
	}
}
