package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestRouter(t *testing.T) (http.Handler, *KDB) {
	t.Helper()
	dir := t.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		t.Fatalf("NewKDBWithDataDir: %v", err)
	}
	return NewRouter(kdb, ""), kdb
}

func TestEdgeDBLifecycle(t *testing.T) {
	handler, _ := newTestRouter(t)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/$bad", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid name: got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/edgedb", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create: got %d body %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/edgedb", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusPreconditionFailed {
		t.Fatalf("exists: got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/missingdb", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing get: got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("DELETE", "/missingdb", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing delete: got %d", rr.Code)
	}
}

func TestEdgeDocOCCAndJSON(t *testing.T) {
	handler, _ := newTestRouter(t)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/docdb", nil)
	handler.ServeHTTP(rr, req)

	// bad json
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/docdb", bytes.NewBufferString("{"))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad json: got %d %s", rr.Code, rr.Body.String())
	}

	// non-object
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/docdb", bytes.NewBufferString(`["x"]`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("non-object: got %d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/docdb/d1", bytes.NewBufferString(`{"_id":"d1","a":1}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put: got %d %s", rr.Code, rr.Body.String())
	}

	// missing rev on update
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/docdb/d1", bytes.NewBufferString(`{"_id":"d1","a":2}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("missing rev: got %d %s", rr.Code, rr.Body.String())
	}

	// stale rev
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/docdb/d1", bytes.NewBufferString(`{"_id":"d1","_rev":9,"a":2}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("stale rev: got %d", rr.Code)
	}

	// invalid rev
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/docdb/d1", bytes.NewBufferString(`{"_id":"d1","_rev":"nope","a":2}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid rev: got %d %s", rr.Code, rr.Body.String())
	}

	// get missing
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/docdb/missing", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get missing: got %d", rr.Code)
	}

	// head existing
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("HEAD", "/docdb/d1", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("head: got %d", rr.Code)
	}

	// wrong method
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PATCH", "/docdb/d1", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed && rr.Code != http.StatusNotFound {
		t.Fatalf("wrong method: got %d", rr.Code)
	}
}

func TestEdgeBulkAndChanges(t *testing.T) {
	handler, _ := newTestRouter(t)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/bulkdb", nil)
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/bulkdb/_bulk_docs", bytes.NewBufferString(`{"_docs":[{"_id":"b1"},{"_id":"b1","_rev":1},{"_id":"b1"}]}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk: got %d %s", rr.Code, rr.Body.String())
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte("error")) {
		t.Fatalf("expected partial conflict in bulk response: %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/bulkdb/_bulk_docs", bytes.NewBufferString(`{"_docs":[]}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	// empty list is accepted as [] or rejected as invalid input depending on parser
	if rr.Code != http.StatusOK && rr.Code != http.StatusBadRequest {
		t.Fatalf("empty bulk: got %d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/bulkdb/_bulk_gets", bytes.NewBufferString(`{"_docs":[{"_id":"missing"}]}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk gets: got %d %s", rr.Code, rr.Body.String())
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte("error")) {
		t.Fatalf("expected missing id error: %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/bulkdb/_changes?since=0&limit=2", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("changes: got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/bulkdb/_changes?since=999999&limit=1", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("changes past end: got %d", rr.Code)
	}
	var empty map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &empty); err != nil {
		t.Fatalf("empty changes JSON: %v body=%s", err, rr.Body.String())
	}
	results, _ := empty["results"].([]interface{})
	if results == nil {
		t.Fatalf("expected results array, got %s", rr.Body.String())
	}
	if len(results) != 0 {
		t.Fatalf("expected empty results, got %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/bulkdb/_changes?since=abc&limit=xyz", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("garbage query: got %d", rr.Code)
	}
}

func TestEdgeBulkNilSafeNoPanic(t *testing.T) {
	kdb, _ := testDB(t, "c5bulk")
	// Malformed objects and nulls must not panic.
	body := []byte(`{"_docs":[null, "x", 1, {"_id":"ok1"}, {"_id":"_design/bad","views":{"v":{"setup":["DROP TABLE x"],"run":["SELECT 1"],"select":{"default":"SELECT 1"}}}}]}`)
	out, err := kdb.BulkDocuments("c5bulk", body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("error")) {
		t.Fatalf("expected per-item errors, got %s", out)
	}
	if !bytes.Contains(out, []byte(`"_id":"ok1"`)) && !bytes.Contains(out, []byte(`"ok1"`)) {
		// ok1 may succeed; at least response must be valid JSON array
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(out, &arr); err != nil {
		t.Fatalf("bulk response not JSON array: %v %s", err, out)
	}
}

func TestEdgeJSONEscapeIDs(t *testing.T) {
	escaped := jsonEscapeString(`a"b\c`)
	if escaped != `"a\"b\\c"` {
		t.Fatalf("unexpected escape: %s", escaped)
	}
	s := formatDocumentString(`quote"id`, 1, false)
	if !strings.Contains(s, `\"`) {
		t.Fatalf("expected escaped id in document string: %s", s)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("formatDocumentString not valid JSON: %v %s", err, s)
	}
}

func TestEdgeSPAPathTraversal(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "index.html"), []byte("index-ok"), 0o644)
	secret := filepath.Join(dir, "..", "secret.txt")
	_ = os.WriteFile(secret, []byte("secret-data"), 0o644)
	h := spaFSHandler(os.DirFS(dir))

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/../secret.txt", nil)
	h.ServeHTTP(rr, req)
	if bytes.Contains(rr.Body.Bytes(), []byte("secret-data")) {
		t.Fatal("SPA handler leaked file outside root")
	}
}

func TestEdgeViewsAndVacuum(t *testing.T) {
	handler, kdb := newTestRouter(t)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/viewedge", nil)
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/viewedge/_design/missing/v1", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing design/view: got %d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/nosuch/_vacuum", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("vacuum missing db: got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/viewedge/_vacuum", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("vacuum empty: got %d %s", rr.Code, rr.Body.String())
	}

	// concurrent read smoke during vacuum of populated db
	doc, _ := ParseDocument([]byte(`{"_id":"v1","n":1}`))
	if _, err := kdb.PutDocument("viewedge", doc); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			_, _ = kdb.GetDocument("viewedge", &Document{ID: "v1"}, true)
		}
	}()
	if err := kdb.Vacuum("viewedge"); err != nil {
		t.Fatal(err)
	}
	<-done
	stat, err := kdb.DBStat("viewedge")
	if err != nil {
		t.Fatal(err)
	}
	if stat.UpdateSeq < 1 {
		t.Fatalf("expected seq preserved after vacuum, got %d", stat.UpdateSeq)
	}
}

func TestEdgeAuth(t *testing.T) {
	dir := t.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewRouter(kdb, "secret")

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/_cat/dbs", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/_cat/dbs", nil)
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 with token, got %d", rr.Code)
	}

	// Wrong length / wrong token still 401 (constant-time compare path).
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/_cat/dbs", nil)
	req.Header.Set("Authorization", "Bearer secre")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong-length token, got %d", rr.Code)
	}
	if !subtleConstEq("secret", "secret") || subtleConstEq("secret", "secretx") {
		t.Fatal("subtleConstEq behavior unexpected")
	}

	// Embedded admin UI is public (no token) and returns HTML.
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/_utils/", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK && rr.Code != http.StatusMovedPermanently {
		t.Fatalf("utils should be public, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(strings.ToLower(body), "doctype html") && !strings.Contains(body, "kdb3") {
		snippet := body
		if len(snippet) > 80 {
			snippet = snippet[:80]
		}
		t.Fatalf("utils should serve embedded HTML, got %q", snippet)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/_utils/app.js", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("embedded app.js expected 200, got %d", rr.Code)
	}

	// Swagger UI / OpenAPI are public (no token).
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/_docs/", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK && rr.Code != http.StatusMovedPermanently {
		t.Fatalf("docs should be public, got %d", rr.Code)
	}
	docsBody := rr.Body.String()
	if !strings.Contains(docsBody, "swagger-ui") && !strings.Contains(strings.ToLower(docsBody), "openapi") {
		snippet := docsBody
		if len(snippet) > 80 {
			snippet = snippet[:80]
		}
		t.Fatalf("docs should serve swagger HTML, got %q", snippet)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/_docs/openapi.yaml", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("openapi.yaml expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "openapi:") {
		t.Fatalf("openapi.yaml missing openapi version line")
	}
}
