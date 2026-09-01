package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestValidateAttachmentName(t *testing.T) {
	ok := []string{"photo.jpg", "a", "file_1-2.txt", "my file.png"}
	for _, name := range ok {
		if !ValidateAttachmentName(name) {
			t.Fatalf("expected valid name %q", name)
		}
	}
	bad := []string{"", "..", ".", "a/b", "a\\b", strings.Repeat("x", 201), "file\x00.jpg"}
	for _, name := range bad {
		if ValidateAttachmentName(name) {
			t.Fatalf("expected invalid name %q", name)
		}
	}
}

func TestReadAttachmentBodyTooLarge(t *testing.T) {
	_, err := readAttachmentBody(bytes.NewReader([]byte("abcde")), 4)
	if !errors.Is(err, ErrAttachmentTooLarge) {
		t.Fatalf("expected too large, got %v", err)
	}
	got, err := readAttachmentBody(bytes.NewReader([]byte("abcd")), 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestInjectAttachmentStubs(t *testing.T) {
	raw := []byte(`{"_id":"d","_rev":1,"title":"x"}`)
	out := injectAttachmentStubs(raw, []AttachmentMeta{{
		Name: "photo.jpg", ContentType: "image/jpeg", Length: 3, Digest: "md5-aaa", RevPos: 2,
	}})
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["_attachments"]; !ok {
		t.Fatalf("missing stubs: %s", out)
	}
	if string(injectAttachmentStubs(raw, nil)) != string(raw) {
		t.Fatal("empty metas should leave JSON unchanged")
	}
}

func TestAttachmentsHTTP(t *testing.T) {
	kdb, _ := testDB(t, "atthttp")
	handler := NewRouter(kdb, "")

	// Create empty doc + attachment
	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/atthttp/recipe/photo.jpg", bytes.NewReader([]byte("IMG")))
	req.Header.Set("Content-Type", "image/jpeg")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put att: %d %s", rr.Code, rr.Body.String())
	}
	var meta map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	if meta["_id"] != "recipe" {
		t.Fatalf("id: %v", meta["_id"])
	}

	// GET attachment
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/atthttp/recipe/photo.jpg", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || rr.Body.String() != "IMG" {
		t.Fatalf("get att: %d %q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Content-Type") != "image/jpeg" {
		t.Fatalf("content-type %s", rr.Header().Get("Content-Type"))
	}
	if rr.Header().Get("E-Tag") == "" {
		t.Fatal("missing E-Tag")
	}

	// HEAD
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("HEAD", "/atthttp/recipe/photo.jpg", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || rr.Body.Len() != 0 {
		t.Fatalf("head: %d body=%q", rr.Code, rr.Body.String())
	}

	// Document GET stubs
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/atthttp/recipe", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get doc: %d %s", rr.Code, rr.Body.String())
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"_attachments"`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"photo.jpg"`)) {
		t.Fatalf("expected stubs: %s", rr.Body.String())
	}

	rev := int(meta["_rev"].(float64))

	// PUT without rev on existing → 409
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/atthttp/recipe/photo.jpg", bytes.NewReader([]byte("XX")))
	req.Header.Set("Content-Type", "image/jpeg")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d %s", rr.Code, rr.Body.String())
	}

	// Replace with If-Match
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/atthttp/recipe/photo.jpg", bytes.NewReader([]byte("NEW")))
	req.Header.Set("Content-Type", "image/jpeg")
	req.Header.Set("If-Match", `"`+strconv.Itoa(rev)+`"`)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("replace: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	rev = int(meta["_rev"].(float64))

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/atthttp/recipe/photo.jpg", nil)
	handler.ServeHTTP(rr, req)
	if rr.Body.String() != "NEW" {
		t.Fatalf("replaced body %q", rr.Body.String())
	}

	// Document PUT must not drop attachments
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/atthttp/recipe", bytes.NewBufferString(`{"_id":"recipe","_rev":`+strconv.Itoa(rev)+`,"title":"pie","_attachments":{"x":{"stub":true}}}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("doc put: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	rev = int(meta["_rev"].(float64))

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/atthttp/recipe/photo.jpg", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || rr.Body.String() != "NEW" {
		t.Fatalf("att lost after doc put: %d %q", rr.Code, rr.Body.String())
	}

	// DELETE att
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("DELETE", "/atthttp/recipe/photo.jpg?rev="+strconv.Itoa(rev), nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete att: %d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/atthttp/recipe/photo.jpg", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/atthttp/recipe", nil)
	handler.ServeHTTP(rr, req)
	if bytes.Contains(rr.Body.Bytes(), []byte(`"_attachments"`)) {
		t.Fatalf("stubs should be gone: %s", rr.Body.String())
	}

	// Invalid name
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/atthttp/recipe/bad*name", bytes.NewReader([]byte("x")))
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid name: %d %s", rr.Code, rr.Body.String())
	}
}
