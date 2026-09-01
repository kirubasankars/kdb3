package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/valyala/fastjson"
)

// https://blog.questionable.services/article/testing-http-handlers-go/
func TestGetUUID(t *testing.T) {
	kdb, _ := NewKDB()
	var parser fastjson.Parser
	req, _ := http.NewRequest("GET", "/_uuids?count=10", nil)
	rr := httptest.NewRecorder()
	handler := NewRouter(kdb, "")
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
	handler := NewRouter(kdb, "")

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/", nil)
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	var info struct {
		Name    string `json:"name"`
		Version struct {
			KDB3   string `json:"kdb3"`
			Commit string `json:"commit"`
			SQLite string `json:"sqlite"`
		} `json:"version"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Name != "kdb3" || info.Version.KDB3 == "" || info.Version.SQLite == "" {
		t.Fatalf("unexpected server info: %s", rr.Body.String())
	}
}

func TestHandlerPutDatabase(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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

func TestHandlerBulkAllOrNothing(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")

	req, _ := http.NewRequest("PUT", "/testdb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"all_or_nothing":true,"_docs":[{"_id":"a"},{"_id":"b"}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	testExpect200(t, rr)
	testExpectJSONContentType(t, rr)

	expected := `[{"_id":"a","_rev":1},{"_id":"b","_rev":1}]`
	if expected != rr.Body.String() {
		t.Errorf("expected %s, got %s", expected, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"all_or_nothing":true,"_docs":[{"_id":"c"},{"_id":"a","_rev":99}]}`)
	req, _ = http.NewRequest("POST", "/testdb/_bulk_docs", body)
	req.Header.Add("Content-Type", "application/json")
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d %s", rr.Code, rr.Body.String())
	}
	var failed map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &failed); err != nil {
		t.Fatalf("invalid bulk_failed response: %v", err)
	}
	if string(failed["error"]) != `"bulk_failed"` {
		t.Fatalf("expected bulk_failed, got %s", failed["error"])
	}

	_, err := kdb.GetDocument("testdb", &Document{ID: "c"}, true)
	if !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("expected c not created after rollback, got %v", err)
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerBulkGetDocuments(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")

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
	LastSeq int64        `json:"last_seq"`
}

type testChange struct {
	ID        string `json:"id"`
	Rev       int    `json:"rev"`
	UpdateSeq int64  `json:"update_seq"`
}

func TestHandlerGetChanges(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")

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
	if a.LastSeq != a4.UpdateSeq {
		t.Errorf("expected last_seq %d, got %d", a4.UpdateSeq, a.LastSeq)
	}

	req, _ = http.NewRequest("DELETE", "/testdb", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
}

func TestHandlerChangesInvalidFeed(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")

	req, _ := http.NewRequest("PUT", "/feeddb", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	req, _ = http.NewRequest("GET", "/feeddb/_changes?feed=websocket", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandlerChangesEventSource(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")
	ts := httptest.NewServer(handler)
	defer ts.Close()

	delReq, _ := http.NewRequest("DELETE", ts.URL+"/ssedb", nil)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatal(err)
	}
	delResp.Body.Close()

	req, _ := http.NewRequest("PUT", ts.URL+"/ssedb", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create db: %d %s", resp.StatusCode, body)
	}

	// Discover current seq so we only wait for the new write.
	resp, err = http.Get(ts.URL + "/ssedb/_changes?since=0")
	if err != nil {
		t.Fatal(err)
	}
	var baseline ChangesResult
	if err := json.NewDecoder(resp.Body).Decode(&baseline); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ = http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/ssedb/_changes?feed=eventsource&since=%d", ts.URL, baseline.LastSeq), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sse status %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "text/event-stream" {
		t.Fatalf("content-type %q", ct)
	}

	type sseHit struct {
		change Change
		err    error
	}
	hits := make(chan sseHit, 1)
	go func() {
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 512)
		for {
			n, err := resp.Body.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
				for {
					idx := bytes.Index(buf, []byte("\n\n"))
					if idx < 0 {
						break
					}
					frame := string(buf[:idx])
					buf = buf[idx+2:]
					for _, line := range strings.Split(frame, "\n") {
						line = strings.TrimSuffix(line, "\r")
						if !strings.HasPrefix(line, "data:") {
							continue
						}
						payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
						var ch Change
						if err := json.Unmarshal([]byte(payload), &ch); err != nil {
							hits <- sseHit{err: err}
							return
						}
						if ch.ID == "livedoc" {
							hits <- sseHit{change: ch}
							return
						}
					}
				}
			}
			if err != nil {
				hits <- sseHit{err: err}
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	putBody := bytes.NewBufferString(`{"hello":"sse"}`)
	putReq, _ := http.NewRequest("PUT", ts.URL+"/ssedb/livedoc", putBody)
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	putBytes, _ := io.ReadAll(putResp.Body)
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("put doc: %d %s", putResp.StatusCode, putBytes)
	}

	select {
	case hit := <-hits:
		if hit.err != nil {
			t.Fatalf("sse read: %v", hit.err)
		}
		if hit.change.ID != "livedoc" || hit.change.Rev != 1 {
			t.Fatalf("unexpected change: %+v", hit.change)
		}
		if hit.change.UpdateSeq <= baseline.LastSeq {
			t.Fatalf("expected update_seq > %d, got %d", baseline.LastSeq, hit.change.UpdateSeq)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for SSE change")
	}
	cancel()
}

func TestHandlerGetDocument(t *testing.T) {
	kdb, _ := NewKDB()
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
	handler := NewRouter(kdb, "")

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
