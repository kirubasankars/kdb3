package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// --- helpers ---

func repCreateDB(t *testing.T, base, db string) {
	t.Helper()
	req, _ := http.NewRequest("PUT", base+"/"+db, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create db %s: %v", db, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("create db %s: status %d", db, resp.StatusCode)
	}
}

func repPutDoc(t *testing.T, base, db, id, body string) int {
	t.Helper()
	req, _ := http.NewRequest("PUT", base+"/"+db+"/"+id, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put doc %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("put doc %s: status %d body %s", id, resp.StatusCode, b)
	}
	var out struct {
		Rev int `json:"_rev"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return out.Rev
}

func repDeleteDoc(t *testing.T, base, db, id string, rev int) {
	t.Helper()
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/%s/%s?rev=%d", base, db, id, rev), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete doc %s: %v", id, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete doc %s: status %d", id, resp.StatusCode)
	}
}

func repGetDoc(t *testing.T, base, db, id string) (map[string]any, int) {
	t.Helper()
	resp, err := http.Get(base + "/" + db + "/" + id)
	if err != nil {
		t.Fatalf("get doc %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	var doc map[string]any
	json.NewDecoder(resp.Body).Decode(&doc)
	return doc, resp.StatusCode
}

func repReplicate(t *testing.T, base string, req ReplicationRequest) (*ReplicationResult, int) {
	t.Helper()
	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest("POST", base+"/_replicate", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	var result ReplicationResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode replicate result: %v (body %s)", err, raw)
	}
	return &result, resp.StatusCode
}

func jsonRaw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func waitForDoc(t *testing.T, base, db, id string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		doc, code := repGetDoc(t, base, db, id)
		if code == http.StatusOK {
			return doc
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s/%s", db, id)
	return nil
}

// --- tests ---

func TestReplicateOneShotLocal(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	repCreateDB(t, ts.URL, "src")
	repPutDoc(t, ts.URL, "src", "a", `{"_id":"a","name":"alice"}`)
	repPutDoc(t, ts.URL, "src", "b", `{"_id":"b","name":"bob"}`)

	result, code := repReplicate(t, ts.URL, ReplicationRequest{
		Source:       jsonRaw("src"),
		Target:       jsonRaw("dst"),
		CreateTarget: true,
	})
	if code != http.StatusOK {
		t.Fatalf("replicate status %d", code)
	}
	if !result.OK || result.DocsWritten < 2 {
		t.Fatalf("unexpected result: %+v", result)
	}

	doc, _ := repGetDoc(t, ts.URL, "dst", "a")
	if doc["name"] != "alice" {
		t.Fatalf("doc a not replicated: %+v", doc)
	}
	doc, _ = repGetDoc(t, ts.URL, "dst", "b")
	if doc["name"] != "bob" {
		t.Fatalf("doc b not replicated: %+v", doc)
	}
}

func TestReplicateIdempotentNoOps(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	repCreateDB(t, ts.URL, "src")
	repPutDoc(t, ts.URL, "src", "a", `{"_id":"a","name":"alice"}`)

	req := ReplicationRequest{Source: jsonRaw("src"), Target: jsonRaw("dst"), CreateTarget: true}
	if _, code := repReplicate(t, ts.URL, req); code != http.StatusOK {
		t.Fatalf("first replicate status %d", code)
	}
	// Second run should write nothing (identical content) and not bump revs.
	result, code := repReplicate(t, ts.URL, req)
	if code != http.StatusOK {
		t.Fatalf("second replicate status %d", code)
	}
	if result.DocsWritten != 0 {
		t.Fatalf("expected 0 docs written on re-run, got %d", result.DocsWritten)
	}
	doc, _ := repGetDoc(t, ts.URL, "dst", "a")
	if rev, ok := doc["_rev"].(float64); !ok || rev != 1 {
		t.Fatalf("rev should stay 1 after idempotent replication, got %v", doc["_rev"])
	}
}

func TestReplicateDeletionLocal(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	repCreateDB(t, ts.URL, "src")
	rev := repPutDoc(t, ts.URL, "src", "a", `{"_id":"a","name":"alice"}`)

	req := ReplicationRequest{Source: jsonRaw("src"), Target: jsonRaw("dst"), CreateTarget: true}
	repReplicate(t, ts.URL, req)
	if _, code := repGetDoc(t, ts.URL, "dst", "a"); code != http.StatusOK {
		t.Fatalf("doc a should exist on target before delete")
	}

	repDeleteDoc(t, ts.URL, "src", "a", rev)
	repReplicate(t, ts.URL, req)

	if _, code := repGetDoc(t, ts.URL, "dst", "a"); code != http.StatusNotFound {
		t.Fatalf("doc a should be deleted on target, got status %d", code)
	}
}

func TestReplicateContinuousLocal(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kdb.StopReplications()
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	repCreateDB(t, ts.URL, "src")
	repCreateDB(t, ts.URL, "dst")

	result, code := repReplicate(t, ts.URL, ReplicationRequest{
		Source:     jsonRaw("src"),
		Target:     jsonRaw("dst"),
		Continuous: true,
	})
	if code != http.StatusOK || !result.Continuous || result.ReplicationID == "" {
		t.Fatalf("continuous start failed: %+v (code %d)", result, code)
	}

	// _active_tasks should list the running replication.
	resp, _ := http.Get(ts.URL + "/_active_tasks")
	var tasks []map[string]any
	json.NewDecoder(resp.Body).Decode(&tasks)
	resp.Body.Close()
	if len(tasks) != 1 || tasks[0]["replication_id"] != result.ReplicationID {
		t.Fatalf("active_tasks unexpected: %+v", tasks)
	}

	// Write after replication started; continuous should carry it across.
	repPutDoc(t, ts.URL, "src", "live1", `{"_id":"live1","v":1}`)
	doc := waitForDoc(t, ts.URL, "dst", "live1", 5*time.Second)
	if doc["v"].(float64) != 1 {
		t.Fatalf("live1 not replicated correctly: %+v", doc)
	}

	// Update propagates too.
	rev := int(doc["_rev"].(float64))
	_ = rev
	repPutDoc(t, ts.URL, "src", "live2", `{"_id":"live2","v":2}`)
	waitForDoc(t, ts.URL, "dst", "live2", 5*time.Second)

	// Cancel.
	cancelResult, code := repReplicate(t, ts.URL, ReplicationRequest{
		Cancel:        true,
		ReplicationID: result.ReplicationID,
	})
	if code != http.StatusOK || !cancelResult.Cancelled {
		t.Fatalf("cancel failed: %+v (code %d)", cancelResult, code)
	}

	resp, _ = http.Get(ts.URL + "/_active_tasks")
	tasks = nil
	json.NewDecoder(resp.Body).Decode(&tasks)
	resp.Body.Close()
	if len(tasks) != 0 {
		t.Fatalf("expected no active tasks after cancel, got %+v", tasks)
	}
}

func TestReplicateCancelUnknown(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	_, code := repReplicate(t, ts.URL, ReplicationRequest{Cancel: true, ReplicationID: "does-not-exist"})
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 cancelling unknown replication, got %d", code)
	}
}

// TestReplicateRemotePull replicates FROM a remote kdb3 URL INTO a local db,
// exercising the remote HTTP read path and local write path.
func TestReplicateRemotePull(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	repCreateDB(t, ts.URL, "remotesrc")
	repPutDoc(t, ts.URL, "remotesrc", "x", `{"_id":"x","from":"remote"}`)

	result, code := repReplicate(t, ts.URL, ReplicationRequest{
		Source:       jsonRaw(ts.URL + "/remotesrc"),
		Target:       jsonRaw("localdst"),
		CreateTarget: true,
	})
	if code != http.StatusOK || result.DocsWritten < 1 {
		t.Fatalf("remote pull failed: %+v (code %d)", result, code)
	}
	doc, _ := repGetDoc(t, ts.URL, "localdst", "x")
	if doc["from"] != "remote" {
		t.Fatalf("remote doc not replicated: %+v", doc)
	}
}

// TestReplicateRemotePush replicates FROM a local db TO a remote kdb3 URL,
// exercising the remote HTTP write path (HEAD/PUT/DELETE).
func TestReplicateRemotePush(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	repCreateDB(t, ts.URL, "localsrc")
	rev := repPutDoc(t, ts.URL, "localsrc", "y", `{"_id":"y","pushed":true}`)

	req := ReplicationRequest{
		Source:       jsonRaw("localsrc"),
		Target:       jsonRaw(ts.URL + "/remotedst"),
		CreateTarget: true,
	}
	result, code := repReplicate(t, ts.URL, req)
	if code != http.StatusOK || result.DocsWritten < 1 {
		t.Fatalf("remote push failed: %+v (code %d)", result, code)
	}
	doc, _ := repGetDoc(t, ts.URL, "remotedst", "y")
	if doc["pushed"] != true {
		t.Fatalf("doc not pushed to remote target: %+v", doc)
	}

	// Now delete on source and re-push; remote target doc should be deleted.
	repDeleteDoc(t, ts.URL, "localsrc", "y", rev)
	repReplicate(t, ts.URL, req)
	if _, code := repGetDoc(t, ts.URL, "remotedst", "y"); code != http.StatusNotFound {
		t.Fatalf("doc should be deleted on remote target, got %d", code)
	}
}

func TestReplicateBadRequest(t *testing.T) {
	kdb, err := NewKDBWithDataDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(kdb, ""))
	defer ts.Close()

	// Missing target.
	_, code := repReplicate(t, ts.URL, ReplicationRequest{Source: jsonRaw("src")})
	if code == http.StatusOK {
		t.Fatalf("expected error for missing target")
	}

	// Non-existent source db.
	_, code = repReplicate(t, ts.URL, ReplicationRequest{Source: jsonRaw("nope"), Target: jsonRaw("dst"), CreateTarget: true})
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing source db, got %d", code)
	}
}
