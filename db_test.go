package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

func testDB(t *testing.T, name string) (*KDB, string) {
	t.Helper()
	dir := t.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		t.Fatalf("NewKDBWithDataDir: %v", err)
	}
	if err := kdb.Open(name, true); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return kdb, dir
}

func TestDatabasePutGetDelete(t *testing.T) {
	kdb, _ := testDB(t, "testdb")

	doc, err := ParseDocument([]byte(`{"_id":"a","title":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := kdb.PutDocument("testdb", doc)
	if err != nil {
		t.Fatal(err)
	}
	if out.Version != 1 {
		t.Fatalf("expected rev 1, got %d", out.Version)
	}

	got, err := kdb.GetDocument("testdb", &Document{ID: "a"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 {
		t.Fatalf("expected rev 1, got %d", got.Version)
	}

	del, err := kdb.DeleteDocument("testdb", &Document{ID: "a", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !del.Deleted {
		t.Fatal("expected deleted flag")
	}
}

func TestDatabaseOCCConflict(t *testing.T) {
	kdb, _ := testDB(t, "occdb")

	doc, _ := ParseDocument([]byte(`{"_id":"x","v":1}`))
	if _, err := kdb.PutDocument("occdb", doc); err != nil {
		t.Fatal(err)
	}

	bad, _ := ParseDocument([]byte(`{"_id":"x","v":2}`))
	_, err := kdb.PutDocument("occdb", bad)
	if !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	stale, _ := ParseDocument([]byte(`{"_id":"x","_rev":99,"v":3}`))
	_, err = kdb.PutDocument("occdb", stale)
	if !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("expected conflict on stale rev, got %v", err)
	}

	ok, _ := ParseDocument([]byte(`{"_id":"x","_rev":1,"v":3}`))
	out, err := kdb.PutDocument("occdb", ok)
	if err != nil {
		t.Fatal(err)
	}
	if out.Version != 2 {
		t.Fatalf("expected rev 2, got %d", out.Version)
	}
}

func TestDatabaseRecreateAfterDelete(t *testing.T) {
	kdb, _ := testDB(t, "recreate")

	doc, _ := ParseDocument([]byte(`{"_id":"r1","n":1}`))
	if _, err := kdb.PutDocument("recreate", doc); err != nil {
		t.Fatal(err)
	}
	if _, err := kdb.DeleteDocument("recreate", &Document{ID: "r1", Version: 1}); err != nil {
		t.Fatal(err)
	}

	again, _ := ParseDocument([]byte(`{"_id":"r1","_rev":2,"n":2}`))
	out, err := kdb.PutDocument("recreate", again)
	if err != nil {
		t.Fatal(err)
	}
	if out.Version < 3 {
		t.Fatalf("expected rev >= 3 after recreate, got %d", out.Version)
	}
}

func TestDatabaseStatAndChanges(t *testing.T) {
	kdb, _ := testDB(t, "statdb")

	doc, _ := ParseDocument([]byte(`{"_id":"s1"}`))
	if _, err := kdb.PutDocument("statdb", doc); err != nil {
		t.Fatal(err)
	}

	stat, err := kdb.DBStat("statdb")
	if err != nil {
		t.Fatal(err)
	}
	if stat.DocCount < 1 {
		t.Fatalf("expected doc_count >= 1, got %d", stat.DocCount)
	}
	if stat.UpdateSeq < 1 {
		t.Fatalf("expected update_seq >= 1, got %d", stat.UpdateSeq)
	}

	changes, err := kdb.Changes("statdb", 0, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) == 0 {
		t.Fatal("expected changes payload")
	}
}

func TestNewDatabaseServiceLocatorPaths(t *testing.T) {
	dir := t.TempDir()
	sl := NewServiceLocator(dir)
	if sl.GetDBDirPath() != filepath.Join(dir, "dbs") {
		t.Fatalf("unexpected db path %s", sl.GetDBDirPath())
	}
	if sl.GetViewDirPath() != filepath.Join(dir, "views") {
		t.Fatalf("unexpected view path %s", sl.GetViewDirPath())
	}
}

func TestDatabaseAllDocsView(t *testing.T) {
	kdb, _ := testDB(t, "alldocs")
	doc, _ := ParseDocument([]byte(`{"_id":"d1","title":"t"}`))
	if _, err := kdb.PutDocument("alldocs", doc); err != nil {
		t.Fatal(err)
	}

	rs, err := kdb.SelectView("alldocs", "_design/_views", "_all_docs", "default", url.Values{
		"limit":  []string{"10"},
		"offset": []string{"0"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) == 0 {
		t.Fatal("expected all_docs result")
	}
}

func TestOpenReadersNeverEnqueuesNil(t *testing.T) {
	kdb, _ := testDB(t, "c7readers")
	db := kdb.dbs["c7readers"].(*DefaultDatabase)

	n := cap(db.reader)
	pending := make([]DatabaseReader, 0, n)
	for i := 0; i < n; i++ {
		pending = append(pending, <-db.reader)
	}
	for i := 0; i < n; i++ {
		db.reader <- &failOpenReader{}
	}

	err := db.openReaders()
	if !errors.Is(err, errFakeOpen) {
		t.Fatalf("expected fake open error, got %v", err)
	}
	if len(db.reader) != n {
		t.Fatalf("expected %d readers in pool after failure, got %d", n, len(db.reader))
	}
	for i := 0; i < n; i++ {
		r := <-db.reader
		if r == nil {
			t.Fatal("nil reader enqueued after Open failure")
		}
		db.reader <- r
	}

	// Restore working readers so cleanup can Close.
	for i := 0; i < n; i++ {
		<-db.reader
	}
	for _, r := range pending {
		db.reader <- r
	}
}

func TestCloseWriterFailureReturnsToken(t *testing.T) {
	kdb, _ := testDB(t, "c8close")
	db := kdb.dbs["c8close"].(*DefaultDatabase)

	real := <-db.writer
	db.writer <- &failCloseWriter{DatabaseWriter: real, failClose: true}

	err := db.Close(false)
	if !errors.Is(err, errFakeClose) {
		t.Fatalf("expected fake close error, got %v", err)
	}

	// Writer token must be available; Put must not hang.
	done := make(chan error, 1)
	go func() {
		doc, _ := ParseDocument([]byte(`{"_id":"after_close_fail"}`))
		_, err := kdb.PutDocument("c8close", doc)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Put after failed Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put hung — writer token likely dropped on Close failure")
	}

	// Allow clean Close: unwrap fake.
	fake := <-db.writer
	fw := fake.(*failCloseWriter)
	fw.failClose = false
	db.writer <- fw
}

func TestCountDeltaOnDeleteUndelete(t *testing.T) {
	kdb, _ := testDB(t, "h5counts")
	db := kdb.dbs["h5counts"].(*DefaultDatabase)
	baseDocs := db.DocumentCount()
	baseDeleted := db.DeletedDocumentCount()

	doc, _ := ParseDocument([]byte(`{"_id":"c1","n":1}`))
	if _, err := kdb.PutDocument("h5counts", doc); err != nil {
		t.Fatal(err)
	}
	if db.DocumentCount() != baseDocs+1 {
		t.Fatalf("after put: docs=%d want %d", db.DocumentCount(), baseDocs+1)
	}

	if _, err := kdb.DeleteDocument("h5counts", &Document{ID: "c1", Version: 1}); err != nil {
		t.Fatal(err)
	}
	if db.DocumentCount() != baseDocs || db.DeletedDocumentCount() != baseDeleted+1 {
		t.Fatalf("after delete: docs=%d deleted=%d", db.DocumentCount(), db.DeletedDocumentCount())
	}

	// Re-delete with correct rev should conflict / no-op counts if conflict.
	_, err := kdb.DeleteDocument("h5counts", &Document{ID: "c1", Version: 1})
	if !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("expected conflict re-deleting with stale rev, got %v", err)
	}
	if db.DocumentCount() != baseDocs || db.DeletedDocumentCount() != baseDeleted+1 {
		t.Fatalf("counts changed on conflicting re-delete: docs=%d deleted=%d", db.DocumentCount(), db.DeletedDocumentCount())
	}

	again, _ := ParseDocument([]byte(`{"_id":"c1","_rev":2,"n":2}`))
	if _, err := kdb.PutDocument("h5counts", again); err != nil {
		t.Fatal(err)
	}
	if db.DocumentCount() != baseDocs+1 || db.DeletedDocumentCount() != baseDeleted {
		t.Fatalf("after undelete: docs=%d deleted=%d", db.DocumentCount(), db.DeletedDocumentCount())
	}
}

func TestBulkPutAllOrNothingSuccess(t *testing.T) {
	kdb, _ := testDB(t, "bulkatomic")
	db := kdb.dbs["bulkatomic"].(*DefaultDatabase)
	countBefore := db.DocumentCount()
	seqBefore := db.UpdateSequence()

	body := []byte(`{"all_or_nothing":true,"_docs":[{"_id":"a"},{"_id":"b"}]}`)
	result, err := kdb.BulkDocuments("bulkatomic", body)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", result.StatusCode, result.Body)
	}
	if db.UpdateSequence() != seqBefore+2 {
		t.Fatalf("expected update_seq %d, got %d", seqBefore+2, db.UpdateSequence())
	}
	if db.DocumentCount() != countBefore+2 {
		t.Fatalf("expected %d docs, got %d", countBefore+2, db.DocumentCount())
	}
}

func TestBulkPutAllOrNothingRollback(t *testing.T) {
	kdb, _ := testDB(t, "bulkrollback")
	db := kdb.dbs["bulkrollback"].(*DefaultDatabase)

	okBody := []byte(`{"_docs":[{"_id":"seed"}]}`)
	if _, err := kdb.BulkDocuments("bulkrollback", okBody); err != nil {
		t.Fatal(err)
	}
	seqBefore := db.UpdateSequence()
	countBefore := db.DocumentCount()

	body := []byte(`{"all_or_nothing":true,"_docs":[{"_id":"good"},{"_id":"seed","_rev":99}]}`)
	result, err := kdb.BulkDocuments("bulkrollback", body)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d body=%s", result.StatusCode, result.Body)
	}
	if db.UpdateSequence() != seqBefore {
		t.Fatalf("update_seq changed on rollback: before=%d after=%d", seqBefore, db.UpdateSequence())
	}
	if db.DocumentCount() != countBefore {
		t.Fatalf("doc count changed on rollback: before=%d after=%d", countBefore, db.DocumentCount())
	}

	var resp map[string]json.RawMessage
	if err := json.Unmarshal(result.Body, &resp); err != nil {
		t.Fatalf("invalid bulk_failed JSON: %v", err)
	}
	if string(resp["error"]) != `"bulk_failed"` {
		t.Fatalf("expected bulk_failed error, got %s", resp["error"])
	}
}

func TestBulkPutPartialSuccessUnchanged(t *testing.T) {
	kdb, _ := testDB(t, "bulkpartial")
	db := kdb.dbs["bulkpartial"].(*DefaultDatabase)
	countBefore := db.DocumentCount()

	body := []byte(`{"_docs":[{"_id":"p1"},{"_id":"p1"}]}`)
	result, err := kdb.BulkDocuments("bulkpartial", body)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", result.StatusCode)
	}
	if db.DocumentCount() != countBefore+1 {
		t.Fatalf("partial bulk should commit first doc, got count %d want %d", db.DocumentCount(), countBefore+1)
	}
}
