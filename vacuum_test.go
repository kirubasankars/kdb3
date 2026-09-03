package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVacuumFailsWithoutRename(t *testing.T) {
	kdb, dir := testDB(t, "c1vacfail")
	doc, _ := ParseDocument([]byte(`{"_id":"keep","n":1}`))
	if _, err := kdb.PutDocument("c1vacfail", doc); err != nil {
		t.Fatal(err)
	}

	db := kdb.dbs["c1vacfail"].(*DefaultDatabase)
	local := db.serviceLocator.GetLocalDB()
	oldName := local.GetDatabaseFileName("c1vacfail")
	oldPath := filepath.Join(db.serviceLocator.GetDBDirPath(), oldName+dbExt)

	// Swap in failing vacuum manager.
	<-db.vacuumManager
	db.vacuumManager <- &failingVacuumManager{failAt: "copy"}

	err := kdb.Vacuum("c1vacfail")
	if !errors.Is(err, errFakeVacuum) {
		t.Fatalf("expected fake vacuum error, got %v", err)
	}

	if got := local.GetDatabaseFileName("c1vacfail"); got != oldName {
		t.Fatalf("filename renamed on failure: old=%s new=%s", oldName, got)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old db file missing after failed vacuum: %v", err)
	}
	if _, err := kdb.GetDocument("c1vacfail", &Document{ID: "keep"}, true); err != nil {
		t.Fatalf("document unreadable after failed vacuum: %v", err)
	}

	// Restore a working manager for cleanup.
	<-db.vacuumManager
	db.vacuumManager <- &DefaultVacuumManager{}
	_ = dir
}

func TestVacuumPreservesDocuments(t *testing.T) {
	kdb, dir := testDB(t, "vacdb")

	doc, _ := ParseDocument([]byte(`{"_id":"keep","val":42}`))
	out, err := kdb.PutDocument("vacdb", doc)
	if err != nil {
		t.Fatal(err)
	}
	seqBefore := out.Version

	statBefore, err := kdb.DBStat("vacdb")
	if err != nil {
		t.Fatal(err)
	}

	db := kdb.dbs["vacdb"].(*DefaultDatabase)
	oldName := db.serviceLocator.GetLocalDB().GetDatabaseFileName("vacdb")
	oldPath := filepath.Join(db.serviceLocator.GetDBDirPath(), oldName+dbExt)

	if err := kdb.Vacuum("vacdb"); err != nil {
		t.Fatal(err)
	}

	got, err := kdb.GetDocument("vacdb", &Document{ID: "keep"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != seqBefore {
		t.Fatalf("expected rev %d after vacuum, got %d", seqBefore, got.Version)
	}

	statAfter, err := kdb.DBStat("vacdb")
	if err != nil {
		t.Fatal(err)
	}
	if statAfter.UpdateSeq != statBefore.UpdateSeq {
		t.Fatalf("update_seq changed: before %d after %d", statBefore.UpdateSeq, statAfter.UpdateSeq)
	}
	if statAfter.DocCount != statBefore.DocCount {
		t.Fatalf("doc_count changed: before %d after %d", statBefore.DocCount, statAfter.DocCount)
	}

	if _, err := kdb.PutAttachment("vacdb", "keep", "blob.bin", "application/octet-stream", []byte("VAC"), got.Version); err != nil {
		t.Fatalf("put attachment: %v", err)
	}
	if err := kdb.Vacuum("vacdb"); err != nil {
		t.Fatal(err)
	}
	att, _, err := kdb.GetAttachment("vacdb", "keep", "blob.bin")
	if err != nil {
		t.Fatalf("attachment after vacuum: %v", err)
	}
	if string(att.Data) != "VAC" {
		t.Fatalf("attachment body after vacuum: %q", att.Data)
	}

	// Writes must work after vacuum (READONLY_DBMOVED / 1032 regression).
	doc2, _ := ParseDocument([]byte(`{"_id":"after","val":1}`))
	if _, err := kdb.PutDocument("vacdb", doc2); err != nil {
		t.Fatalf("put after vacuum: %v", err)
	}

	// Old DB + WAL/SHM sidecars must be gone.
	for _, p := range []string{oldPath, oldPath + "-wal", oldPath + "-shm"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("expected %s removed after vacuum, err=%v", p, err)
		}
	}
	_ = dir
}

func TestVacuumPurgesDeletedDocuments(t *testing.T) {
	kdb, _ := testDB(t, "vacpurge")

	live, _ := ParseDocument([]byte(`{"_id":"live","n":1}`))
	if _, err := kdb.PutDocument("vacpurge", live); err != nil {
		t.Fatal(err)
	}
	gone, _ := ParseDocument([]byte(`{"_id":"gone","n":2}`))
	if _, err := kdb.PutDocument("vacpurge", gone); err != nil {
		t.Fatal(err)
	}
	if _, err := kdb.DeleteDocument("vacpurge", &Document{ID: "gone", Version: 1}); err != nil {
		t.Fatal(err)
	}

	statBefore, err := kdb.DBStat("vacpurge")
	if err != nil {
		t.Fatal(err)
	}
	if statBefore.DeletedDocCount < 1 {
		t.Fatalf("expected deleted_doc_count >= 1 before vacuum, got %d", statBefore.DeletedDocCount)
	}

	if err := kdb.Vacuum("vacpurge"); err != nil {
		t.Fatal(err)
	}

	// Live doc still present.
	if _, err := kdb.GetDocument("vacpurge", &Document{ID: "live"}, true); err != nil {
		t.Fatalf("live doc missing after vacuum: %v", err)
	}

	// Tombstone purged — deleted doc should be gone.
	_, err = kdb.GetDocument("vacpurge", &Document{ID: "gone"}, true)
	if err == nil {
		t.Fatal("expected deleted doc to be purged after vacuum")
	}
	if !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("expected doc_not_found after vacuum purge, got %v", err)
	}

	statAfter, err := kdb.DBStat("vacpurge")
	if err != nil {
		t.Fatal(err)
	}
	if statAfter.DeletedDocCount != 0 {
		t.Fatalf("expected deleted_doc_count 0 after vacuum, got %d", statAfter.DeletedDocCount)
	}
	if statAfter.DocCount < 1 {
		t.Fatalf("expected live docs after vacuum, got doc_count %d", statAfter.DocCount)
	}
}

func TestVacuumRebuildsAllDocsDropsPurgedLinks(t *testing.T) {
	kdb, _ := testDB(t, "vaclinks")

	live, _ := ParseDocument([]byte(`{"_id":"keep_me","n":1}`))
	if _, err := kdb.PutDocument("vaclinks", live); err != nil {
		t.Fatal(err)
	}
	gone, _ := ParseDocument([]byte(`{"_id":"drop_me","n":2}`))
	if _, err := kdb.PutDocument("vaclinks", gone); err != nil {
		t.Fatal(err)
	}

	// Build _all_docs so the deleted id is indexed before vacuum.
	rs, err := kdb.SelectView("vaclinks", "_design/_views", "_all_docs", "default", url.Values{
		"limit": {"50"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rs), "drop_me") {
		t.Fatalf("expected drop_me in _all_docs before delete/vacuum, got %s", rs)
	}

	if _, err := kdb.DeleteDocument("vaclinks", &Document{ID: "drop_me", Version: 1}); err != nil {
		t.Fatal(err)
	}

	// Leave the view behind: stale=true so Build does not apply the tombstone.
	rs, err = kdb.SelectView("vaclinks", "_design/_views", "_all_docs", "default", url.Values{
		"limit": {"50"},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rs), "drop_me") {
		t.Fatalf("expected stale _all_docs to still list drop_me, got %s", rs)
	}

	statBefore, err := kdb.DBStat("vaclinks")
	if err != nil {
		t.Fatal(err)
	}

	if err := kdb.Vacuum("vaclinks"); err != nil {
		t.Fatal(err)
	}

	_, err = kdb.GetDocument("vaclinks", &Document{ID: "drop_me"}, true)
	if !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("expected purged doc_not_found, got %v", err)
	}

	rs, err = kdb.SelectView("vaclinks", "_design/_views", "_all_docs", "default", url.Values{
		"limit": {"50"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rs), "drop_me") {
		t.Fatalf("_all_docs still links to purged doc after vacuum: %s", rs)
	}
	if !strings.Contains(string(rs), "keep_me") {
		t.Fatalf("expected keep_me in _all_docs after vacuum, got %s", rs)
	}

	statAfter, err := kdb.DBStat("vaclinks")
	if err != nil {
		t.Fatal(err)
	}
	if statAfter.UpdateSeq < statBefore.UpdateSeq {
		t.Fatalf("update_seq rewound: before %d after %d", statBefore.UpdateSeq, statAfter.UpdateSeq)
	}
}

func TestVacuumPreservesUpdateSeqHighWater(t *testing.T) {
	kdb, _ := testDB(t, "vachwm")

	a, _ := ParseDocument([]byte(`{"_id":"a","n":1}`))
	if _, err := kdb.PutDocument("vachwm", a); err != nil {
		t.Fatal(err)
	}
	b, _ := ParseDocument([]byte(`{"_id":"b","n":2}`))
	if _, err := kdb.PutDocument("vachwm", b); err != nil {
		t.Fatal(err)
	}
	// Delete the highest-seq doc so MAX(live seq) drops after purge.
	if _, err := kdb.DeleteDocument("vachwm", &Document{ID: "b", Version: 1}); err != nil {
		t.Fatal(err)
	}

	statBefore, err := kdb.DBStat("vachwm")
	if err != nil {
		t.Fatal(err)
	}

	if err := kdb.Vacuum("vachwm"); err != nil {
		t.Fatal(err)
	}

	statAfter, err := kdb.DBStat("vachwm")
	if err != nil {
		t.Fatal(err)
	}
	if statAfter.UpdateSeq != statBefore.UpdateSeq {
		t.Fatalf("expected update_seq high-water %d preserved, got %d", statBefore.UpdateSeq, statAfter.UpdateSeq)
	}

	c, _ := ParseDocument([]byte(`{"_id":"c","n":3}`))
	out, err := kdb.PutDocument("vachwm", c)
	if err != nil {
		t.Fatal(err)
	}
	// PutDocument returns Version as rev, not update_seq — check DBStat / changes.
	statNew, err := kdb.DBStat("vachwm")
	if err != nil {
		t.Fatal(err)
	}
	if statNew.UpdateSeq <= statBefore.UpdateSeq {
		t.Fatalf("expected new update_seq > %d after put, got %d (doc rev %d)", statBefore.UpdateSeq, statNew.UpdateSeq, out.Version)
	}

	rs, err := kdb.SelectView("vachwm", "_design/_views", "_all_docs", "default", url.Values{
		"limit": {"50"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Rows []struct {
			ID string `json:"id"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(rs, &parsed); err != nil {
		t.Fatalf("parse _all_docs: %v body=%s", err, rs)
	}
	found := map[string]bool{}
	for _, r := range parsed.Rows {
		found[r.ID] = true
	}
	if !found["a"] || !found["c"] {
		t.Fatalf("expected a and c in _all_docs, got %#v", parsed.Rows)
	}
	if found["b"] {
		t.Fatal("purged b must not appear in _all_docs")
	}
}
