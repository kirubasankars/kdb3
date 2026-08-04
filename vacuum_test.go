package main

import (
	"errors"
	"os"
	"path/filepath"
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
