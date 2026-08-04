package main

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentVacuumAndDelete(t *testing.T) {
	kdb, _ := testDB(t, "c2vacdel")
	for i := 0; i < 20; i++ {
		doc, _ := ParseDocument([]byte(fmt.Sprintf(`{"_id":"d%d","n":%d}`, i, i)))
		if _, err := kdb.PutDocument("c2vacdel", doc); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_, _ = kdb.DeleteDocument("c2vacdel", &Document{ID: fmt.Sprintf("d%d", i), Version: 1})
		}
	}()
	go func() {
		defer wg.Done()
		if err := kdb.Vacuum("c2vacdel"); err != nil {
			errCh <- err
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	// Live docs that were not deleted should still be readable.
	if _, err := kdb.GetDocument("c2vacdel", &Document{ID: "d15"}, true); err != nil {
		t.Fatalf("expected live doc after vacuum||delete: %v", err)
	}
}

func TestConcurrentVacuumAndSelectView(t *testing.T) {
	kdb, _ := testDB(t, "c3vacview")
	doc, _ := ParseDocument([]byte(`{"_id":"v1","n":1}`))
	if _, err := kdb.PutDocument("c3vacview", doc); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			_, err := kdb.SelectView("c3vacview", "_design/_views", "_all_docs", "default", url.Values{
				"limit":  []string{"10"},
				"offset": []string{"0"},
			}, false)
			// Transient SQLite busy/locked during compact is OK; hang/deadlock is not.
			if err != nil && err != ErrDatabaseNotFound && err != ErrViewNotFound &&
				!strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy") {
				done <- err
				return
			}
		}
	}()

	for i := 0; i < 3; i++ {
		var err error
		for attempt := 0; attempt < 5; attempt++ {
			err = kdb.Vacuum("c3vacview")
			if err == nil || (!strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy")) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			close(stop)
			t.Fatal(err)
		}
	}
	close(stop)

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timeout: vacuum||SelectView likely deadlocked")
	}
}

func TestConcurrentLocalDBUpdateAndList(t *testing.T) {
	dir := t.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := kdb.Open("c4a", true); err != nil {
		t.Fatal(err)
	}
	if err := kdb.Open("c4b", true); err != nil {
		t.Fatal(err)
	}

	local := kdb.serviceLocator.GetLocalDB()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = local.UpdateDatabaseFileName("c4a", fmt.Sprintf("c4a_renamed_%d", i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := local.ListDatabases(); err != nil {
				t.Errorf("ListDatabases: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}

func TestConcurrentPutStatAndView(t *testing.T) {
	kdb, _ := testDB(t, "h1race")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				doc, _ := ParseDocument([]byte(fmt.Sprintf(`{"_id":"p%d_%d","n":%d}`, n, j, j)))
				_, _ = kdb.PutDocument("h1race", doc)
				_, _ = kdb.DBStat("h1race")
				_, _ = kdb.SelectView("h1race", "_design/_views", "_all_docs", "default", url.Values{
					"limit":  []string{"5"},
					"offset": []string{"0"},
				}, true)
			}
		}(i)
	}
	wg.Wait()
	stat, err := kdb.DBStat("h1race")
	if err != nil {
		t.Fatal(err)
	}
	if stat.DocCount < 1 || stat.UpdateSeq < 1 {
		t.Fatalf("unexpected stats after concurrent puts: %+v", stat)
	}
}

func TestVacuumDoesNotBlockOtherDBOpen(t *testing.T) {
	dir := t.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := kdb.Open("h2a", true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		doc, _ := ParseDocument([]byte(fmt.Sprintf(`{"_id":"x%d"}`, i)))
		if _, err := kdb.PutDocument("h2a", doc); err != nil {
			t.Fatal(err)
		}
	}

	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		close(started)
		errCh <- kdb.Vacuum("h2a")
	}()
	<-started

	openDone := make(chan error, 1)
	go func() {
		openDone <- kdb.Open("h2b", true)
	}()

	select {
	case err := <-openDone:
		if err != nil {
			t.Fatalf("Open dbB during vacuum: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Open dbB blocked too long while vacuuming dbA")
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
