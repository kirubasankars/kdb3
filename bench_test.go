package main

import (
	"fmt"
	"net/url"
	"testing"
)

func BenchmarkBulkPut(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			kdb, err := NewKDBWithDataDir(dir)
			if err != nil {
				b.Fatal(err)
			}
			if err := kdb.Open("benchbulk", true); err != nil {
				b.Fatal(err)
			}
			docs := make([]*Document, n)
			for i := 0; i < n; i++ {
				docs[i], _ = ParseDocument([]byte(fmt.Sprintf(`{"_id":"b%d","n":%d}`, i, i)))
			}
			db := kdb.dbs["benchbulk"].(*DefaultDatabase)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// unique ids per iteration
				batch := make([]*Document, n)
				for j := 0; j < n; j++ {
					batch[j], _ = ParseDocument([]byte(fmt.Sprintf(`{"_id":"b%d_%d","n":%d}`, i, j, j)))
				}
				_, errs := db.BulkPutDocuments(batch, BulkPutOptions{})
				for _, e := range errs {
					if e != nil {
						b.Fatal(e)
					}
				}
			}
			_ = docs
		})
	}
}

func BenchmarkViewSelect_Stale(b *testing.B) {
	dir := b.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := kdb.Open("benchview", true); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		doc, _ := ParseDocument([]byte(fmt.Sprintf(`{"_id":"d%d","n":%d}`, i, i)))
		if _, err := kdb.PutDocument("benchview", doc); err != nil {
			b.Fatal(err)
		}
	}
	// Warm view.
	_, _ = kdb.SelectView("benchview", "_design/_views", "_all_docs", "default", url.Values{
		"limit": []string{"10"}, "offset": []string{"0"},
	}, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := kdb.SelectView("benchview", "_design/_views", "_all_docs", "default", url.Values{
			"limit": []string{"10"}, "offset": []string{"0"},
		}, true)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetDocument(b *testing.B) {
	dir := b.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := kdb.Open("benchget", true); err != nil {
		b.Fatal(err)
	}
	doc, _ := ParseDocument([]byte(`{"_id":"g1","payload":"hello"}`))
	if _, err := kdb.PutDocument("benchget", doc); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kdb.GetDocument("benchget", &Document{ID: "g1"}, true); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDBStat(b *testing.B) {
	dir := b.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := kdb.Open("benchstat", true); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kdb.DBStat("benchstat"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBulkGet(b *testing.B) {
	dir := b.TempDir()
	kdb, err := NewKDBWithDataDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := kdb.Open("benchbg", true); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		doc, _ := ParseDocument([]byte(fmt.Sprintf(`{"_id":"g%d"}`, i)))
		if _, err := kdb.PutDocument("benchbg", doc); err != nil {
			b.Fatal(err)
		}
	}
	body := []byte(`{"_docs":[{"_id":"g0"},{"_id":"g1"},{"_id":"g2"},{"_id":"missing"}]}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kdb.BulkGetDocuments("benchbg", body); err != nil {
			b.Fatal(err)
		}
	}
}
