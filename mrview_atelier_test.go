package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAtelierDryRunGoodWindow(t *testing.T) {
	kdb, dir := testDB(t, "atelier1")

	for _, body := range []string{
		`{"_id":"a","n":1}`,
		`{"_id":"b","n":2}`,
		`{"_id":"c","n":3}`,
	} {
		doc, err := ParseDocument([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := kdb.PutDocument("atelier1", doc); err != nil {
			t.Fatal(err)
		}
	}

	viewsBefore, _ := filepath.Glob(filepath.Join(dir, "views", "*.db"))

	req := ViewAtelierDryRunRequest{
		Setup: []string{
			"CREATE TABLE IF NOT EXISTS items (key TEXT PRIMARY KEY, n INT) WITHOUT ROWID",
		},
		Run: []string{
			`INSERT OR REPLACE INTO items (key, n)
			 SELECT doc_id, json_extract(data, '$.n') FROM latest_documents WHERE deleted = 0`,
		},
		Select: map[string]string{
			"default": `SELECT json_group_array(json_object('id', key, 'n', n)) FROM items ORDER BY key LIMIT ${limit}`,
		},
		Since:      0,
		Limit:      300,
		SelectName: "default",
		Params:     map[string]string{"limit": "10"},
	}

	result, err := kdb.DryRunView("atelier1", "_design/x", "items", req)
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if !result.OK {
		t.Fatalf("expected ok, got %#v", result)
	}
	if result.DocsInWindow < 3 {
		t.Fatalf("expected docs_in_window >= 3, got %d", result.DocsInWindow)
	}
	if result.NextSeq <= result.CurrentSeq {
		t.Fatalf("expected next > current, got current=%d next=%d", result.CurrentSeq, result.NextSeq)
	}
	found := false
	for _, tbl := range result.Tables {
		if tbl.Name == "items" {
			found = true
			if tbl.Rows < 3 {
				t.Fatalf("expected items rows >= 3, got %d", tbl.Rows)
			}
		}
	}
	if !found {
		t.Fatalf("expected items table in counts: %#v", result.Tables)
	}
	if len(result.Result) == 0 {
		t.Fatal("expected select result")
	}

	viewsAfter, _ := filepath.Glob(filepath.Join(dir, "views", "*.db"))
	if len(viewsAfter) != len(viewsBefore) {
		t.Fatalf("dry-run wrote view files: before=%d after=%d", len(viewsBefore), len(viewsAfter))
	}
}

func TestAtelierDryRunBadSetup(t *testing.T) {
	kdb, _ := testDB(t, "atelier2")

	req := ViewAtelierDryRunRequest{
		Setup: []string{"CREATE TABLE broken ("},
		Run:   []string{"SELECT 1"},
	}
	result, err := kdb.DryRunView("atelier2", "_design/x", "v", req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidSQLStmt) {
		t.Fatalf("expected ErrInvalidSQLStmt, got %v", err)
	}
	if result == nil || result.OK || len(result.Errors) == 0 {
		t.Fatalf("expected structured errors, got %#v", result)
	}
	if result.Errors[0].Phase != "setup" || result.Errors[0].Index != 0 {
		t.Fatalf("expected setup[0], got %#v", result.Errors[0])
	}
	if !strings.HasPrefix(result.Reason, "setup[0]:") {
		t.Fatalf("unexpected reason %q", result.Reason)
	}
}

func TestAtelierDryRunBadRun(t *testing.T) {
	kdb, _ := testDB(t, "atelier3")

	req := ViewAtelierDryRunRequest{
		Setup: []string{"CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY) WITHOUT ROWID"},
		Run:   []string{"INSERT INTO t (id) SELECT no_such_column FROM latest_documents"},
	}
	result, err := kdb.DryRunView("atelier3", "_design/x", "v", req)
	if err == nil {
		t.Fatal("expected error")
	}
	if result == nil || len(result.Errors) == 0 || result.Errors[0].Phase != "run" {
		t.Fatalf("expected run error, got %#v", result)
	}
	if !strings.HasPrefix(result.Reason, "run[0]:") {
		t.Fatalf("unexpected reason %q", result.Reason)
	}
}

func TestAtelierDryRunSelectParam(t *testing.T) {
	kdb, _ := testDB(t, "atelier4")
	doc, _ := ParseDocument([]byte(`{"_id":"p1","v":9}`))
	if _, err := kdb.PutDocument("atelier4", doc); err != nil {
		t.Fatal(err)
	}

	req := ViewAtelierDryRunRequest{
		Setup: []string{"CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v INT) WITHOUT ROWID"},
		Run: []string{
			`INSERT OR REPLACE INTO t (id, v) SELECT doc_id, json_extract(data,'$.v') FROM latest_documents WHERE deleted = 0`,
		},
		Select: map[string]string{
			"by_id": `SELECT json_object('id', id, 'v', v) FROM t WHERE id = ${id}`,
		},
		SelectName: "by_id",
		Params:     map[string]string{"id": "p1"},
	}
	result, err := kdb.DryRunView("atelier4", "_design/x", "v", req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.Result), `"id":"p1"`) && !strings.Contains(string(result.Result), `"id": "p1"`) {
		t.Fatalf("unexpected select result %s", result.Result)
	}
}

func TestAtelierDryRunRejectsKeyword(t *testing.T) {
	kdb, _ := testDB(t, "atelier5")
	req := ViewAtelierDryRunRequest{
		Setup: []string{"DROP TABLE IF EXISTS x"},
	}
	result, err := kdb.DryRunView("atelier5", "_design/x", "v", req)
	if err == nil {
		t.Fatal("expected keyword rejection")
	}
	if result == nil || !strings.Contains(result.Reason, "invalid keyword: DROP") {
		t.Fatalf("unexpected reason %#v", result)
	}
}

func TestAtelierDryRunIncludeSQL(t *testing.T) {
	kdb, _ := testDB(t, "atelier6")
	doc, _ := ParseDocument([]byte(`{"_id":"z","n":1}`))
	if _, err := kdb.PutDocument("atelier6", doc); err != nil {
		t.Fatal(err)
	}
	req := ViewAtelierDryRunRequest{
		Setup:      []string{"CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY) WITHOUT ROWID"},
		Run:        []string{`INSERT OR REPLACE INTO t (id) SELECT doc_id FROM latest_documents WHERE deleted = 0`},
		IncludeSQL: true,
	}
	result, err := kdb.DryRunView("atelier6", "_design/x", "v", req)
	if err != nil {
		t.Fatal(err)
	}
	if result.GeneratedSQL == "" || !strings.Contains(result.GeneratedSQL, "BEGIN;") {
		t.Fatalf("expected generated SQL, got %q", result.GeneratedSQL)
	}
}

func TestViewStatusLag(t *testing.T) {
	kdb, dir := testDB(t, "atelier7")

	status, err := kdb.GetViewStatus("atelier7", "_design/_views", "_all_docs")
	if err != nil {
		t.Fatal(err)
	}
	if status.Built && status.Open {
		// may or may not be open yet
	}
	_ = dir

	// Force a build via select so the view catches up.
	if _, err := kdb.SelectView("atelier7", "_design/_views", "_all_docs", "default", map[string][]string{"limit": {"10"}}, false); err != nil {
		t.Fatal(err)
	}

	stat, err := kdb.DBStat("atelier7")
	if err != nil {
		t.Fatal(err)
	}
	status, err = kdb.GetViewStatus("atelier7", "_design/_views", "_all_docs")
	if err != nil {
		t.Fatal(err)
	}
	if status.Lag != 0 {
		t.Fatalf("expected lag 0 after build, got %#v db_seq=%d", status, stat.UpdateSeq)
	}
	if status.ViewUpdateSeq != stat.UpdateSeq {
		t.Fatalf("view seq %d != db seq %d", status.ViewUpdateSeq, stat.UpdateSeq)
	}

	doc, _ := ParseDocument([]byte(`{"_id":"lagme","x":1}`))
	if _, err := kdb.PutDocument("atelier7", doc); err != nil {
		t.Fatal(err)
	}
	// Read with stale so view does not catch up.
	if _, err := kdb.SelectView("atelier7", "_design/_views", "_all_docs", "default", map[string][]string{"limit": {"10"}}, true); err != nil {
		t.Fatal(err)
	}
	status, err = kdb.GetViewStatus("atelier7", "_design/_views", "_all_docs")
	if err != nil {
		t.Fatal(err)
	}
	if status.Lag < 1 {
		t.Fatalf("expected lag >= 1 after new doc with stale read, got %#v", status)
	}
}

func TestViewStatusMissingView(t *testing.T) {
	kdb, _ := testDB(t, "atelier8")
	status, err := kdb.GetViewStatus("atelier8", "_design/missing", "noview")
	if err != nil {
		t.Fatal(err)
	}
	if status.Built || status.Open {
		t.Fatalf("expected not built, got %#v", status)
	}
	if status.ViewUpdateSeq != 0 {
		t.Fatalf("expected view_update_seq 0, got %d", status.ViewUpdateSeq)
	}
}

func TestValidateDesignDocumentReasonFormat(t *testing.T) {
	kdb, _ := testDB(t, "atelier9")
	body := `{"_id":"_design/bad","views":{"v":{"setup":["CREATE TABLE broken ("],"run":["SELECT 1"],"select":{"default":"SELECT 1"}}}}`
	parsed, err := ParseDocument([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	_, err = kdb.PutDocument("atelier9", parsed)
	if err == nil {
		t.Fatal("expected validation failure")
	}
	if !errors.Is(err, ErrInvalidSQLStmt) {
		t.Fatalf("expected ErrInvalidSQLStmt, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "setup[0]:") {
		t.Fatalf("expected phase/index in error, got %q", msg)
	}
}

func TestValidateDesignDocumentKeywordReasonFormat(t *testing.T) {
	kdb, _ := testDB(t, "atelier10")
	cur, err := kdb.GetDocument("atelier10", &Document{ID: "_design/_views"}, true)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"_id":"_design/_views","_rev":` + itoa(cur.Version) + `,"views":{"bad":{"setup":["DROP TABLE IF EXISTS x"],"run":["SELECT 1"],"select":{"default":"SELECT 1"}}}}`)
	parsed, err := ParseDocument(body)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kdb.PutDocument("atelier10", parsed)
	if err == nil {
		t.Fatal("expected keyword rejection")
	}
	if !strings.Contains(err.Error(), "invalid keyword: DROP") {
		t.Fatalf("unexpected error %q", err.Error())
	}
}

func TestAtelierDryRunDoesNotCreateViewDirFiles(t *testing.T) {
	kdb, dir := testDB(t, "atelier11")
	viewDir := filepath.Join(dir, "views")
	before, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatal(err)
	}
	req := ViewAtelierDryRunRequest{
		Setup: []string{"CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY) WITHOUT ROWID"},
		Run:   []string{`INSERT OR IGNORE INTO t (id) SELECT doc_id FROM latest_documents`},
	}
	if _, err := kdb.DryRunView("atelier11", "_design/x", "v", req); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("view dir changed: before=%d after=%d", len(before), len(after))
	}
}
