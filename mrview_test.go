package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestViewIncrementalUpdateAndSelect(t *testing.T) {
	kdb, _ := testDB(t, "viewdb")

	ddoc := `{
		"_id":"_design/posts",
		"views":{
			"by_title":{
				"setup":["CREATE TABLE IF NOT EXISTS posts (title, doc_id, PRIMARY KEY(doc_id)) WITHOUT ROWID"],
				"run":[
					"DELETE FROM posts WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
					"INSERT OR REPLACE INTO posts (title, doc_id) SELECT json_extract(data, '$.title'), doc_id FROM latest_documents WHERE deleted = 0 AND json_extract(data, '$.title') is not null"
				],
				"select":{
					"default":"SELECT JSON_OBJECT('rows', JSON_GROUP_ARRAY(JSON_OBJECT('title', title, 'id', doc_id))) FROM posts"
				}
			}
		}
	}`
	doc, err := ParseDocument([]byte(ddoc))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kdb.PutDocument("viewdb", doc); err != nil {
		t.Fatal(err)
	}

	p1, _ := ParseDocument([]byte(`{"_id":"p1","title":"one"}`))
	if _, err := kdb.PutDocument("viewdb", p1); err != nil {
		t.Fatal(err)
	}

	rs, err := kdb.SelectView("viewdb", "_design/posts", "by_title", "default", url.Values{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rs), "one") {
		t.Fatalf("expected view to contain title one, got %s", rs)
	}

	// missing view
	_, err = kdb.SelectView("viewdb", "_design/posts", "nope", "default", url.Values{}, false)
	if !errors.Is(err, ErrViewNotFound) {
		t.Fatalf("expected view_not_found, got %v", err)
	}

	// delete doc and ensure view updates
	if _, err := kdb.DeleteDocument("viewdb", &Document{ID: "p1", Version: 1}); err != nil {
		t.Fatal(err)
	}
	rs, err = kdb.SelectView("viewdb", "_design/posts", "by_title", "default", url.Values{}, false)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(rs, &payload); err != nil {
		t.Fatal(err)
	}
}

func TestViewMissingSelect(t *testing.T) {
	kdb, _ := testDB(t, "viewsel")
	_, err := kdb.SelectView("viewsel", "_design/_views", "_all_docs", "missing_select", url.Values{
		"limit":  []string{"10"},
		"offset": []string{"0"},
	}, false)
	if err == nil {
		t.Fatal("expected error for missing select")
	}
}

func TestViewStaleSelect(t *testing.T) {
	kdb, _ := testDB(t, "staledb")
	doc, _ := ParseDocument([]byte(`{"_id":"a1","x":1}`))
	if _, err := kdb.PutDocument("staledb", doc); err != nil {
		t.Fatal(err)
	}
	rs, err := kdb.SelectView("staledb", "_design/_views", "_all_docs", "default", url.Values{
		"limit":  []string{"10"},
		"offset": []string{"0"},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) == 0 {
		t.Fatal("expected stale view result")
	}
}

func TestValidateDesignDocumentInvalidKeyword(t *testing.T) {
	kdb, _ := testDB(t, "badsql")
	cur, err := kdb.GetDocument("badsql", &Document{ID: "_design/_views"}, true)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"_id":"_design/_views","_rev":` + itoa(cur.Version) + `,"views":{"bad":{"setup":["DROP TABLE IF EXISTS x"],"run":["SELECT 1"],"select":{"default":"SELECT 1"}}}}`)
	parsed, err := ParseDocument(body)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kdb.PutDocument("badsql", parsed)
	if err == nil {
		t.Fatal("expected invalid design document SQL to fail")
	}
}

func TestValidateDesignDocumentAttachAndCustomID(t *testing.T) {
	kdb, _ := testDB(t, "badsql2")

	cases := []struct {
		name string
		body string
	}{
		{
			name: "attach in run",
			body: `{"_id":"_design/custom","views":{"v":{"setup":["CREATE TABLE IF NOT EXISTS t (id)"],"run":["ATTACH DATABASE 'x.db' AS evil"],"select":{"default":"SELECT 1"}}}}`,
		},
		{
			name: "drop in select",
			body: `{"_id":"_design/custom2","views":{"v":{"setup":["CREATE TABLE IF NOT EXISTS t (id)"],"run":["SELECT 1"],"select":{"default":"DROP TABLE t"}}}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseDocument([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			_, err = kdb.PutDocument("badsql2", parsed)
			if err == nil {
				t.Fatal("expected design SQL validation to reject document")
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
