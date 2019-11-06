package main

import "testing"

func TestParseDocumentBadJSON(t *testing.T) {
	_, err := ParseDocument([]byte(`{"_name"}`))
	if err == nil {
		t.Errorf("expected to fail with %s", BAD_JSON)
	}

	if err == nil && err.Error() != BAD_JSON {
		t.Errorf("expected to fail with %s", BAD_JSON)
	}
}

func TestParseDocumentWithVerisonandNoID(t *testing.T) {
	_, err := ParseDocument([]byte(`{"_version":1}`))
	if err == nil {
		t.Errorf("expected to fail with %s", INVALID_DOC_ID)
	}

	if err == nil && err.Error() != INVALID_DOC_ID {
		t.Errorf("expected to fail with %s", INVALID_DOC_ID)
	}
}

func TestParseDocumentGoodDoc(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_version":1, "_id":1, "test":"1"}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}

	if doc.ID != "1" || doc.Version != 1 || doc.Deleted || string(doc.Data) != `{"test":"1"}` {
		t.Errorf("failed to parse doc")
	}
}

func TestParseDocumentGoodDocDeleted(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_version":1, "_id":1, "_deleted":true}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}

	if doc.ID != "1" || doc.Version != 1 || !doc.Deleted {
		t.Errorf("failed to parse doc")
	}
}
