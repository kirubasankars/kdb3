package main

import (
	"errors"
	"testing"
)

func TestParseDocumentBadJSON(t *testing.T) {
	_, err := ParseDocument([]byte(`{"_name"}`))
	if err == nil {
		t.Errorf("expected to fail with %s", ErrBadJSON)
	}
	if err != nil && !errors.Is(err, ErrBadJSON) {
		t.Errorf("expected to fail with %s", ErrBadJSON)
	}
}

func TestParseDocumentWithVerisonandNoID(t *testing.T) {
	_, err := ParseDocument([]byte(`{"_rev":"1-6c62272e07bb014262b821756295c58d"}`))
	if err == nil {
		t.Errorf("expected to fail with %s", ErrDocInvalidID)
	}

	if err != nil && !errors.Is(err, ErrDocInvalidInput) {
		t.Errorf("expected to fail with %s", ErrDocInvalidID)
	}
}

func TestParseDocumentGoodDoc(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_rev":"1-6c62272e07bb014262b821756295c58d", "_id":1, "test":"1"}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}

	if doc.ID != "1" || doc.Version != 1 || doc.Deleted || string(doc.Data) != `{"test":"1"}` {
		t.Errorf("failed to parse doc")
	}
}

func TestParseDocumentGoodDocDeleted(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_rev":"1-6c62272e07bb014262b821756295c58d", "_id":1, "_deleted":true}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}
	if doc.ID != "1" || doc.Version != 1 || !doc.Deleted {
		t.Errorf("failed to parse doc")
	}
}
