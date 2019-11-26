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
	_, err := ParseDocument([]byte(`{"_rev":"1-hash"}`))
	if err == nil {
		t.Errorf("expected to fail with %s", ErrDocInvalidID)
	}

	if err != nil && !errors.Is(err, ErrDocInvalidInput) {
		t.Errorf("expected to fail with %s", ErrDocInvalidID)
	}
}

func TestParseDocumentGoodDoc(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_rev":"1-hash", "_id":1, "test":"1"}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}

	if doc.ID != "1" || doc.Version != 1 || doc.Deleted || string(doc.Data) != `{"test":"1"}` {
		t.Errorf("failed to parse doc")
	}
}

func TestParseDocumentGoodDocDeleted(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_rev":"1-hash", "_id":1, "_deleted":true}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}

	if doc.ID != "1" || doc.Version != 1 || !doc.Deleted {
		t.Errorf("failed to parse doc")
	}
}

func TestParseDocumentKind(t *testing.T) {
	doc, err := ParseDocument([]byte(`{"_rev":"1-hash", "_id":1, "_kind":1 ,"test":1, "_deleted":true}`))
	if err != nil {
		t.Errorf("unexpected to fail with %s", err.Error())
	}

	if doc.ID != "1" || doc.Version != 1 || !doc.Deleted || doc.Kind != "1" {
		t.Errorf("failed to parse doc")
	}

	doc.CalculateNextVersion()
	if doc.Version != 2 || string(doc.Data) != `{"_id":"1","_rev":"2-44e7a186db2c24f55e9bf22e427df2ad","_kind":"1","test":1}` {
		t.Errorf("failed to parse doc")
	}
}

func TestParseDocumentObject(t *testing.T) {
	_, err := ParseDocument([]byte(`[]`))
	if err == nil {
		t.Errorf("expected to fail")
	}
	if !errors.Is(err, ErrDocInvalidInput) {
		t.Errorf("expected to fail with %s, got %s", err.Error(), ErrDocInvalidInput)
	}
}
