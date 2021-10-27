package main

import "testing"

func TestFormatDocString1(t *testing.T) {
	o := formatDocumentString("1", 1, "4dd69f96755b8be0c5d6a4c4d875e705", false)
	expected := `{"_id":"1","_rev":"1-4dd69f96755b8be0c5d6a4c4d875e705"}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestFormatDocString4(t *testing.T) {
	o := formatDocumentString("1", 2, "4dd69f96755b8be0c5d6a4c4d875e705", true)
	expected := `{"_id":"1","_rev":"2-4dd69f96755b8be0c5d6a4c4d875e705","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestOKTrue(t *testing.T) {
	o := OK(true, formatDocumentString("1", 2, "4dd69f96755b8be0c5d6a4c4d875e705", true))
	expected := `{"ok":true,"_id":"1","_rev":"2-4dd69f96755b8be0c5d6a4c4d875e705","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestOKFalse(t *testing.T) {
	o := OK(false, formatDocumentString("1", 2, "4dd69f96755b8be0c5d6a4c4d875e705", true))
	expected := `{"ok":false,"_id":"1","_rev":"2-4dd69f96755b8be0c5d6a4c4d875e705","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}
