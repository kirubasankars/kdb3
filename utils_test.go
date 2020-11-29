package main

import "testing"

func TestFormatDocString1(t *testing.T) {
	o := formatDocString("1", 1, "", false)
	expected := `{"_id":"1","_version":1}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestFormatDocString2(t *testing.T) {
	o := formatDocString("1", 0, "", false)
	expected := `{"_id":"1"}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestFormatDocString3(t *testing.T) {
	o := formatDocString("1", 0, "", true)
	expected := `{"_id":"1","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestFormatDocString4(t *testing.T) {
	o := formatDocString("1", 2, "", true)
	expected := `{"_id":"1","_version":2,"_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestOKTrue(t *testing.T) {
	o := OK(true, formatDocString("1", 2, "", true))
	expected := `{"ok":true,"_id":"1","_version":2,"_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestOKFalse(t *testing.T) {
	o := OK(false, formatDocString("1", 2, "", true))
	expected := `{"ok":false,"_id":"1","_version":2,"_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}
