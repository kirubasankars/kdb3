package main

import "testing"

func TestFormatDocString1(t *testing.T) {
	o := formatDocString("1", 1, "6c62272e07bb014262b821756295c58d", false)
	expected := `{"_id":"1","_rev":"1-6c62272e07bb014262b821756295c58d"}`

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
	o := formatDocString("1", 2, "6c62272e07bb014262b821756295c58d", true)
	expected := `{"_id":"1","_rev":"2-6c62272e07bb014262b821756295c58d","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestOKTrue(t *testing.T) {
	o := OK(true, formatDocString("1", 2, "6c62272e07bb014262b821756295c58d", true))
	expected := `{"ok":true,"_id":"1","_rev":"2-6c62272e07bb014262b821756295c58d","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}

func TestOKFalse(t *testing.T) {
	o := OK(false, formatDocString("1", 2, "6c62272e07bb014262b821756295c58d", true))
	expected := `{"ok":false,"_id":"1","_rev":"2-6c62272e07bb014262b821756295c58d","_deleted":true}`

	if o != expected {
		t.Errorf("expected %s, got %s", expected, o)
	}
}
