package main

import (
	"errors"
	"testing"
)

func TestError(t *testing.T) {
	msg := errorString(errors.New(DB_EXISTS))
	if msg != MSG_DB_EXISTS {
		t.Errorf("expected %s, got %s", msg, MSG_DB_EXISTS)
	}
}
