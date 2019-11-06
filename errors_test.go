package main

import (
	"errors"
	"testing"
)

func TestErrorDB_EXISTS(t *testing.T) {
	msg := errorString(errors.New(DB_EXISTS))
	if msg != MSG_DB_EXISTS {
		t.Errorf("expected %s, got %s", msg, MSG_DB_EXISTS)
	}
}

func TestErrorBAD_JSON(t *testing.T) {
	msg := errorString(errors.New(BAD_JSON))
	if msg != MSG_BAD_JSON {
		t.Errorf("expected %s, got %s", msg, MSG_BAD_JSON)
	}
}

func TestErrorDB_NOT_FOUND(t *testing.T) {
	msg := errorString(errors.New(DB_NOT_FOUND))
	if msg != MSG_DB_NOT_FOUND {
		t.Errorf("expected %s, got %s", msg, MSG_DB_NOT_FOUND)
	}
}

func TestErrorINVALID_DB_NAME(t *testing.T) {
	msg := errorString(errors.New(INVALID_DB_NAME))
	if msg != MSG_INVALID_DB_NAME {
		t.Errorf("expected %s, got %s", msg, MSG_INVALID_DB_NAME)
	}
}

func TestErrorINVALID_DOC_ID(t *testing.T) {
	msg := errorString(errors.New(INVALID_DOC_ID))
	if msg != MSG_INVALID_DOC_ID {
		t.Errorf("expected %s, got %s", msg, MSG_INVALID_DOC_ID)
	}
}

func TestErrorDOC_CONFLICT(t *testing.T) {
	msg := errorString(errors.New(DOC_CONFLICT))
	if msg != MSG_DOC_CONFLICT {
		t.Errorf("expected %s, got %s", msg, MSG_DOC_CONFLICT)
	}
}

func TestErrorDOC_NOT_FOUND(t *testing.T) {
	msg := errorString(errors.New(DOC_NOT_FOUND))
	if msg != MSG_DOC_NOT_FOUND {
		t.Errorf("expected %s, got %s", msg, MSG_DOC_NOT_FOUND)
	}
}

func TestErrorVIEW_NOT_FOUND(t *testing.T) {
	msg := errorString(errors.New(VIEW_NOT_FOUND))
	if msg != MSG_VIEW_NOT_FOUND {
		t.Errorf("expected %s, got %s", msg, MSG_VIEW_NOT_FOUND)
	}
}

func TestErrorVIEW_RESULT_ERROR(t *testing.T) {
	msg := errorString(errors.New(VIEW_RESULT_ERROR))
	if msg != MSG_VIEW_RESULT_ERROR {
		t.Errorf("expected %s, got %s", msg, MSG_VIEW_RESULT_ERROR)
	}
}

func TestErrorINTERAL_ERROR(t *testing.T) {
	msg := errorString(errors.New(INTERAL_ERROR))
	if msg != MSG_INTERAL_ERROR {
		t.Errorf("expected %s, got %s", msg, MSG_INTERAL_ERROR)
	}
}
