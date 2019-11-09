package main

import (
	"testing"
)

func TestErrorDB_EXISTS(t *testing.T) {
	code, reason := errorString(ErrDBExists)
	if code != ErrDBExists.Error() || reason != MsgDBExists {
		t.Errorf("expected %s, got %s", ErrDBExists, code)
	}
}

func TestErrorBAD_JSON(t *testing.T) {
	code, reason := errorString(ErrBadJSON)
	if code != ErrBadJSON.Error() || reason != ErrBadJSON.Error() {
		t.Errorf("expected %s, got %s", ErrBadJSON, code)
	}
}

func TestErrorDB_NOT_FOUND(t *testing.T) {
	code, reason := errorString(ErrDBNotFound)
	if code != ErrDBNotFound.Error() || reason != MsgDBNotFound {
		t.Errorf("expected %s, got %s", ErrDBNotFound, code)
	}
}

func TestErrorINVALID_DB_NAME(t *testing.T) {
	code, reason := errorString(ErrDBInvalidName)
	if code != ErrDBInvalidName.Error() || reason != ErrDBInvalidName.Error() {
		t.Errorf("expected %s, got %s", ErrDBInvalidName, code)
	}
}

func TestErrorINVALID_DOC_ID(t *testing.T) {
	code, reason := errorString(ErrDBInvalidName)
	if code != ErrDBInvalidName.Error() || reason != ErrDBInvalidName.Error() {
		t.Errorf("expected %s, got %s", ErrDBInvalidName, code)
	}
}

func TestErrorDOC_CONFLICT(t *testing.T) {
	code, reason := errorString(ErrDocConflict)
	if code != ErrDocConflict.Error() || reason != MsgDocConflict {
		t.Errorf("expected %s, got %s", ErrDocConflict, code)
	}
}

func TestErrorDOC_NOT_FOUND(t *testing.T) {
	code, reason := errorString(ErrDocNotFound)
	if code != ErrDocNotFound.Error() || reason != MsgDocNotFound {
		t.Errorf("expected %s, got %s", ErrDocNotFound, code)
	}
}

func TestErrorVIEW_NOT_FOUND(t *testing.T) {
	code, reason := errorString(ErrViewNotFound)
	if code != ErrViewNotFound.Error() || reason != MsgViewNotFound {
		t.Errorf("expected %s, got %s", ErrViewNotFound, code)
	}
}

func TestErrorVIEW_RESULT_ERROR(t *testing.T) {
	code, reason := errorString(ErrViewResult)
	if code != ErrViewResult.Error() || reason != ErrViewResult.Error() {
		t.Errorf("expected %s, got %s", ErrViewResult, code)
	}
}

func TestErrorINTERAL_ERROR(t *testing.T) {
	code, reason := errorString(ErrInternalError)
	if code != ErrInternalError.Error() || reason != ErrInternalError.Error() {
		t.Errorf("expected %s, got %s", ErrInternalError, code)
	}
}
