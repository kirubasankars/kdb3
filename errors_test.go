package main

import (
	"testing"
)

func TestErrorDB_EXISTS(t *testing.T) {
	code, reason := errorString(ErrDatabaseExists)
	if code != ErrDatabaseExists.Error() || reason != MessageDatabaseExists {
		t.Errorf("expected %s, got %s", ErrDatabaseExists, code)
	}
}

func TestErrorBAD_JSON(t *testing.T) {
	code, reason := errorString(ErrBadJSON)
	if code != ErrBadJSON.Error() || reason != ErrBadJSON.Error() {
		t.Errorf("expected %s, got %s", ErrBadJSON, code)
	}
}

func TestErrorDB_NOT_FOUND(t *testing.T) {
	code, reason := errorString(ErrDatabaseNotFound)
	if code != ErrDatabaseNotFound.Error() || reason != MessageDatabaseNotFound {
		t.Errorf("expected %s, got %s", ErrDatabaseNotFound, code)
	}
}

func TestErrorINVALID_DB_NAME(t *testing.T) {
	code, reason := errorString(ErrDatabaseInvalidName)
	if code != ErrDatabaseInvalidName.Error() || reason != ErrDatabaseInvalidName.Error() {
		t.Errorf("expected %s, got %s", ErrDatabaseInvalidName, code)
	}
}

func TestErrorINVALID_DOC_ID(t *testing.T) {
	code, reason := errorString(ErrDatabaseInvalidName)
	if code != ErrDatabaseInvalidName.Error() || reason != ErrDatabaseInvalidName.Error() {
		t.Errorf("expected %s, got %s", ErrDatabaseInvalidName, code)
	}
}

func TestErrorDOC_CONFLICT(t *testing.T) {
	code, reason := errorString(ErrDocumentConflict)
	if code != ErrDocumentConflict.Error() || reason != MessageDocumentConflict {
		t.Errorf("expected %s, got %s", ErrDocumentConflict, code)
	}
}

func TestErrorDOC_NOT_FOUND(t *testing.T) {
	code, reason := errorString(ErrDocumentNotFound)
	if code != ErrDocumentNotFound.Error() || reason != MessageDocumentNotFound {
		t.Errorf("expected %s, got %s", ErrDocumentNotFound, code)
	}
}

func TestErrorVIEW_NOT_FOUND(t *testing.T) {
	code, reason := errorString(ErrViewNotFound)
	if code != ErrViewNotFound.Error() || reason != MessageViewNotFound {
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
