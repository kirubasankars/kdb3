package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

var (
	// ErrBadJSON bad_json
	ErrBadJSON = errors.New("bad_json")
	// ErrDatabaseExists db_exists
	ErrDatabaseExists = errors.New("db_exists")
	// ErrDatabaseNotFound db_not_found
	ErrDatabaseNotFound = errors.New("db_not_found")
	// ErrDatabaseInvalidName invalid_db_name
	ErrDatabaseInvalidName = errors.New("invalid_db_name")
	// ErrDocumentInvalidID invalid_doc_id
	ErrDocumentInvalidID = errors.New("invalid_doc_id")
	// ErrDocumentConflict doc_conflict
	ErrDocumentConflict = errors.New("doc_conflict")
	// ErrDocumentNotFound doc_not_found
	ErrDocumentNotFound = errors.New("doc_not_found")
	// ErrViewNotFound view_not_found
	ErrViewNotFound = errors.New("view_not_found")
	// ErrViewResult view_result_error
	ErrViewResult = errors.New("view_result_error")
	// ErrDocumentInvalidInput doc_invalid_input
	ErrDocumentInvalidInput = errors.New("doc_invalid_input")
	// ErrInvalidSQLStmt invalid_sql_stmt
	ErrInvalidSQLStmt = errors.New("invalid_sql_stmt")
	// ErrInternalError internal_error
	ErrInternalError = errors.New("internal_error")

	// MessageBadJSON error message for ErrBadJSON
	MessageBadJSON = "invalid json format"
	// MessageDatabaseExists error message for ErrDBExists
	MessageDatabaseExists = "database already exists"
	// MessageDatabaseNotFound error message for ErrDBNotFound
	MessageDatabaseNotFound = "database not found"
	// MessageDatabaseInvalidName error message for ErrDBInvalidName
	MessageDatabaseInvalidName = "invalid db name"
	// MessageDocumentInvalidID error message for ErrDocInvalidID
	MessageDocumentInvalidID = "invalid doc id"
	// MessageDocumentConflict error message for ErrDocConflict
	MessageDocumentConflict = "document conflict"
	// MessageDocumentNotFound error message for ErrDocNotFound
	MessageDocumentNotFound = "document not found"
	// MessageViewNotFound error message for MessageViewNotFound
	MessageViewNotFound = "view not found"
	// MessageInternalError error message for ErrInternalError
	MessageInternalError = "internal error"
)

func getErrorDescription(err error) string {
	e := errors.Unwrap(err)
	if e == nil {
		return err.Error()
	}
	return strings.Trim(strings.TrimRight(strings.ReplaceAll(err.Error(), e.Error(), ""), " "), ":")
}

func errorString(err error) (string, string) {
	switch {
	case errors.Is(err, ErrDatabaseExists):
		return err.Error(), MessageDatabaseExists
	case errors.Is(err, ErrBadJSON):
		return ErrBadJSON.Error(), getErrorDescription(err)
	case errors.Is(err, ErrDatabaseNotFound):
		return ErrDatabaseNotFound.Error(), MessageDatabaseNotFound
	case errors.Is(err, ErrDatabaseInvalidName):
		return ErrDatabaseInvalidName.Error(), getErrorDescription(err)
	case errors.Is(err, ErrDocumentInvalidID):
		return ErrDocumentInvalidID.Error(), getErrorDescription(err)
	case errors.Is(err, ErrDocumentConflict):
		return ErrDocumentConflict.Error(), MessageDocumentConflict
	case errors.Is(err, ErrDocumentNotFound):
		return ErrDocumentNotFound.Error(), MessageDocumentNotFound
	case errors.Is(err, ErrViewNotFound):
		return ErrViewNotFound.Error(), MessageViewNotFound
	case errors.Is(err, ErrViewResult):
		return ErrViewResult.Error(), getErrorDescription(err)
	case errors.Is(err, ErrInvalidSQLStmt):
		return ErrInvalidSQLStmt.Error(), getErrorDescription(err)
	default:
		return ErrInternalError.Error(), getErrorDescription(err)
	}
}

func NotOK(err error, w http.ResponseWriter) {
	var (
		statusCode = 0
		code       = ""
		reason     = ""
	)

	switch {
	case errors.Is(err, ErrDatabaseExists) || errors.Is(err, ErrDatabaseInvalidName) || errors.Is(err, ErrInvalidSQLStmt):
		statusCode = http.StatusPreconditionFailed
	case errors.Is(err, ErrDocumentConflict):
		statusCode = http.StatusConflict
	case errors.Is(err, ErrDatabaseNotFound) || errors.Is(err, ErrDocumentNotFound) || errors.Is(err, ErrViewNotFound):
		statusCode = http.StatusNotFound
	case errors.Is(err, ErrBadJSON):
		statusCode = http.StatusBadRequest
	}

	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}

	code, reason = errorString(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": code, "reason": reason})
}
