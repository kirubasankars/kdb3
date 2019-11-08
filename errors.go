package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

var (
	ErrBadJSON         = errors.New("bad_json")
	ErrDBExists        = errors.New("db_exists")
	ErrDBNotFound      = errors.New("db_not_found")
	ErrDBInvalidName   = errors.New("invalid_db_name")
	ErrDocInvalidID    = errors.New("invalid_doc_id")
	ErrDocConflict     = errors.New("doc_conflict")
	ErrDocNotFound     = errors.New("doc_not_found")
	ErrViewNotFound    = errors.New("view_not_found")
	ErrViewResult      = errors.New("view_result_error")
	ErrDocInvalidInput = errors.New("doc_invalid_input")
	ErrInvalidSQLStmt  = errors.New("invalid_sql_stmt")
	ErrInternalError   = errors.New("internal_error")

	MSG_INTERAL_ERROR     = "internal error"
	MSG_DB_EXISTS         = "database already exists"
	MSG_BAD_JSON          = "invalid json format"
	MSG_DB_NOT_FOUND      = "database not found"
	MSG_INVALID_DB_NAME   = "invalid db name"
	MSG_INVALID_DOC_ID    = "invalid doc id"
	MSG_DOC_CONFLICT      = "document conflict"
	MSG_DOC_NOT_FOUND     = "document not found"
	MSG_VIEW_NOT_FOUND    = "view not found"
	MSG_VIEW_RESULT_ERROR = "view expect 1 column"
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
	case errors.Is(err, ErrDBExists):
		return err.Error(), MSG_DB_EXISTS
	case errors.Is(err, ErrBadJSON):
		return ErrBadJSON.Error(), getErrorDescription(err)
	case errors.Is(err, ErrDBNotFound):
		return ErrDBNotFound.Error(), MSG_DB_NOT_FOUND
	case errors.Is(err, ErrDBInvalidName):
		return ErrDBInvalidName.Error(), getErrorDescription(err)
	case errors.Is(err, ErrDocInvalidID):
		return ErrDocInvalidID.Error(), getErrorDescription(err)
	case errors.Is(err, ErrDocConflict):
		return ErrDocConflict.Error(), MSG_DOC_CONFLICT
	case errors.Is(err, ErrDocNotFound):
		return ErrDocNotFound.Error(), MSG_DOC_NOT_FOUND
	case errors.Is(err, ErrViewNotFound):
		return ErrViewNotFound.Error(), MSG_VIEW_NOT_FOUND
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
	case errors.Is(err, ErrDBExists) || errors.Is(err, ErrDBInvalidName) || errors.Is(err, ErrInvalidSQLStmt):
		statusCode = http.StatusPreconditionFailed
	case errors.Is(err, ErrDocConflict):
		statusCode = http.StatusConflict
	case errors.Is(err, ErrDBNotFound) || errors.Is(err, ErrDocNotFound) || errors.Is(err, ErrViewNotFound):
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
