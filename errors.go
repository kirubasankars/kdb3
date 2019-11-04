package main

var (
	DB_EXISTS         = "db_exists"
	BAD_JSON          = "bad_json"
	DB_NOT_FOUND      = "db_not_found"
	INVALID_DB_NAME   = "invalid_db_name"
	DOC_CONFLICT      = "doc_conflict"
	DOC_NOT_FOUND     = "doc_not_found"
	VIEW_NOT_FOUND    = "view_not_found"
	VIEW_RESULT_ERROR = "view_result_error"

	MSG_INTERAL_ERROR     = "internal error"
	MSG_DB_EXISTS         = "database already exists"
	MSG_BAD_JSON          = "invalid json format"
	MSG_DB_NOT_FOUND      = "database is not found"
	MSG_INVALID_DB_NAME   = "invalid json format"
	MSG_DOC_CONFLICT      = "invalid database name"
	MSG_DOC_NOT_FOUND     = "document not found"
	MSG_VIEW_NOT_FOUND    = "view not found"
	MSG_VIEW_RESULT_ERROR = "view expect 1 column"
)

type Error struct {
	err string
	e   error
}

func errorString(err error) string {
	switch errr := err.Error(); errr {
	case DB_EXISTS:
		return MSG_DB_EXISTS
	case BAD_JSON:
		return MSG_BAD_JSON
	case DB_NOT_FOUND:
		return MSG_DB_NOT_FOUND
	case INVALID_DB_NAME:
		return MSG_INVALID_DB_NAME
	case DOC_CONFLICT:
		return MSG_DOC_CONFLICT
	case DOC_NOT_FOUND:
		return MSG_DOC_NOT_FOUND
	case VIEW_NOT_FOUND:
		return MSG_VIEW_NOT_FOUND
	case VIEW_RESULT_ERROR:
		return MSG_VIEW_RESULT_ERROR
	default:
		return MSG_INTERAL_ERROR
	}
}
