package main

func errorString(err error) string {
	var errString = "internal error"

	switch errr := err.Error(); errr {
	case "db_exists":
		errString = "database already exists"
	case "db_not_found":
		errString = "database is not found"
	case "invalid_db_name":
		errString = "invalid database name"
	case "doc_conflict":
		errString = "document update conflict"
	case "doc_not_found":
		errString = "document not found"
	case "view_not_found":
		errString = "view not found"
	}

	return errString
}
