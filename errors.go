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
	case "mismatched_rev":
		errString = "mismatched rev number"
	case "doc_not_found":
		errString = "document not found"
	}

	return errString
}
