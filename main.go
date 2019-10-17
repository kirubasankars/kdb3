package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

var kdb, _ = NewKDB()

func NotOK(err error, w http.ResponseWriter) {
	var (
		statusCode = 0
		reason     = ""
	)

	switch {
	case err.Error() == "db_exists" || err.Error() == "invalid_db_name":
		statusCode = http.StatusPreconditionFailed
		reason = errorString(err)
	case err.Error() == "doc_conflict":
		statusCode = http.StatusConflict
		reason = errorString(err)
	case err.Error() == "db_not_found" || err.Error() == "doc_not_found" || err.Error() == "view_not_found":
		statusCode = http.StatusNotFound
		reason = errorString(err)
	}
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
		reason = errorString(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error(), "reason": reason})
}

func main() {

	router := NewRouter()

	srv := &http.Server{
		Handler:      router,
		Addr:         "127.0.0.1:8001",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
