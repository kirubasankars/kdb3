package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

var kdb *KDBEngine

func NotOK(err error, w http.ResponseWriter) {
	var (
		statusCode = 0
		reason     = ""
	)

	switch {
	case err.Error() == "db_exists" || err.Error() == "invalid_db_name" || err.Error() == "mismatched_rev":
		statusCode = http.StatusPreconditionFailed
		reason = errorString(err)
	case err.Error() == "db_not_found" || err.Error() == "doc_not_found":
		statusCode = http.StatusNotFound
		reason = errorString(err)
	}
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
		reason = errorString(err)
	}
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error(), "reason": reason})
}

func main() {

	kdb, err := NewKDB()
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter().StrictSlash(true)

	router.PathPrefix("/_utils").Handler(http.StripPrefix("/_utils", http.FileServer(http.Dir("./share/www/"))))

	router.HandleFunc("/_all_dbs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			list, err := kdb.ListDataBases()
			if err != nil {
				NotOK(err, w)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			if len(list) > 0 {
				json.NewEncoder(w).Encode(list)
			} else {
				w.Write([]byte("[]"))
			}
		}
	}).Methods("GET")

	router.HandleFunc("/{db}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name := vars["db"]

		if r.Method == "GET" {
			if err := kdb.Open(name, false); err != nil {
				NotOK(err, w)
				return
			}
			stat, err := kdb.DBStat(name)
			if err != nil {
				NotOK(err, w)
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(stat)

		}
		if r.Method == "PUT" {
			if err := kdb.Open(name, true); err != nil {
				NotOK(err, w)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}
		if r.Method == "DELETE" {
			if err := kdb.Delete(name); err != nil {
				NotOK(err, w)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}
		if r.Method == "POST" {
			body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
			if err != nil {
				NotOK(err, w)
				return
			}

			input, err := ParseDocument(body)
			if err != nil {
				NotOK(err, w)
				return
			}

			doc, err := kdb.PutDocument(name, input)
			if err != nil {
				NotOK(err, w)
				return
			}

			output := `"_id":"` + doc.ID + `","_rev":"` + formatRev(doc.RevNumber, doc.RevID) + `"`
			if doc.Deleted {
				output += `,"_deleted":true`
			}

			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{` + output + `}`))
		}
	}).Methods("GET", "PUT", "POST", "DELETE")

	router.HandleFunc("/{db}/_compact", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name := vars["db"]
		if err := kdb.Vacuum(name); err != nil {
			NotOK(err, w)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}).Methods("POST")

	router.HandleFunc("/{db}/_all_docs", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name := vars["db"]
		rs, _ := kdb.SelectView(name, "_design/_views", "_all_docs", "default", r.Form, false)
		w.WriteHeader(http.StatusCreated)
		w.Write(rs)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	}).Methods("GET", "POST")

	router.HandleFunc("/{db}/_design/{ddocid}/{view}/{select}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name := vars["db"]
		ddocID := "_design/" + vars["ddocid"]
		view := vars["view"]
		selectName := vars["select"]
		r.ParseForm()
		fmt.Println(name, ddocID, view, selectName)
		rs, _ := kdb.SelectView(name, ddocID, view, selectName, r.Form, false)
		w.WriteHeader(http.StatusCreated)
		w.Write(rs)
		w.Header().Set("Content-Type", "application/json")

	}).Methods("GET", "POST")

	router.HandleFunc("/{db}/_design/{ddocid}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name := vars["db"]
		id := "_design/" + vars["ddocid"]
		if r.Method == "GET" {
			rev := r.FormValue("rev")
			var jsondoc string
			if rev != "" {
				jsondoc = `{"_id" : "` + id + `", "_rev": "` + rev + `"}`
			} else {
				jsondoc = `{"_id" : "` + id + `"}`
			}
			doc, err := ParseDocument([]byte(jsondoc))
			if err != nil {
				NotOK(err, w)
				return
			}

			rec, err := kdb.GetDocument(name, doc, true)
			if err != nil {
				NotOK(err, w)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			w.Write(rec.Data)
			if len(rec.Data) > 0 {
				w.Write([]byte("\n"))
			}
		}
	}).Methods("GET", "POST")

	router.HandleFunc("/{db}/{id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		db := vars["db"]
		id := vars["id"]

		if r.Method == "GET" {
			rev := r.FormValue("rev")
			var jsondoc string
			if rev != "" {
				jsondoc = `{"_id" : "` + id + `", "_rev": "` + rev + `"}`
			} else {
				jsondoc = `{"_id" : "` + id + `"}`
			}
			doc, err := ParseDocument([]byte(jsondoc))
			if err != nil {
				NotOK(err, w)
				return
			}

			rec, err := kdb.GetDocument(db, doc, true)
			if err != nil {
				NotOK(err, w)
				return
			}

			w.WriteHeader(http.StatusAccepted)
			w.Write(rec.Data)
			if len(rec.Data) > 0 {
				w.Write([]byte("\n"))
			}
		}
		if r.Method == "DELETE" {
			rev := r.FormValue("rev")
			if rev != "" {
				rev = r.Header.Get("If-Match")
			}
			jsondoc := `{"_id" : ` + id + `, "_rev": "` + rev + `"}`
			doc, err := ParseDocument([]byte(jsondoc))
			if err != nil {
				NotOK(err, w)
				return
			}

			_, err = kdb.DeleteDocument(db, doc)
			if err != nil {
				NotOK(err, w)
				return
			}

			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}

		if r.Method == "PUT" {
			body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
			if err != nil {
				NotOK(err, w)
				return
			}

			doc, err := ParseDocument(body)
			if err != nil {
				NotOK(err, w)
				return
			}
			doc.ID = id

			_, err = kdb.PutDocument(db, doc)
			if err != nil {
				NotOK(err, w)
				return
			}

			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}
	}).Methods("GET", "DELETE", "PUT")

	srv := &http.Server{
		Handler:      router,
		Addr:         "127.0.0.1:8001",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
