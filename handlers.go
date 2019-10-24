package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/valyala/fastjson"

	"github.com/gorilla/mux"
)

func GetDatabase(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	if err := kdb.Open(db, false); err != nil {
		NotOK(err, w)
		return
	}
	stat, err := kdb.DBStat(db)
	if err != nil {
		NotOK(err, w)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(stat)
}

func PutDatabase(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	if err := kdb.Open(db, true); err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"ok":true}`)

}

func DeleteDatabase(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	if err := kdb.Delete(db); err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"ok":true}`)
}

func DatabaseAllDocs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	rs, err := kdb.SelectView(db, "_design/_views", "_all_docs", "default", r.Form, false)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(rs)
}

func DatabaseChanges(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	rs, err := kdb.Changes(db)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(rs)
}

func DatabaseCompact(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	err := kdb.Vacuum(db)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"ok":true}`)
}

func putDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
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
	if docid != "" {
		input.ID = docid
	}
	doc, err := kdb.PutDocument(db, input)
	if err != nil {
		NotOK(err, w)
		return
	}
	output := formatDocString(doc.ID, doc.Version, doc.Deleted)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(output))
}

func getDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
	ver := r.FormValue("version")

	var jsondoc string
	if ver != "" {
		version, _ := strconv.Atoi(ver)
		jsondoc = formatDocString(docid, version, false)
	} else {
		jsondoc = formatDocString(docid, 0, false)
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(rec.Data)
}

func deleteDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
	ver := r.FormValue("version")

	if ver == "" {
		ver = r.Header.Get("If-Match")
	}
	if ver == "" {
		NotOK(errors.New("version_missing"), w)
		return
	}
	version, _ := strconv.Atoi(ver)
	jsondoc := formatDocString(docid, version, true)
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"ok":true}`)
}

func GetDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]

	getDocument(db, docid, w, r)
}

func DeleteDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]
	deleteDocument(db, docid, w, r)
}

func PutDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]
	putDocument(db, docid, w, r)
}

func BulkPutDocuments(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		NotOK(err, w)
		return
	}
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		NotOK(errors.New("bad_json"), w)
		return
	}
	outputs, _ := fastjson.ParseBytes([]byte("[]"))
	for idx, item := range fValues.GetArray("_docs") {
		idoc, _ := ParseDocument([]byte(item.String()))
		var jsonb []byte
		odoc, err := kdb.PutDocument(db, idoc)
		if err != nil {
			jsonb = []byte(fmt.Sprintf(`{"error":"%s","reason":"%s"}`, err.Error(), errorString(err)))
		} else {
			jsonb = []byte(formatDocString(odoc.ID, odoc.Version, odoc.Deleted))
		}
		v := fastjson.MustParse(string(jsonb))
		outputs.SetArrayItem(idx, v)
	}
	w.Write([]byte(outputs.String()))
}

func BulkGetDocuments(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		NotOK(err, w)
		return
	}
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		NotOK(errors.New("bad_json"), w)
		return
	}
	outputs, _ := fastjson.ParseBytes([]byte("[]"))
	for idx, item := range fValues.GetArray("_docs") {
		idoc, _ := ParseDocument([]byte(item.String()))
		var jsonb []byte
		odoc, err := kdb.GetDocument(db, idoc, true)
		if err != nil {
			jsonb = []byte(fmt.Sprintf(`{"error":"%s","reason":"%s"}`, err.Error(), errorString(err)))
		} else {
			jsonb = odoc.Data
		}
		v := fastjson.MustParse(string(jsonb))
		outputs.SetArrayItem(idx, v)
	}
	w.Write([]byte(`{"results":` + outputs.String() + `}`))
}

func GetDDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := "_design/" + vars["docid"]
	getDocument(db, docid, w, r)
}

func DeleteDDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := "_design/" + vars["docid"]
	deleteDocument(db, docid, w, r)
}

func PutDDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := "_design/" + vars["docid"]
	putDocument(db, docid, w, r)
}

func AllDatabases(w http.ResponseWriter, r *http.Request) {
	list, err := kdb.ListDataBases()
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if len(list) > 0 {
		json.NewEncoder(w).Encode(list)
	} else {
		w.Write([]byte("[]"))
	}
}

func SelectView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	db := vars["db"]
	ddocID := "_design/" + vars["docid"]
	view := vars["view"]
	selectName := vars["select"]
	r.ParseForm()
	stale, _ := strconv.ParseBool(r.FormValue("stale"))
	rs, err := kdb.SelectView(db, ddocID, view, selectName, r.Form, stale)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(rs)
}

func Info(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(kdb.Info())
}
