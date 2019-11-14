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
	w.WriteHeader(http.StatusOK)
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
	w.WriteHeader(http.StatusOK)
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
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true}`)
}

func DatabaseAllDocs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	r.ParseForm()
	includeDocs, _ := strconv.ParseBool(r.FormValue("include_docs"))
	selectName := "default"
	if includeDocs {
		selectName = "with_docs"
	}
	rs, err := kdb.SelectView(db, "_design/_views", "_all_docs", selectName, r.Form, false)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(rs)
}

func DatabaseChanges(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	r.ParseForm()
	since := r.FormValue("since")
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	rs, err := kdb.Changes(db, since, limit)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
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
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true}`)
}

func putDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		NotOK(err, w)
		return
	}
	inputDoc, err := ParseDocument(body)
	if err != nil {
		NotOK(err, w)
		return
	}
	if docid != "" && docid != inputDoc.ID {
		NotOK(errors.New("mismatch_id"), w)
		return
	}
	outputDoc, err := kdb.PutDocument(db, inputDoc)
	if err != nil {
		NotOK(err, w)
		return
	}
	output := formatDocString(outputDoc.ID, outputDoc.Version, outputDoc.Deleted)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
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

	inputDoc, err := ParseDocument([]byte(jsondoc))
	if err != nil {
		NotOK(err, w)
		return
	}

	outputDoc, err := kdb.GetDocument(db, inputDoc, true)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outputDoc.Data)
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
	inputDoc, err := ParseDocument([]byte(jsondoc))
	if err != nil {
		NotOK(err, w)
		return
	}

	outputDoc, err := kdb.DeleteDocument(db, inputDoc)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, formatDocString(outputDoc.ID, outputDoc.Version, outputDoc.Deleted))
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
		NotOK(fmt.Errorf("%s:%w", err, ErrBadJSON), w)
		return
	}
	outputs, _ := fastjson.ParseBytes([]byte("[]"))
	for idx, item := range fValues.GetArray("_docs") {
		inputDoc, _ := ParseDocument([]byte(item.String()))
		var jsonb []byte
		outputDoc, err := kdb.PutDocument(db, inputDoc)
		if err != nil {
			code, reason := errorString(err)
			jsonb = []byte(fmt.Sprintf(`{"error":"%s","reason":"%s"}`, code, reason))
		} else {
			jsonb = []byte(formatDocString(outputDoc.ID, outputDoc.Version, outputDoc.Deleted))
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
		inputDoc, _ := ParseDocument([]byte(item.String()))
		var jsonb []byte
		outputDoc, err := kdb.GetDocument(db, inputDoc, true)
		if err != nil {
			code, reason := errorString(err)
			jsonb = []byte(fmt.Sprintf(`{"error":"%s","reason":"%s"}`, code, reason))
		} else {
			jsonb = outputDoc.Data
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
	w.WriteHeader(http.StatusOK)
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
	if selectName == "" {
		selectName = "default"
	}

	r.ParseForm()
	includeDocs, _ := strconv.ParseBool(r.FormValue("include_docs"))

	if includeDocs {
		selectName = selectName + "_with_docs"
	}
	r.ParseForm()
	stale, _ := strconv.ParseBool(r.FormValue("stale"))
	rs, err := kdb.SelectView(db, ddocID, view, selectName, r.Form, stale)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(rs)
}

func GetInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(kdb.Info())
}

var seq = NewSequenceUUIDGenarator()

func GetUUIDs(w http.ResponseWriter, r *http.Request) {
	c := r.FormValue("count")
	count, _ := strconv.Atoi(c)
	if count <= 0 {
		count = 1
	}
	var list []string
	for i := 0; i < count; i++ {
		list = append(list, seq.Next())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(list)
}
