package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

type KDBHandler struct {
	kdb *KDB
	seq *SequenceUUIDGenarator
}

func (handler KDBHandler) HeadDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]
	handler.getDocument(db, docid, false, w, r)
}

func (handler KDBHandler) GetDatabase(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) PutDatabase(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) DeleteDatabase(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) DatabaseAllDocs(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) DatabaseChanges(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) DatabaseCompact(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) putDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

	if inputDoc.ID == "" {
		inputDoc.ID = docid
	}
	if docid == "" {
		docid = inputDoc.ID
	}

	if docid != inputDoc.ID {
		NotOK(errors.New("mismatch_id"), w)
		return
	}
	outputDoc, err := kdb.PutDocument(db, inputDoc)
	if err != nil {
		NotOK(err, w)
		return
	}
	output := formatDocString(outputDoc.ID, outputDoc.Version, outputDoc.Hash, outputDoc.Deleted)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(output))
}

func (handler KDBHandler) getDocument(db, docid string, includeDocs bool, w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	ver := r.FormValue("version")
	var inputDoc = &Document{}
	if ver != "" {
		version, _ := strconv.Atoi(ver)
		inputDoc.ID = docid
		inputDoc.Version = version
	} else {
		inputDoc.ID = docid
	}

	outputDoc, err := kdb.GetDocument(db, inputDoc, includeDocs)
	if err != nil {
		NotOK(err, w)
		return
	}
	w.Header().Set("E-Tag", strconv.Itoa(outputDoc.Version))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if includeDocs {
		w.Write(outputDoc.Data)
	}
}

func (handler KDBHandler) deleteDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	ver := r.FormValue("version")

	if ver == "" {
		ver = r.Header.Get("If-Match")
	}
	if ver == "" {
		NotOK(errors.New("version_missing"), w)
		return
	}
	version, _ := strconv.Atoi(ver)
	inputDoc := &Document{ID: docid, Version: version, Deleted: true}
	outputDoc, err := kdb.DeleteDocument(db, inputDoc)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, formatDocString(outputDoc.ID, outputDoc.Version, outputDoc.Hash, outputDoc.Deleted))
}

func (handler KDBHandler) GetDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]

	handler.getDocument(db, docid, true, w, r)
}

func (handler KDBHandler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]
	handler.deleteDocument(db, docid, w, r)
}

func (handler KDBHandler) PutDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]
	handler.putDocument(db, docid, w, r)
}

func (handler KDBHandler) BulkPutDocuments(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	vars := mux.Vars(r)
	db := vars["db"]
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		NotOK(err, w)
		return
	}

	outputs, err := kdb.BulkDocuments(db, body)
	if err != nil {
		NotOK(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outputs)
}

func (handler KDBHandler) BulkGetDocuments(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	vars := mux.Vars(r)
	db := vars["db"]
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		NotOK(err, w)
		return
	}

	outputs, err := kdb.BulkGetDocuments(db, body)
	if err != nil {
		NotOK(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outputs)
}

func (handler KDBHandler) GetDDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := "_design/" + vars["docid"]
	handler.getDocument(db, docid, true, w, r)
}

func (handler KDBHandler) DeleteDDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := "_design/" + vars["docid"]
	handler.deleteDocument(db, docid, w, r)
}

func (handler KDBHandler) PutDDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := "_design/" + vars["docid"]
	handler.putDocument(db, docid, w, r)
}

func (handler KDBHandler) AllDatabases(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	list, err := kdb.ListDatabases()
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

func (handler KDBHandler) SelectView(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func (handler KDBHandler) SQL(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	vars := mux.Vars(r)

	db := vars["db"]
	ddocID := "_design/" + vars["docid"]
	view := vars["view"]

	r.ParseForm()
	fromSeqID := r.FormValue("from")
	rs, _ := kdb.SQL(db, ddocID, view, fromSeqID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(rs)
}

func (handler KDBHandler) GetInfo(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(kdb.Info())
}

func (handler KDBHandler) GetUUIDs(w http.ResponseWriter, r *http.Request) {
	c := r.FormValue("count")
	count, _ := strconv.Atoi(c)
	if count <= 0 {
		count = 1
	}
	var list []string
	for i := 0; i < count; i++ {
		list = append(list, handler.seq.Next())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(list)
}

func (handler KDBHandler) Vacuum(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
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

func NewKDBHandler(kdb *KDB) KDBHandler {
	handler := new(KDBHandler)
	handler.kdb = kdb
	handler.seq = NewSequenceUUIDGenarator()
	return *handler
}
