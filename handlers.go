package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	stat, err := kdb.DBStat(db)
	if err != nil {
		NotOK(err, w)
		return
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
	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, `{"ok":true}`)
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
	fmt.Fprint(w, `{"ok":true}`)
}

func (handler KDBHandler) DatabaseAllDocs(w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	vars := mux.Vars(r)
	db := vars["db"]

	r.ParseForm()

	selectName := "default"

	includeDocs, _ := strconv.ParseBool(r.FormValue("include_docs"))
	if includeDocs {
		selectName = "with_docs"
	}

	page := 1
	limit := 10

	if !r.Form.Has("limit") {
		r.Form.Set("limit", "10")
	} else {
		limit, _ = strconv.Atoi(r.FormValue("limit"))
		r.Form.Set("limit", strconv.Itoa(limit))
	}

	if !r.Form.Has("page") {
		r.Form.Set("page", "1")
	} else {
		page, _ = strconv.Atoi(r.FormValue("page"))
		r.Form.Set("page", strconv.Itoa(page))
	}

	r.Form.Add("offset", strconv.Itoa((page-1)*limit))

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

	since, _ := strconv.ParseInt(r.FormValue("since"), 10, 64)
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	descending, _ := strconv.ParseBool(r.FormValue("descending"))
	rs, err := kdb.Changes(db, since, limit, descending)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(rs)
}

func (handler KDBHandler) putDocument(db, docid string, w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	body, err := io.ReadAll(io.LimitReader(r.Body, 1048576))
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
		NotOK(ErrDocumentInvalidInput, w)
		return
	}
	outputDoc, err := kdb.PutDocument(db, inputDoc)
	if err != nil {
		NotOK(err, w)
		return
	}
	output := formatDocumentString(outputDoc.ID, outputDoc.Version, outputDoc.Deleted)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(output))
}

func (handler KDBHandler) getDocument(db, docid string, includeDocs bool, w http.ResponseWriter, r *http.Request) {
	kdb := handler.kdb
	rev := r.FormValue("rev")
	version := 0
	if rev != "" {
		var err error
		version, err = strconv.Atoi(rev)
		if err != nil {
			NotOK(err, w)
			return
		}
	}
	var inputDoc = &Document{ID: docid, Version: version}
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
	rev := r.FormValue("rev")
	version, err := strconv.Atoi(rev)
	if err != nil {
		NotOK(errors.New("rev should be int and can't be empty."), w)
		return
	}
	inputDoc := &Document{ID: docid, Version: version, Deleted: true}
	outputDoc, err := kdb.DeleteDocument(db, inputDoc)
	if err != nil {
		NotOK(err, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, formatDocumentString(outputDoc.ID, outputDoc.Version, outputDoc.Deleted))
}

func (handler KDBHandler) GetDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]

	handler.getDocument(db, docid, true, w, r)
}

func ValidateRequestJSON(w http.ResponseWriter, r *http.Request) error {
	if r.Header.Get("Content-Type") != "application/json" {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(fmt.Sprintf("Content-Type header [%s] is not supported", r.Header.Get("Content-Type"))))
		return ErrInternalError
	}
	return nil
}

func (handler KDBHandler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]

	kdb := handler.kdb
	_, err := kdb.DBStat(db)
	if err != nil {
		NotOK(err, w)
	}

	handler.deleteDocument(db, docid, w, r)
}

func (handler KDBHandler) PutDocument(w http.ResponseWriter, r *http.Request) {
	if err := ValidateRequestJSON(w, r); err != nil {
		return
	}
	vars := mux.Vars(r)
	db := vars["db"]
	docid := vars["docid"]
	handler.putDocument(db, docid, w, r)
}

func (handler KDBHandler) BulkPutDocuments(w http.ResponseWriter, r *http.Request) {
	if err := ValidateRequestJSON(w, r); err != nil {
		return
	}

	kdb := handler.kdb
	vars := mux.Vars(r)
	db := vars["db"]
	body, err := io.ReadAll(io.LimitReader(r.Body, 1048576))
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
	if err := ValidateRequestJSON(w, r); err != nil {
		return
	}

	kdb := handler.kdb
	vars := mux.Vars(r)
	db := vars["db"]
	body, err := io.ReadAll(io.LimitReader(r.Body, 1048576))
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
	if err := ValidateRequestJSON(w, r); err != nil {
		return
	}
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
	fromSeq, _ := strconv.Atoi(r.FormValue("from"))
	rs, _ := kdb.SQL(db, ddocID, view, int64(fromSeq))

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
	fmt.Fprint(w, `{"ok":true}`)
}

func NewKDBHandler(kdb *KDB) KDBHandler {
	handler := new(KDBHandler)
	handler.kdb = kdb
	handler.seq = NewSequenceUUIDGenarator()
	return *handler
}
