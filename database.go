package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Database struct {
	name            string
	updateSeqNumber int
	updateSeqID     string

	dbPath          string
	viewPath        string
	views           map[string]*View
	designDocuments map[string]*DesignDocument
	db              *sql.DB

	mux    sync.Mutex
	seqGen *SequenceGenarator
}

func NewDatabase(dbPath, viewPath string) *Database {
	db := &Database{}
	db.dbPath = dbPath
	db.viewPath = viewPath
	return db
}

func (database *Database) Open(name string, createIfNotExists bool) error {
	if !validatename(name) {
		return errors.New("invalid_db_name")
	}

	database.name = name
	database.views = make(map[string]*View)
	database.designDocuments = make(map[string]*DesignDocument)

	path := filepath.Join(database.dbPath, name+dbExt)
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if !createIfNotExists {
			return errors.New("db_not_found")
		}
	} else {
		if createIfNotExists {
			return errors.New("db_exists")
		}
	}

	db, err := sql.Open("sqlite3", path+"?_journal=WAL")
	if err != nil {
		return err
	}

	buildSQL := `
		CREATE TABLE IF NOT EXISTS documents (
			doc_id 		TEXT,
			data 		TEXT,
			PRIMARY KEY (doc_id)
		) WITHOUT ROWID;

		CREATE TABLE IF NOT EXISTS changes (
			seq_number  INTEGER,
			seq_id 		TEXT, 
			doc_id 		TEXT, 
			rev_number  INTEGER, 
			rev_id 		TEXT, 
			PRIMARY KEY (seq_number, seq_id)
		) WITHOUT ROWID;

		CREATE TABLE IF NOT EXISTS revisions (
			doc_id 		TEXT,
			rev_number  INTEGER,
			rev_id 		TEXT,
			deleted 	BOOL,
			PRIMARY KEY (doc_id, rev_number DESC, rev_id)
		) WITHOUT ROWID;`

	if _, err = db.Exec(buildSQL); err != nil {
		return err
	}

	database.db = db

	database.updateSeqNumber, database.updateSeqID = database.GetLastUpdateSequence()
	database.seqGen = NewSequenceGenarator(138, database.updateSeqNumber, database.updateSeqID)

	if createIfNotExists {
		ddoc := &DesignDocument{}
		ddoc.ID = "_design/_views"
		ddoc.Views = make(map[string]*DesignDocumentView)
		ddv := &DesignDocumentView{}
		ddv.Setup = append(ddv.Setup, "CREATE TABLE IF NOT EXISTS all_docs (key TEXT PRIMARY KEY, value TEXT, doc_id TEXT)")
		ddv.Delete = append(ddv.Delete, "DELETE FROM all_docs WHERE doc_id IN (SELECT DISTINCT doc_id FROM docsdb.changes WHERE seq_number > ${begin_seq_number} AND seq_id > ${begin_seq_id} AND seq_number <= ${end_seq_number} AND seq_id <= ${end_seq_id})")
		ddv.Update = append(ddv.Update, "INSERT INTO all_docs (key, value, doc_id) SELECT d.doc_id, JSON_OBJECT('rev',JSON_EXTRACT(d.data, '$._rev')), d.doc_id FROM docsdb.documents d JOIN (SELECT DISTINCT doc_id FROM docsdb.changes WHERE seq_number > ${begin_seq_number} AND seq_id > ${begin_seq_id} AND seq_number <= ${end_seq_number} AND seq_id <= ${end_seq_id}) c USING(doc_id) ")
		ddv.Select = make(map[string]string)
		ddv.Select["default"] = "SELECT JSON_OBJECT('offset', 0,'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) as rs FROM all_docs WHERE (${key} IS NULL or key = ${key})"
		ddoc.Views["_all_docs"] = ddv

		ddocJSON, _ := JSONMarshal(ddoc)
		designDoc, err := ParseDocument(ddocJSON)
		if err != nil {
			panic(err)
		}

		err = database.PutDocument(designDoc)
		if err != nil {
			return err
		}

	}

	docs, _ := database.GetAllDesignDocuments()
	for _, x := range docs {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(x.value, ddoc)
		if err != nil {
			panic("invalid_design_document " + x.id)
		}
		database.designDocuments[x.id] = ddoc
	}

	return nil
}

func (database *Database) Close() error {
	for idx := range database.views {
		view := database.views[idx]
		view.Close()
	}
	return database.db.Close()
}

func JSONMarshal(t interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(t)
	return buffer.Bytes(), err
}

func (database *Database) PutDocument(newDoc *Document) error {
	db := database.db

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	row := tx.QueryRow("SELECT doc_id, rev_number, rev_id, deleted FROM revisions WHERE doc_id = ? LIMIT 1", newDoc.id)
	currentDoc := Document{}
	row.Scan(&currentDoc.id, &currentDoc.revNumber, &currentDoc.revID, &currentDoc.deleted)

	if currentDoc.id == "" && (newDoc.revNumber > 0 || newDoc.revID != "") {
		return errors.New("doc_not_found")
	} else if currentDoc.revNumber != newDoc.revNumber || currentDoc.revID != newDoc.revID {
		return errors.New("mismatched_rev")
	}

	newDoc.CalculateRev()

	if newDoc.deleted {
		if _, err := tx.Exec("DELETE FROM documents WHERE doc_id = ?", currentDoc.id); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, data) VALUES(?, ?)", newDoc.id, newDoc.value); err != nil {
			return err
		}
	}

	if _, err := tx.Exec("INSERT INTO revisions (doc_id, rev_number, rev_id, deleted) VALUES(?, ?, ?, ?)", newDoc.id, newDoc.revNumber, newDoc.revID, newDoc.deleted); err != nil {
		return err
	}

	database.mux.Lock()
	database.updateSeqNumber, database.updateSeqID = database.seqGen.Next()
	database.mux.Unlock()

	if _, err := tx.Exec("INSERT INTO changes (seq_number, seq_id, doc_id, rev_number, rev_id) VALUES(?, ?, ?, ?, ?)", database.updateSeqNumber, database.updateSeqID, newDoc.id, newDoc.revNumber, newDoc.revID); err != nil {
		return err
	}

	if strings.HasPrefix(newDoc.id, "_design/") {
		database.MarkViewUpdated(newDoc.id, newDoc.value)
		fmt.Println(newDoc.id)
	}

	tx.Commit()

	return nil
}

func (database *Database) GetDocument(newDoc *Document, includeDoc bool) error {

	db := database.db

	var row *sql.Row
	if newDoc.revNumber >= 0 {
		row = db.QueryRow("SELECT doc_id, rev_number, rev_id, deleted FROM revisions WHERE doc_id = ? AND rev_number = ? AND rev_id = ? LIMIT 1", newDoc.id, newDoc.revNumber, newDoc.revID)
	} else {
		row = db.QueryRow("SELECT doc_id, rev_number, rev_id, deleted FROM revisions WHERE doc_id = ? LIMIT 1", newDoc.id)
	}
	row.Scan(&newDoc.id, &newDoc.revNumber, &newDoc.revID, &newDoc.deleted)

	if newDoc.id == "" || newDoc.deleted {
		return errors.New("doc_not_found")
	}

	if includeDoc {
		row := db.QueryRow("SELECT data FROM documents WHERE doc_id = ?", newDoc.id)
		var data string
		row.Scan(&data)
		newDoc.value = []byte(data)
	}

	return nil
}

func (database *Database) GetAllDesignDocuments() ([]Document, error) {
	db := database.db
	var ddocs []Document
	rows, err := db.Query("SELECT doc_id, data FROM documents WHERE doc_id like '_design/%'")
	if err != nil {
		return nil, err
	}

	for {

		if !rows.Next() {
			break
		}

		var (
			docID string
			data  string
		)
		err = rows.Scan(&docID, &data)
		if err != nil {
			return nil, err
		}

		jsondoc := `{"_id" : "` + docID + `"}`
		doc, _ := ParseDocument([]byte(jsondoc))
		err = database.GetDocument(doc, true)
		if err != nil {
			return nil, err
		}

		ddocs = append(ddocs, *doc)
	}

	return ddocs, nil
}

func (database *Database) DeleteDocument(newDoc *Document) error {
	newDoc.deleted = true
	return database.PutDocument(newDoc)
}

func (database *Database) OpenView(viewName string, ddoc *DesignDocument) error {

	view := NewView(database.dbPath, database.viewPath, database.name, viewName, ddoc)

	if err := view.Open(); err != nil {
		return err
	}

	name := ddoc.ID + "$" + viewName

	database.views[name] = view

	return nil
}

func (database *Database) SelectView(ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	name := ddocID + "$" + viewName

	view, ok := database.views[name]
	if !ok {
		ddoc, ok := database.designDocuments[ddocID]
		if !ok {
			return nil, errors.New("doc_not_found")
		}
		_, ok = ddoc.Views[viewName]
		if !ok {
			return nil, errors.New("view_not_found")
		}

		err := database.OpenView(viewName, ddoc)
		if err != nil {
			return nil, err
		}
		view = database.views[name]
	}

	if !stale {
		err := view.Build(database.updateSeqNumber, database.updateSeqID)
		if err != nil {
			return nil, err
		}
	}

	return view.Select(selectName, values), nil
}

func (database *Database) MarkViewUpdated(ddocID string, value []byte) {
	for k, x := range database.views {
		if x.designDocID == ddocID {
			x.Close()
			delete(database.views, k)
		}
	}
	ddoc := &DesignDocument{}
	err := json.Unmarshal(value, ddoc)
	if err != nil {
		panic("invalid_design_document " + ddocID)
	}
	database.designDocuments[ddocID] = ddoc
}

func (database *Database) GetLastUpdateSequence() (int, string) {
	sqlGetMaxSeq := "SELECT seq_number, seq_id FROM (SELECT MAX(seq_number) as seq_number, MAX(seq_id)  as seq_id FROM changes WHERE seq_id = (SELECT MAX(seq_id) FROM changes) UNION ALL SELECT 0, '') WHERE seq_number IS NOT NULL LIMIT 1"
	row := database.db.QueryRow(sqlGetMaxSeq)
	var (
		maxSeqNumber int
		maxSeqID     string
	)

	err := row.Scan(&maxSeqNumber, &maxSeqID)
	if err != nil {
		panic(err)
	}

	return maxSeqNumber, maxSeqID
}

func (database *Database) GetDocumentCount() int {
	sqlGetCount := "SELECT COUNT(1) FROM documents"
	row := database.db.QueryRow(sqlGetCount)
	var (
		count int
	)

	err := row.Scan(&count)
	if err != nil {
		panic(err)
	}

	return count
}

func (database *Database) Stat() *DBStat {
	stat := &DBStat{}
	stat.DBName = database.name
	stat.UpdateSeq = strconv.Itoa(database.updateSeqNumber) + "-" + database.updateSeqID
	stat.DocCount = database.GetDocumentCount()
	return stat
}

func (database *Database) Vacuum() error {
	_, err := database.db.Exec("VACUUM")
	return err
}
