package main

import (
	"bytes"
	"encoding/json"
	"errors"
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

	dbPath   string
	viewPath string
	reader   *DataBaseReader
	writer   *DataBaseWriter
	mux      sync.Mutex
	seqGen   *SequenceGenarator

	views           map[string]*View
	designDocuments map[string]*DesignDocument
}

func NewDatabase(dbPath, viewPath string) *Database {
	db := &Database{}
	db.dbPath = dbPath
	db.viewPath = viewPath
	return db
}

func (db *Database) Open(name string, createIfNotExists bool) error {
	db.name = name
	db.views = make(map[string]*View)
	db.designDocuments = make(map[string]*DesignDocument)

	path := filepath.Join(db.dbPath, name+dbExt)
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

	db.reader = new(DataBaseReader)
	db.reader.Open(path + "?_journal=WAL")

	db.writer = new(DataBaseWriter)
	db.writer.Open(path + "?_journal=WAL")

	db.writer.Begin()
	if err = db.writer.ExecBuildScript(); err != nil {
		return err
	}
	db.writer.Commit()

	db.updateSeqNumber, db.updateSeqID = db.GetLastUpdateSequence()
	db.seqGen = NewSequenceGenarator(138, db.updateSeqNumber, db.updateSeqID)

	if createIfNotExists {
		ddoc := &DesignDocument{}
		ddoc.ID = "_design/_views"
		ddoc.Views = make(map[string]*DesignDocumentView)
		ddv := &DesignDocumentView{}
		ddv.Setup = append(ddv.Setup, "CREATE TABLE IF NOT EXISTS all_docs (key TEXT PRIMARY KEY, value TEXT, doc_id TEXT)")
		ddv.Delete = append(ddv.Delete, "DELETE FROM all_docs WHERE doc_id IN (SELECT DISTINCT doc_id FROM docsdb.changes WHERE seq_number > ${begin_seq_number} AND seq_id > ${begin_seq_id} AND seq_number <= ${end_seq_number} AND seq_id <= ${end_seq_id})")
		ddv.Update = append(ddv.Update, "INSERT INTO all_docs (key, value, doc_id) SELECT d.doc_id, JSON_OBJECT('rev',JSON_EXTRACT(d.data, '$._rev')), d.doc_id FROM docsdb.documents d JOIN (SELECT DISTINCT doc_id FROM docsdb.changes WHERE seq_number > ${begin_seq_number} AND seq_id > ${begin_seq_id} AND seq_number <= ${end_seq_number} AND seq_id <= ${end_seq_id}) c USING(doc_id) ")
		ddv.Select = make(map[string]string)
		ddv.Select["default"] = "SELECT JSON_OBJECT('offset', 0,'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) as rs FROM all_docs WHERE (${key} IS NULL or key = ${key}) ORDER BY key"
		ddoc.Views["_all_docs"] = ddv

		ddocJSON, _ := JSONMarshal(ddoc)
		designDoc, err := ParseDocument(ddocJSON)
		if err != nil {
			panic(err)
		}

		err = db.PutDocument(designDoc)
		if err != nil {
			return err
		}

	}

	docs, _ := db.GetAllDesignDocuments()
	for _, x := range docs {
		ddoc := &DesignDocument{}
		err := json.Unmarshal(x.Data, ddoc)
		if err != nil {
			panic("invalid_design_document " + x.ID)
		}
		db.designDocuments[x.ID] = ddoc
	}

	return nil
}

func (db *Database) Close() error {
	for idx := range db.views {
		view := db.views[idx]
		view.Close()
	}

	db.writer.Close()
	db.reader.Close()

	return nil
}

func (db *Database) PutDocument(newDoc *Document) error {
	writer := db.writer
	err := writer.Begin()
	if err != nil {
		return err
	}
	defer writer.Rollback()

	currentDoc, err := writer.GetDocumentRevisionByID(newDoc.ID)
	if err != nil && err.Error() != "doc_not_found" {
		return err
	}

	if currentDoc != nil && !currentDoc.Deleted && (currentDoc.RevNumber != newDoc.RevNumber || currentDoc.RevID != newDoc.RevID) {
		return errors.New("mismatched_rev")
	}

	if currentDoc != nil && currentDoc.Deleted {
		newDoc.RevNumber = currentDoc.RevNumber
		newDoc.CalculateRev()
	} else {
		newDoc.CalculateRev()
	}

	if newDoc.Deleted {
		if err := writer.DeleteDocumentByID(newDoc.ID); err != nil {
			return err
		}
	} else {
		if err := writer.InsertDocument(newDoc.ID, newDoc.Data); err != nil {
			return err
		}
	}

	if err := writer.InsertRevision(newDoc.ID, newDoc.RevNumber, newDoc.RevID, newDoc.Deleted); err != nil {
		return err
	}

	db.mux.Lock()
	db.updateSeqNumber, db.updateSeqID = db.seqGen.Next()
	db.mux.Unlock()

	if err := writer.InsertChange(db.updateSeqNumber, db.updateSeqID, newDoc.ID, newDoc.RevNumber, newDoc.RevID, newDoc.Deleted); err != nil {
		return err
	}

	if strings.HasPrefix(newDoc.ID, "_design/") {
		db.MarkViewUpdated(newDoc.ID, newDoc.Data)
	}

	writer.Commit()

	return nil
}

func (db *Database) GetDocument(doc *Document, includeData bool) (*Document, error) {

	reader := db.reader

	if includeData {
		if doc.RevNumber > 0 {
			return reader.GetDocumentByIDandRev(doc.ID, doc.RevNumber, doc.RevID)
		}
		return reader.GetDocumentByID(doc.ID)

	}
	if doc.RevNumber > 0 {
		return reader.GetDocumentRevisionByIDandRev(doc.ID, doc.RevNumber, doc.RevID)
	}
	return reader.GetDocumentRevisionByID(doc.ID)
}

func (db *Database) GetAllDesignDocuments() ([]*Document, error) {
	return db.reader.GetAllDesignDocuments()
}

func (db *Database) DeleteDocument(doc *Document) error {
	doc.Deleted = true
	return db.PutDocument(doc)
}

func (db *Database) OpenView(viewName string, ddoc *DesignDocument) error {

	view := NewView(db.dbPath, db.viewPath, db.name, viewName, ddoc)

	if err := view.Open(); err != nil {
		return err
	}

	name := ddoc.ID + "$" + viewName

	db.views[name] = view

	return nil
}

func (db *Database) SelectView(ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	name := ddocID + "$" + viewName
	view, ok := db.views[name]
	if !ok {
		ddoc, ok := db.designDocuments[ddocID]
		if !ok {
			return nil, errors.New("doc_not_found")
		}
		_, ok = ddoc.Views[viewName]
		if !ok {
			return nil, errors.New("view_not_found")
		}

		err := db.OpenView(viewName, ddoc)
		if err != nil {
			return nil, err
		}
		view = db.views[name]
	}

	if !stale {
		err := view.Build(db.updateSeqNumber, db.updateSeqID)
		if err != nil {
			return nil, err
		}
	}

	return view.Select(selectName, values), nil
}

func (db *Database) MarkViewUpdated(ddocID string, value []byte) {
	for k, x := range db.views {
		if x.designDocID == ddocID {
			x.Close()
			delete(db.views, k)
			os.Remove(filepath.Join(x.viewPath, x.fileName))
		}
	}
	ddoc := &DesignDocument{}
	err := json.Unmarshal(value, ddoc)
	if err != nil {
		panic("invalid_design_document " + ddocID)
	}
	db.designDocuments[ddocID] = ddoc
}

func (db *Database) GetLastUpdateSequence() (int, string) {
	return db.reader.GetLastUpdateSequence()
}

func (db *Database) GetDocumentCount() int {
	return db.reader.GetDocumentCount()
}

func (db *Database) Stat() *DBStat {
	stat := &DBStat{}
	stat.DBName = db.name
	stat.UpdateSeq = strconv.Itoa(db.updateSeqNumber) + "-" + db.updateSeqID
	stat.DocCount = db.GetDocumentCount()
	return stat
}

func (db *Database) Vacuum() error {
	return db.writer.Vacuum()
}

func JSONMarshal(t interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(t)
	return buffer.Bytes(), err
}
