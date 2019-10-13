package main

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Database struct {
	name            string
	updateSeqNumber int
	updateSeqID     string

	dbPath    string
	reader    *DataBaseReader
	writer    *DataBaseWriter
	mux       sync.Mutex
	changeSeq *ChangeSequenceGenarator
	idSeq     *SequenceUUIDGenarator
	viewmgr   *ViewManager
}

func NewDatabase(name, dbPath, viewPath string) *Database {
	db := &Database{name: name, dbPath: dbPath}
	db.viewmgr = NewViewManager(dbPath, viewPath, name)
	return db
}

func (db *Database) Open(createIfNotExists bool) error {
	path := filepath.Join(db.dbPath, db.name+dbExt)
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
	db.changeSeq = NewChangeSequenceGenarator(138, db.updateSeqNumber, db.updateSeqID)
	db.idSeq = NewSequenceUUIDGenarator()

	viewmgr := db.viewmgr
	if createIfNotExists {
		err = viewmgr.SetupViews(db)
		if err != nil {
			return err
		}
	}

	err = viewmgr.Initialize(db)
	if err != nil {
		return err
	}

	return nil
}

func (db *Database) Close() error {
	db.viewmgr.CloseViews()
	db.writer.Close()
	db.reader.Close()
	return nil
}

func (db *Database) PutDocument(newDoc *Document) (*Document, error) {
	writer := db.writer

	if newDoc.ID == "" {
		newDoc.ID = db.idSeq.Next()
	}

	currentDoc, err := writer.GetDocumentRevisionByID(newDoc.ID)
	if err != nil && err.Error() != "doc_not_found" {
		return nil, err
	}

	if currentDoc != nil && !currentDoc.Deleted && (currentDoc.RevNumber != newDoc.RevNumber || currentDoc.RevID != newDoc.RevID) {
		return nil, errors.New("mismatched_rev")
	}

	if currentDoc != nil && currentDoc.Deleted {
		newDoc.RevNumber = currentDoc.RevNumber
	}

	newDoc.CalculateRev()

	db.mux.Lock()

	err = writer.Begin()
	if err != nil {
		return nil, err
	}

	defer writer.Rollback()

	updateSeqNumber, updateSeqID := db.changeSeq.Next()

	if newDoc.Deleted {
		if err := writer.DeleteDocumentByID(newDoc.ID); err != nil {
			return nil, err
		}
	} else {
		if err := writer.InsertDocument(newDoc.ID, newDoc.RevNumber, newDoc.RevID, newDoc.Deleted, newDoc.Data); err != nil {
			return nil, err
		}
	}

	if err := writer.InsertChanges(updateSeqNumber, updateSeqID, newDoc.ID, newDoc.RevNumber, newDoc.RevID, newDoc.Deleted); err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	db.updateSeqNumber = updateSeqNumber
	db.updateSeqID = updateSeqID

	db.mux.Unlock()

	if strings.HasPrefix(newDoc.ID, "_design/") {
		db.viewmgr.UpdateDesignDocument(newDoc.ID, newDoc.Data)
	}

	doc := Document{
		Revision: Revision{
			ID:        newDoc.ID,
			RevNumber: newDoc.RevNumber,
			RevID:     newDoc.RevID,
			Deleted:   newDoc.Deleted,
		},
	}

	return &doc, nil
}

func (db *Database) GetDocument(doc *Revision, includeData bool) (*Document, error) {

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

func (db *Database) DeleteDocument(doc *Document) (*Document, error) {
	doc.Deleted = true
	return db.PutDocument(doc)
}

func (db *Database) SelectView(ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	return db.viewmgr.SelectView(db.updateSeqNumber, db.updateSeqID, ddocID, viewName, selectName, values, stale)
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
	stat.UpdateSeq = formatRev(db.updateSeqNumber, db.updateSeqID)
	stat.DocCount = db.GetDocumentCount()
	return stat
}

func (db *Database) Vacuum() error {
	return db.writer.Vacuum()
}
