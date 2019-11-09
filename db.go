package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
)

type Database struct {
	name        string
	updateSeqID string

	dbPath  string
	readers DatabaseReaderPool
	writer  DatabaseWriter
	mux     sync.Mutex

	changeSeq *ChangeSequenceGenarator
	idSeq     *SequenceUUIDGenarator
	viewmgr   ViewManager
}

func NewDatabase(name, dbPath, viewPath string, createIfNotExists bool) (*Database, error) {
	path := filepath.Join(dbPath, name+dbExt)
	var fileHandler DefaultFileHandler
	if !fileHandler.IsFileExists(path) {
		if !createIfNotExists {
			return nil, ErrDBNotFound
		}
	} else {
		if createIfNotExists {
			return nil, ErrDBExists
		}
	}

	db := &Database{name: name, dbPath: path}
	db.idSeq = NewSequenceUUIDGenarator()

	connStr := db.dbPath + "?_journal=WAL"
	db.writer = new(DefaultDatabaseWriter)
	db.writer.Open(connStr)
	db.readers = NewDatabaseReaderPool(connStr, 4)

	if createIfNotExists {
		db.writer.Begin()
		if err := db.writer.ExecBuildScript(); err != nil {
			return nil, err
		}
		db.writer.Commit()
	}

	db.Open()
	db.viewmgr = NewViewManager(path, viewPath, name)

	if createIfNotExists {
		err := db.viewmgr.SetupViews(db)
		if err != nil {
			return nil, err
		}
	}

	err := db.viewmgr.Initialize(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Database) GetViewManager() ViewManager {
	return db.viewmgr
}

func (db *Database) Open() error {
	db.updateSeqID = db.GetLastUpdateSequence()
	db.changeSeq = NewChangeSequenceGenarator(138, db.updateSeqID)
	return nil
}

func (db *Database) Close() error {
	db.viewmgr.Close()
	db.writer.Close()
	db.readers.Close()
	return nil
}

func (db *Database) PutDocument(newDoc *Document) (*Document, error) {

	writer := db.writer

	db.mux.Lock()
	defer db.mux.Unlock()

	err := writer.Begin()
	defer writer.Rollback()
	if err != nil {
		return nil, err
	}

	if newDoc.ID == "" {
		newDoc.ID = db.idSeq.Next()
	}

	currentDoc, err := writer.GetDocumentRevisionByID(newDoc.ID)
	if err != nil && err != ErrDocNotFound {
		return nil, fmt.Errorf("%s: %w", err.Error(), ErrInternalError)
	}

	if currentDoc != nil {
		if !currentDoc.Deleted {
			if currentDoc.Version != newDoc.Version {
				return nil, ErrDocConflict
			}
		} else {
			if newDoc.Version > 0 {
				return nil, ErrDocConflict
			}
			newDoc.Version = currentDoc.Version
		}
	} else {
		if newDoc.Version > 0 {
			return nil, ErrDocConflict
		}
	}

	newDoc.CalculateVersion()

	updateSeqID := db.changeSeq.Next()

	err = writer.PutDocument(updateSeqID, newDoc, currentDoc)
	if err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	db.updateSeqID = updateSeqID

	doc := Document{
		ID:      newDoc.ID,
		Version: newDoc.Version,
		Deleted: newDoc.Deleted,
	}

	return &doc, nil
}

func (db *Database) GetDocument(doc *Document, includeData bool) (*Document, error) {

	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	if includeData {
		if doc.Version > 0 {
			return reader.GetDocumentByIDandVersion(doc.ID, doc.Version)
		}
		return reader.GetDocumentByID(doc.ID)
	}

	if doc.Version > 0 {
		return reader.GetDocumentRevisionByIDandVersion(doc.ID, doc.Version)
	}
	return reader.GetDocumentRevisionByID(doc.ID)
}

func (db *Database) GetAllDesignDocuments() ([]*Document, error) {
	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	return reader.GetAllDesignDocuments()
}

func (db *Database) DeleteDocument(doc *Document) (*Document, error) {
	doc.Deleted = true
	return db.PutDocument(doc)
}

func (db *Database) GetLastUpdateSequence() string {
	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	return reader.GetLastUpdateSequence()
}

func (db *Database) GetChanges(since string) ([]byte, error) {
	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	return reader.GetChanges(since)
}

func (db *Database) GetDocumentCount() int {
	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	return reader.GetDocumentCount()
}

func (db *Database) Stat() *DBStat {
	stat := &DBStat{}
	stat.DBName = db.name
	stat.UpdateSeq = db.updateSeqID
	stat.DocCount = db.GetDocumentCount()
	return stat
}

func (db *Database) Vacuum() error {
	db.viewmgr.Vacuum()
	return db.writer.Vacuum()
}

func (db *Database) SelectView(ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	idoc, err := ParseDocument([]byte(fmt.Sprintf(`{"_id":"%s"}`, ddocID)))
	if err != nil {
		return nil, err
	}
	doc, err := db.GetDocument(idoc, true)
	if err != nil {
		return nil, err
	}
	return db.viewmgr.SelectView(db.updateSeqID, ddocID, doc, viewName, selectName, values, stale)
}
