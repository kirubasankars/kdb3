package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
)

type Database struct {
	Name      string
	UpdateSeq string
	DBPath    string

	mux         sync.Mutex
	readers     DatabaseReaderPool
	writer      DatabaseWriter
	changeSeq   *ChangeSequenceGenarator
	idSeq       *SequenceUUIDGenarator
	viewManager ViewManager
}

func NewDatabase(name, dbPath, viewPath string, createIfNotExists bool, serviceLocator ServiceLocator) (*Database, error) {
	fileHandler := serviceLocator.GetFileHandler()
	path := filepath.Join(dbPath, name+dbExt)
	if !fileHandler.IsFileExists(path) {
		if !createIfNotExists {
			return nil, ErrDBNotFound
		}
	} else {
		if createIfNotExists {
			return nil, ErrDBExists
		}
	}

	db := &Database{Name: name, DBPath: path}
	db.idSeq = NewSequenceUUIDGenarator()

	connectionString := db.DBPath + "?_journal=WAL"
	db.writer = serviceLocator.GetDatabaseWriter(connectionString)
	db.writer.Open()
	db.readers = serviceLocator.GetDatabaseReaderPool(connectionString, 4)

	if createIfNotExists {
		db.writer.Begin()
		if err := db.writer.ExecBuildScript(); err != nil {
			return nil, err
		}
		db.writer.Commit()
	}

	db.Open()

	absoluteDBPath, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}

	db.viewManager = serviceLocator.GetViewManager(name, absoluteDBPath, viewPath)

	if createIfNotExists {
		err := db.viewManager.SetupViews(db)
		if err != nil {
			return nil, err
		}
	}

	err = db.viewManager.Initialize(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Database) Initialize() {

}

func (db *Database) GetViewManager() ViewManager {
	return db.viewManager
}

func (db *Database) Open() error {
	db.UpdateSeq = db.GetLastUpdateSequence()
	db.changeSeq = NewChangeSequenceGenarator(138, db.UpdateSeq)
	return nil
}

func (db *Database) Close() error {
	db.viewManager.Close()
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

	newDoc.CalculateNextVersion()

	updateSeq := db.changeSeq.Next()

	err = writer.PutDocument(updateSeq, newDoc, currentDoc)
	if err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	db.UpdateSeq = updateSeq

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

func (db *Database) GetChanges(since string, limit int) ([]byte, error) {
	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	return reader.GetChanges(since, limit)
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
	stat.DBName = db.Name
	stat.UpdateSeq = db.UpdateSeq
	stat.DocCount = db.GetDocumentCount()
	return stat
}

func (db *Database) Vacuum() error {
	db.viewManager.Vacuum()
	return db.writer.Vacuum()
}

func (db *Database) SelectView(ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	inputDoc, err := ParseDocument([]byte(fmt.Sprintf(`{"_id":"%s"}`, ddocID)))
	if err != nil {
		return nil, err
	}

	outputDoc, err := db.GetDocument(inputDoc, true)
	if err != nil {
		return nil, err
	}

	return db.viewManager.SelectView(db.UpdateSeq, outputDoc, viewName, selectName, values, stale)
}
