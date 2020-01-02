package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
)

type Database struct {
	Name            string
	UpdateSeq       string
	DocCount        int
	DeletedDocCount int
	DBPath          string
	ViewPath        string

	mux         sync.Mutex
	readers     DatabaseReaderPool
	writer      DatabaseWriter
	changeSeq   *ChangeSequenceGenarator
	idSeq       *SequenceUUIDGenarator
	viewManager ViewManager
}

func (db *Database) Open(connectionString string, createIfNotExists bool) error {

	err := db.writer.Open(connectionString + "&mode=rwc")
	if err != nil {
		panic(err)
	}

	err = db.readers.Open(connectionString + "&mode=ro")
	if err != nil {
		panic(err)
	}

	if createIfNotExists {
		db.writer.Begin()
		if err := db.writer.ExecBuildScript(); err != nil {
			return err
		}
		db.writer.Commit()
	}

	db.DocCount, db.DeletedDocCount = db.GetDocumentCount()

	db.UpdateSeq = db.GetLastUpdateSequence()
	db.changeSeq = NewChangeSequenceGenarator(138, db.UpdateSeq)

	if createIfNotExists {
		err := db.viewManager.SetupViews(db)
		if err != nil {
			return err
		}
	}

	err = db.viewManager.Initialize(db)
	if err != nil {
		return err
	}

	return nil
}

func (db *Database) Close() error {
	db.mux.Lock()
	defer db.mux.Unlock()

	db.viewManager.Close()
	db.writer.Close()
	db.readers.Close()
	return nil
}

func (db *Database) PutDocument(newDoc *Document) (*Document, error) {

	db.mux.Lock()
	defer db.mux.Unlock()

	writer := db.writer

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
		if currentDoc.Deleted {
			if newDoc.Version > 0 && currentDoc.Version > newDoc.Version {
				return nil, ErrDocConflict
			}
			newDoc.Version = currentDoc.Version
		} else {
			if currentDoc.Version != newDoc.Version {
				return nil, ErrDocConflict
			}
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

	if currentDoc == nil {
		db.DocCount++
	}
	if newDoc.Deleted {
		db.DocCount--
		db.DeletedDocCount++
	}

	return newDoc, nil
}

func (db *Database) DeleteDocument(doc *Document) (*Document, error) {
	doc.Deleted = true
	return db.PutDocument(doc)
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

func (db *Database) GetDocumentCount() (int, int) {
	reader := db.readers.Borrow()
	defer db.readers.Return(reader)

	reader.Begin()
	defer reader.Commit()

	return reader.GetDocumentCount()
}

func (db *Database) GetStat() *DBStat {
	db.mux.Lock()
	defer db.mux.Unlock()

	stat := &DBStat{}
	stat.DBName = db.Name
	stat.UpdateSeq = db.UpdateSeq
	stat.DocCount = db.DocCount
	stat.DeletedDocCount = db.DeletedDocCount
	return stat
}

func (db *Database) Vacuum() error {
	return db.writer.Vacuum()
}

func (db *Database) SelectView(ddocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	inputDoc := &Document{ID: ddocID}
	outputDoc, err := db.GetDocument(inputDoc, true)
	if err != nil {
		return nil, err
	}

	return db.viewManager.SelectView(db.UpdateSeq, outputDoc, viewName, selectName, values, stale)
}

func (db *Database) ValidateDesignDocument(doc *Document) error {
	return db.viewManager.ValidateDesignDocument(doc)
}

func NewDatabase(name, fileName, dbPath, defaultViewPath string, createIfNotExists bool, serviceLocator ServiceLocator) (*Database, error) {
	fileHandler := serviceLocator.GetFileHandler()
	path := filepath.Join(dbPath, fileName+dbExt)
	if !fileHandler.IsFileExists(path) {
		if !createIfNotExists {
			return nil, ErrDBNotFound
		}
	} else {
		if createIfNotExists {
			return nil, ErrDBExists
		}
	}

	db := &Database{Name: name, DBPath: path, ViewPath: defaultViewPath}
	db.idSeq = NewSequenceUUIDGenarator()
	connectionString := db.DBPath + "?_journal=WAL&cache=shared&_mutex=no"
	db.readers = NewDatabaseReaderPool(4, serviceLocator)
	db.writer = serviceLocator.GetDatabaseWriter()
	db.viewManager = serviceLocator.GetViewManager()

	err := db.Open(connectionString, createIfNotExists)
	if err != nil {
		panic(err)
	}

	return db, nil
}
