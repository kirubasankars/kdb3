package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
)

// Database interface
type Database interface {
	Open(createIfNotExists bool) error
	Close() error

	PutDocument(doc *Document) (*Document, error)
	DeleteDocument(doc *Document) (*Document, error)
	GetDocument(doc *Document, includeData bool) (*Document, error)
	GetAllDesignDocuments() ([]*Document, error)
	GetLastUpdateSequence() string
	GetChanges(since string, limit int) ([]byte, error)
	GetDocumentCount() (int, int)

	GetStat() *DatabaseStat
	SelectView(designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	ValidateDesignDocument(doc *Document) error
	SetupAllDocsViews() error
	Vacuum() error

	GetViewManager() ViewManager
}

// DefaultDatabase default implementation of database
type DefaultDatabase struct {
	Name                 string
	UpdateSequence       string
	DocumentCount        int
	DeletedDocumentCount int

	mutex     sync.Mutex
	changeSeq *ChangeSequenceGenarator
	idSeq     *SequenceUUIDGenarator

	reader DatabaseReader
	writer DatabaseWriter

	viewManager ViewManager
}

// Open open kdb database
func (db *DefaultDatabase) Open(createIfNotExists bool) error {
	err := db.writer.Open()
	if err != nil {
		panic(err)
	}

	err = db.reader.Open()
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

	db.DocumentCount, db.DeletedDocumentCount = db.GetDocumentCount()
	db.UpdateSequence = db.GetLastUpdateSequence()
	db.changeSeq = NewChangeSequenceGenarator(138, db.UpdateSequence)

	if createIfNotExists {
		err = db.SetupAllDocsViews()
		if err != nil {
			return err
		}
	}

	ddocs, _ := db.GetAllDesignDocuments()
	err = db.viewManager.Initialize(ddocs)
	if err != nil {
		return err
	}

	return nil
}

// Close close the kdb database
func (db *DefaultDatabase) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.viewManager.Close()
	db.reader.Close()
	db.writer.Close()

	return nil
}

// PutDocument put a document
func (db *DefaultDatabase) PutDocument(doc *Document) (*Document, error) {

	writer := db.writer

	err := writer.Begin()
	defer writer.Rollback()
	if err != nil {
		return nil, err
	}

	if doc.ID == "" {
		doc.ID = db.idSeq.Next()
	}

	currentDoc, err := writer.GetDocumentRevisionByID(doc.ID)
	if err != nil && err != ErrDocumentNotFound {
		return nil, fmt.Errorf("%s: %w", err.Error(), ErrInternalError)
	}

	if currentDoc != nil {
		if currentDoc.Deleted {
			if doc.Version > 0 && currentDoc.Version > doc.Version {
				return nil, ErrDocumentConflict
			}
			doc.Version = currentDoc.Version
		} else {
			if currentDoc.Version != doc.Version {
				return nil, ErrDocumentConflict
			}
		}
	}

	doc.CalculateNextVersion()

	updateSeq := db.changeSeq.Next()

	err = writer.PutDocument(updateSeq, doc, currentDoc)
	if err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	db.UpdateSequence = updateSeq

	if currentDoc == nil {
		db.DocumentCount++
	}
	if doc.Deleted {
		db.DocumentCount--
		db.DeletedDocumentCount++

		if strings.HasPrefix(doc.ID, "_design/") {
			db.viewManager.UpdateDesignDocument(doc, "")
		}
	}

	return doc, nil
}

// DeleteDocument delete a document
func (db *DefaultDatabase) DeleteDocument(doc *Document) (*Document, error) {
	doc.Deleted = true
	return db.PutDocument(doc)
}

// GetDocument get a document
func (db *DefaultDatabase) GetDocument(doc *Document, includeData bool) (*Document, error) {

	reader := db.reader

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

// GetAllDesignDocuments get all design document
func (db *DefaultDatabase) GetAllDesignDocuments() ([]*Document, error) {
	reader := db.reader

	reader.Begin()
	defer reader.Commit()

	return reader.GetAllDesignDocuments()
}

// GetLastUpdateSequence get last sequence number
func (db *DefaultDatabase) GetLastUpdateSequence() string {
	reader := db.reader

	reader.Begin()
	defer reader.Commit()

	return reader.GetLastUpdateSequence()
}

// GetChanges get changes
func (db *DefaultDatabase) GetChanges(since string, limit int) ([]byte, error) {
	reader := db.reader

	reader.Begin()
	defer reader.Commit()

	return reader.GetChanges(since, limit)
}

// GetDocumentCount get document count
func (db *DefaultDatabase) GetDocumentCount() (int, int) {
	reader := db.reader

	reader.Begin()
	defer reader.Commit()

	return reader.GetDocumentCount()
}

// GetStat get database stat
func (db *DefaultDatabase) GetStat() *DatabaseStat {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	stat := &DatabaseStat{}
	stat.DBName = db.Name
	stat.UpdateSeq = db.UpdateSequence
	stat.DocCount = db.DocumentCount
	stat.DeletedDocCount = db.DeletedDocumentCount
	return stat
}

// Vacuum vacuum
func (db *DefaultDatabase) Vacuum() error {
	return db.writer.Vacuum()
}

// SelectView select view
func (db *DefaultDatabase) SelectView(designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	inputDoc := &Document{ID: designDocID}
	outputDoc, err := db.GetDocument(inputDoc, true)
	if err != nil {
		return nil, err
	}

	return db.viewManager.SelectView(db.UpdateSequence, outputDoc, viewName, selectName, values, stale)
}

// ValidateDesignDocument validate design document
func (db *DefaultDatabase) ValidateDesignDocument(doc *Document) error {
	return db.viewManager.ValidateDesignDocument(doc)
}

// GetViewManager get a view manager
func (db *DefaultDatabase) GetViewManager() ViewManager {
	return db.viewManager
}

// SetupAllDocsViews setup default views
func (db *DefaultDatabase) SetupAllDocsViews() error {
	ddoc := &DesignDocument{}
	ddoc.ID = "_design/_views"
	ddoc.Kind = "design"
	ddoc.Views = make(map[string]*DesignDocumentView)
	ddv := &DesignDocumentView{}
	ddv.Setup = append(ddv.Setup, "CREATE TABLE IF NOT EXISTS all_docs (key, value, doc_id,  PRIMARY KEY(key)) WITHOUT ROWID")
	ddv.Run = append(ddv.Run, "DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)")
	ddv.Run = append(ddv.Run, "INSERT OR REPLACE INTO all_docs (key, value, doc_id) SELECT doc_id, JSON_OBJECT('version', version), doc_id FROM latest_documents WHERE deleted = 0")
	ddv.Select = make(map[string]string)
	ddv.Select["default"] = "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) WHERE (${key} IS NULL or key = ${key})"
	ddv.Select["with_docs"] = "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('id', doc_id, 'key', key, 'value', JSON(value), 'doc', JSON((SELECT data FROM documents WHERE doc_id = o.doc_id)))),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) o WHERE (${key} IS NULL or key = ${key})"

	ddoc.Views["_all_docs"] = ddv

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(ddoc)
	if err != nil {
		panic(err)
	}

	designDoc, err := ParseDocument(buffer.Bytes())
	if err != nil {
		panic(err)
	}

	_, err = db.PutDocument(designDoc)
	if err != nil {
		return err
	}
	return nil
}

// NewDatabase create database instance
func NewDatabase(name string, createIfNotExists bool, serviceLocator ServiceLocator) Database {
	db := &DefaultDatabase{Name: name}
	db.idSeq = NewSequenceUUIDGenarator()

	db.reader = serviceLocator.GetDatabaseReader(name)
	db.writer = serviceLocator.GetDatabaseWriter(name)
	db.viewManager = serviceLocator.GetViewManager(name)

	err := db.Open(createIfNotExists)
	if err != nil {
		panic(err)
	}

	return db
}
