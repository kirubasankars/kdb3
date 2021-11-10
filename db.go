package main

import (
	"crypto/md5"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Database interface
type Database interface {
	Initialize() error
	ReInitialize() error
	Open(createIfNotExists bool) error
	Close(closeChannel bool) error

	PutDocument(doc *Document) (*Document, error)
	DeleteDocument(doc *Document) (*Document, error)
	GetDocument(doc *Document, includeData bool) (*Document, error)
	GetAllDesignDocuments() ([]Document, error)
	GetLastUpdateSequence() string
	GetChanges(since string, limit int, order bool) ([]byte, error)
	GetDocumentCount() (int, int)

	GetStat() *DatabaseStat
	SelectView(designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error)
	SQL(fromSeqID, designDocID, viewName string) ([]byte, error)
	ValidateDesignDocument(doc Document) error
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

	reader chan DatabaseReader
	writer chan DatabaseWriter

	viewManager   ViewManager
	vacuumManager chan VacuumManager

	schemaValidator SchemaValidator

	serviceLocator ServiceLocator
}

func (db *DefaultDatabase) openReaders() {
	readersCount := cap(db.reader)
	readers := make([]DatabaseReader, readersCount)
	for i := 0; i < readersCount; i++ {
		reader := <-db.reader
		err := reader.Open()
		if err != nil {
			reader.Close()
			continue
		}
		readers[i] = reader
	}
	for _, reader := range readers {
		db.reader <- reader
	}
}

// Open open kdb database
func (db *DefaultDatabase) Open(createIfNotExists bool) error {
	writer := <-db.writer
	err := writer.Open(createIfNotExists)
	if err != nil {
		panic(err)
	}
	db.writer <- writer

	// open all readers
	db.openReaders()

	db.DocumentCount, db.DeletedDocumentCount = db.GetDocumentCount()
	db.UpdateSequence = db.GetLastUpdateSequence()
	db.changeSeq = NewChangeSequenceGenarator(138, db.UpdateSequence)
	db.schemaValidator = &DefaultJSONSchemaValidator{}

	if createIfNotExists {
		if err = db.SetupAllDocsViews(); err != nil {
			return err
		}
	}

	designDocs, err := db.GetAllDesignDocuments()
	if err != nil {
		return err
	}

	db.schemaValidator.Setup(designDocs)

	return db.viewManager.Initialize(designDocs)
}

// Close close the kdb database
func (db *DefaultDatabase) Close(closeChannel bool) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	err := db.viewManager.Close(closeChannel)
	if err != nil {
		return err
	}

	writer := <-db.writer
	err = writer.Close()
	if err != nil {
		return err
	}

	var foundError error
	// close all readers
	func() {
		readersCount := cap(db.reader)
		for i := 0; i < readersCount; i++ {
			reader := <-db.reader
			err = reader.Close()
			if err != nil {
				foundError = err
			}
		}
	}()

	if foundError != nil {
		return foundError
	}

	if closeChannel {
		close(db.writer)
		close(db.reader)
	}

	return nil
}

// PutDocument put a document
func (db *DefaultDatabase) PutDocument(doc *Document) (*Document, error) {
	if errs := db.schemaValidator.Validate(doc); errs != nil {
		return nil, fmt.Errorf("%s: %w", strings.Join(errs, ","), ErrInternalError)
	}

	writer, ok := <-db.writer
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.writer <- writer
	}()

	defer writer.Rollback()
	if err := writer.Begin(); err != nil {
		return nil, err
	}

	if doc.ID == "" {
		doc.ID = db.idSeq.Next()
	}

	currentDoc, err := writer.GetDocumentMetadataByID(doc.ID)
	if err != nil && err != ErrDocumentNotFound {
		return nil, fmt.Errorf("%s: %w", err.Error(), ErrInternalError)
	}

	if currentDoc != nil {
		if currentDoc.Deleted {
			// delete document
			if doc.Version > 0 && currentDoc.Version > doc.Version {
				return nil, ErrDocumentConflict
			}
			doc.Version = currentDoc.Version
		} else {
			// update document
			if doc.Version != 0 {
				if currentDoc.Version != doc.Version {
					return nil, ErrDocumentConflict
				}
			} else {
				return nil, ErrDocumentConflict
			}
		}
	} else {
		// insert document
		if doc.Version > 0 {
			return nil, ErrDocumentConflict
		}
	}

	doc.CalculateNextVersion()
	updateSeq := db.changeSeq.Next()

	doc.Hash = fmt.Sprintf("%x", md5.Sum(doc.Data))

	if err = writer.PutDocument(updateSeq, doc); err != nil {
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
	}

	if currentDoc != nil && strings.HasPrefix(doc.ID, "_design/") {
		if doc.ID == "_design/_schema" {
			designDocs, _ := db.GetAllDesignDocuments()
			db.schemaValidator.Setup(designDocs)
		}
		// call only if design doc changed
		db.viewManager.DeleteViewsIfRemoved(*doc)
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

	reader, ok := <-db.reader
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	if includeData {
		if doc.Version > 0 {
			return reader.GetDocumentByIDandVersion(doc.ID, doc.Version, doc.Hash)
		}
		return reader.GetDocumentByID(doc.ID)
	}

	if doc.Version > 0 {
		return reader.GetDocumentMetadataByIDandVersion(doc.ID, doc.Version, doc.Hash)
	}
	return reader.GetDocumentMetadataByID(doc.ID)
}

// GetAllDesignDocuments get all design document
func (db *DefaultDatabase) GetAllDesignDocuments() ([]Document, error) {
	reader, ok := <-db.reader
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	reader.Begin()
	defer reader.Commit()

	return reader.GetAllDesignDocuments()
}

// GetLastUpdateSequence get last sequence number
func (db *DefaultDatabase) GetLastUpdateSequence() string {
	reader, ok := <-db.reader
	if !ok {
		panic(ErrDatabaseNotFound)
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	return reader.GetLastUpdateSequence()
}

// GetChanges get changes
func (db *DefaultDatabase) GetChanges(since string, limit int, desc bool) ([]byte, error) {
	reader, ok := <-db.reader
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

	return reader.GetChanges(since, limit, desc)
}

// GetDocumentCount get document count
func (db *DefaultDatabase) GetDocumentCount() (int, int) {
	reader, ok := <-db.reader
	if !ok {
		panic(ErrDatabaseNotFound)
	}
	defer func() {
		db.reader <- reader
	}()

	defer reader.Commit()
	reader.Begin()

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
	vacuumManager := <-db.vacuumManager
	defer func() {
		db.vacuumManager <- vacuumManager
	}()

	currentFileName := db.serviceLocator.GetLocalDB().GetDatabaseFileName(db.Name)
	currentDBPath := filepath.Join(db.serviceLocator.GetDBDirPath(), currentFileName+dbExt)
	currentConnectionString := currentDBPath

	id := NewSequenceUUIDGenarator().Next()
	newFileName := db.Name + "_" + id
	newConnectionString := filepath.Join(db.serviceLocator.GetDBDirPath(), newFileName+dbExt)

	vacuumManager.SetNewConnectionString(newConnectionString)
	vacuumManager.SetCurrentConnectionString(currentDBPath, currentConnectionString)

	vacuumManager.SetupDatabase()

	maxUpdateSequence := db.UpdateSequence
	vacuumManager.CopyData("", maxUpdateSequence)

	vacuumManager.Vacuum()

	db.Close(false)

	minUpdateSequence := maxUpdateSequence
	maxUpdateSequence = db.UpdateSequence

	vacuumManager.CopyData(minUpdateSequence, maxUpdateSequence)

	localDB := db.serviceLocator.GetLocalDB()
	localDB.UpdateDatabaseFileName(db.Name, newFileName)

	db.ReInitialize()

	db.viewManager.ReinitializeViews()

	dbPath := db.serviceLocator.GetDBDirPath()
	oldFile := currentFileName + dbExt
	os.Remove(filepath.Join(dbPath, oldFile))

	/*
				1. Copy data (with max update seq) to new data file
				2. Close Writer
				3. Copy remaining data (with min (max from step 1) and max new update seq) to new data file
				4. Close all readers
				5. Close all views
				6. Update localDB with new data file name
				7. Create writer and open it
				8. Create Readers and open it all
				9. Push writer and readers to its corresponding channels
		       10. Delete old data file
	*/

	return nil
}

// SelectView select view
func (db *DefaultDatabase) SelectView(designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	inputDoc := &Document{ID: designDocID}
	outputDoc, err := db.GetDocument(inputDoc, true)
	if err != nil {
		return nil, err
	}

	if !values.Has("limit") {
		values.Set("limit", "10")
	}

	if !values.Has("offset") {
		values.Set("offset", "0")
	}

	return db.viewManager.SelectView(db.UpdateSequence, *outputDoc, viewName, selectName, values, stale)
}

// SQL build sql
func (db *DefaultDatabase) SQL(fromSeqID, designDocID, viewName string) ([]byte, error) {
	inputDoc := &Document{ID: designDocID}
	outputDoc, err := db.GetDocument(inputDoc, true)
	if err != nil {
		return nil, err
	}
	if fromSeqID == db.UpdateSequence {
		return nil, nil
	}
	return db.viewManager.SQL(fromSeqID, *outputDoc, viewName)
}

// ValidateDesignDocument validate design document
func (db *DefaultDatabase) ValidateDesignDocument(doc Document) error {
	return db.viewManager.ValidateDesignDocument(doc)
}

// GetViewManager get a view manager
func (db *DefaultDatabase) GetViewManager() ViewManager {
	return db.viewManager
}

// SetupAllDocsViews setup default views
func (db *DefaultDatabase) SetupAllDocsViews() error {
	doc := `
		{
			"_id" : "_design/_views",
			"views" : {
				"_all_docs" : {
					"setup" : [
						"CREATE TABLE IF NOT EXISTS all_docs (key, rev, doc_id, PRIMARY KEY(doc_id)) WITHOUT ROWID"
					],
					"run" : [
						"DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
						"INSERT OR REPLACE INTO all_docs (key, rev, doc_id) SELECT doc_id, rev, doc_id FROM latest_documents WHERE deleted = 0"
					],
					"select" : {
						"default" : "SELECT JSON_OBJECT('offset', ifnull(min(offset) + 1, 0),'rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', doc_id, 'id', doc_id, 'rev', rev)),'total_rows', (SELECT COUNT(1) FROM all_docs)) as data FROM (SELECT (ROW_NUMBER() OVER(ORDER BY doc_id) - 1) as offset, * FROM all_docs WHERE (${startkey} IS NULL OR doc_id >= ${startkey}) AND (${endkey} IS NULL OR doc_id <= ${endkey}) ORDER BY doc_id LIMIT CAST(${limit} AS INT) OFFSET CAST(${offset} AS INT))",
						"with_docs" : "SELECT JSON_OBJECT('offset', ifnull(min(offset) + 1, 0),'rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', doc_id, 'id', doc_id, 'rev', rev, 'doc', JSON((SELECT data FROM documents WHERE doc_id = o.doc_id)))),'total_rows', (SELECT COUNT(1) FROM all_docs)) as data FROM (SELECT (ROW_NUMBER() OVER(ORDER BY doc_id) - 1) as offset, * FROM all_docs WHERE (${startkey} IS NULL OR doc_id >= ${startkey}) AND (${endkey} IS NULL OR doc_id <= ${endkey}) ORDER BY doc_id LIMIT CAST(${limit} AS INT) OFFSET CAST(${offset} AS INT)) o"						
					}
				}
			}
		}
	`

	designDoc, err := ParseDocument([]byte(doc))
	if err != nil {
		panic(err)
	}

	_, err = db.PutDocument(designDoc)
	if err != nil {
		return err
	}

	return nil
}

func (db *DefaultDatabase) Initialize() error {
	db.writer <- db.serviceLocator.GetDatabaseWriter(db.Name)
	readersCount := cap(db.reader)
	for i := 0; i < readersCount; i++ {
		db.reader <- db.serviceLocator.GetDatabaseReader(db.Name)
	}
	return nil
}

func (db *DefaultDatabase) ReInitialize() error {

	writer := db.serviceLocator.GetDatabaseWriter(db.Name)
	writer.Open(false)

	db.writer <- writer

	readersCount := cap(db.reader)
	for i := 0; i < readersCount; i++ {
		reader := db.serviceLocator.GetDatabaseReader(db.Name)
		if err := reader.Open(); err != nil {
			return err
		}
		db.reader <- reader
	}
	return nil
}

// NewDatabase create database instance
func NewDatabase(name string, createIfNotExists bool, serviceLocator ServiceLocator) Database {
	db := &DefaultDatabase{Name: name}
	db.idSeq = NewSequenceUUIDGenarator()
	db.serviceLocator = serviceLocator

	db.writer = make(chan DatabaseWriter, 1)
	db.reader = make(chan DatabaseReader, 2)
	db.vacuumManager = make(chan VacuumManager, 1)
	db.vacuumManager <- serviceLocator.GetVacuumManager(name)

	db.viewManager = serviceLocator.GetViewManager(name)

	db.Initialize()

	err := db.Open(createIfNotExists)
	if err != nil {
		panic(err)
	}

	return db
}
