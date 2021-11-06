package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/valyala/fastjson"
)

var dbExt = ".db"

// KDB kdb
type KDB struct {
	dbs            map[string]Database
	rwMutex        sync.RWMutex
	serviceLocator ServiceLocator
	fileHandler    FileHandler
	localDB        LocalDB
}

// NewKDB create kdb instance
func NewKDB() (*KDB, error) {
	kdb := new(KDB)
	kdb.dbs = make(map[string]Database)
	kdb.rwMutex = sync.RWMutex{}
	kdb.serviceLocator = NewServiceLocator()

	kdb.localDB = kdb.serviceLocator.GetLocalDB()
	fileHandler := kdb.serviceLocator.GetFileHandler()

	dbPath := kdb.serviceLocator.GetDBDirPath()
	viewPath := kdb.serviceLocator.GetViewDirPath()

	if !fileHandler.IsFileExists(dbPath) {
		if err := fileHandler.MkdirAll(dbPath); err != nil {
			return nil, err
		}
	}

	if !fileHandler.IsFileExists(viewPath) {
		if err := fileHandler.MkdirAll(viewPath); err != nil {
			return nil, err
		}
	}

	if err := kdb.localDB.Open(dbPath); err != nil {
		return nil, err
	}

	list, err := kdb.ListDatabases()
	if err != nil {
		return nil, err
	}

	for idx := range list {
		name := list[idx]
		createIfNotExists := false
		if err = kdb.Open(name, createIfNotExists); err != nil {
			return nil, err
		}
	}

	return kdb, nil
}

// ListDatabases List the databases
func (kdb *KDB) ListDatabases() ([]string, error) {
	return kdb.localDB.ListDatabases()
}

// Open open the kdb database
func (kdb *KDB) Open(name string, createIfNotExists bool) error {
	if !ValidateDatabaseName(name) {
		return ErrDatabaseInvalidName
	}

	kdb.rwMutex.Lock()
	defer kdb.rwMutex.Unlock()

	if _, ok := kdb.dbs[name]; ok && !createIfNotExists {
		return nil
	}

	if createIfNotExists {
		fileName := name + "_" + NewSequenceUUIDGenarator().Next()
		if err := kdb.localDB.CreateDatabase(name, fileName); err != nil {
			if strings.HasPrefix(err.Error(), "sqlite3: constraint failed [1555]") {
				return ErrDatabaseExists
			}
			return err
		}
	}

	if kdb.localDB.GetDatabaseFileName(name) == "" {
		return ErrDatabaseNotFound
	}

	kdb.dbs[name] = kdb.serviceLocator.GetDatabase(name, createIfNotExists)

	return nil
}

// Delete delete the kdb database
func (kdb *KDB) Delete(name string) error {
	kdb.rwMutex.Lock()
	defer kdb.rwMutex.Unlock()

	fileName := kdb.localDB.GetDatabaseFileName(name)
	viewFileNames, _ := kdb.localDB.ListViewFiles(name)

	kdb.localDB.DeleteViews(name)
	kdb.localDB.DeleteDatabase(name)

	db, ok := kdb.dbs[name]
	if !ok {
		return ErrDatabaseNotFound
	}
	delete(kdb.dbs, name)
	db.Close(true)

	kdb.deleteDBFiles(fileName, viewFileNames)

	return nil
}

// PutDocument insert a document
func (kdb *KDB) PutDocument(name string, newDoc *Document) (*Document, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	if !ValidateDocumentID(newDoc.ID) {
		return nil, ErrDocumentInvalidID
	}

	if strings.HasPrefix(newDoc.ID, "_design/") {
		err := db.ValidateDesignDocument(*newDoc)
		if err != nil {
			return nil, err
		}
	}

	return db.PutDocument(newDoc)
}

// DeleteDocument delete a document
func (kdb *KDB) DeleteDocument(name string, doc *Document) (*Document, error) {
	doc.Deleted = true
	return kdb.PutDocument(name, doc)
}

// GetDocument get a document
func (kdb *KDB) GetDocument(name string, doc *Document, includeDoc bool) (*Document, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, errors.New("db_not_found")
	}

	return db.GetDocument(doc, includeDoc)
}

// BulkDocuments insert multiple documents
func (kdb *KDB) BulkDocuments(name string, body []byte) ([]byte, error) {
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", err, ErrBadJSON)
	}
	docs := fValues.GetArray("_docs")
	if docs == nil {
		return nil, fmt.Errorf("%s:%w", "_docs can't be empty", ErrBadJSON)
	}
	outputs, _ := fastjson.ParseBytes([]byte("[]"))
	for idx, item := range fValues.GetArray("_docs") {
		inputDoc, _ := ParseDocument([]byte(item.String()))
		var jsonb []byte
		outputDoc, err := kdb.PutDocument(name, inputDoc)
		if err != nil {
			code, reason := errorString(err)
			jsonb = []byte(fmt.Sprintf(`{"error":"%s","reason":"%s"}`, code, reason))
		} else {
			jsonb = []byte(formatDocumentString(outputDoc.ID, outputDoc.Version, outputDoc.Hash, outputDoc.Deleted))
		}
		v := fastjson.MustParse(string(jsonb))
		outputs.SetArrayItem(idx, v)
	}
	return []byte(outputs.String()), nil
}

// BulkGetDocuments get multiple documents
func (kdb *KDB) BulkGetDocuments(name string, body []byte) ([]byte, error) {
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", err, ErrBadJSON)
	}
	docs := fValues.GetArray("_docs")
	if docs == nil {
		return nil, fmt.Errorf("%s:%w", "_docs can't be empty", ErrDocumentInvalidInput)
	}

	outputs, _ := fastjson.ParseBytes([]byte("[]"))
	for idx, item := range fValues.GetArray("_docs") {
		var jsonb []byte
		var outputDoc *Document

		inputDoc, err := ParseDocument([]byte(item.String()))
		if inputDoc == nil || inputDoc.ID == "" {
			err = fmt.Errorf("%s:%w", "id can't be empty", ErrDocumentInvalidInput)
		}

		if err == nil {
			outputDoc, err = kdb.GetDocument(name, inputDoc, true)
		}

		if err != nil {
			code, reason := errorString(err)
			jsonb = []byte(fmt.Sprintf(`{"_id":"%s", "error":"%s","reason":"%s"}`, inputDoc.ID, code, reason))
		} else {
			jsonb = outputDoc.Data
		}

		v := fastjson.MustParse(string(jsonb))
		outputs.SetArrayItem(idx, v)
	}
	return []byte(outputs.String()), nil
}

// DBStat kdb stat
func (kdb *KDB) DBStat(name string) (*DatabaseStat, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	return db.GetStat(), nil
}

// Vacuum vacuum
func (kdb *KDB) Vacuum(name string) error {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return ErrDatabaseNotFound
	}
	return db.Vacuum()
}

// Changes list changes
func (kdb *KDB) Changes(name string, since string, limit int) ([]byte, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	if limit == 0 {
		limit = 10000
	}
	return db.GetChanges(since, limit)
}

// SelectView select the kdb view
func (kdb *KDB) SelectView(dbName, designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	rs, err := db.SelectView(designDocID, viewName, selectName, values, stale)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// SQL build sql the kdb view
func (kdb *KDB) SQL(dbName, designDocID, viewName, fromSeqID string) ([]byte, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	rs, err := db.SQL(fromSeqID, designDocID, viewName)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// Info get kdb info
func (kdb *KDB) Info() []byte {
	var version string
	conn, _ := sqlite3.Open(":memory:")
	defer conn.Close()
	stmt, _ := conn.Prepare("SELECT sqlite_version()")
	stmt.Step()
	stmt.Scan(&version)
	return []byte(fmt.Sprintf(`{"name":"kdb","version":{"sqlite":"%s"}}`, version))
}

func (kdb *KDB) deleteDBFiles(dbname string, viewFiles []string) {
	dbPath := kdb.serviceLocator.GetDBDirPath()
	viewPath := kdb.serviceLocator.GetViewDirPath()

	for _, vf := range viewFiles {
		os.Remove(filepath.Join(viewPath, vf+dbExt))
	}
	fileName := dbname + dbExt
	os.Remove(filepath.Join(dbPath, fileName))
}

// ValidateDatabaseName validate correctness of the name
func ValidateDatabaseName(name string) bool {
	re := regexp.MustCompile(`^([a-z]+([0-9-]+)?)$`)
	if len(name) <= 0 || !re.Match([]byte(name)) || len(name) > 50 {
		return false
	}
	return true
}

// ValidateDocumentID validate correctness of the document id
func ValidateDocumentID(id string) bool {
	id = strings.Trim(id, " ")
	re := regexp.MustCompile(`^([a-z0-9-]*)$`)

	if !strings.HasPrefix(id, "_design/") {
		if len(id) > 50 || !re.Match([]byte(id)) {
			return false
		}
	}

	return true
}
