package main

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/valyala/fastjson"
	"kdb3/sqlite3"
)

var dbExt = ".db"

// KDB kdb
type KDB struct {
	dbs            map[string]Database
	rwMutex        sync.RWMutex
	serviceLocator ServiceLocator
	localDB        LocalDB
}

// NewKDB create kdb instance with default ./data directory
func NewKDB() (*KDB, error) {
	return NewKDBWithDataDir("./data")
}

// NewKDBWithDataDir create kdb instance using the given data directory
func NewKDBWithDataDir(dataDir string) (*KDB, error) {
	kdb := new(KDB)
	kdb.dbs = make(map[string]Database)
	kdb.rwMutex = sync.RWMutex{}
	kdb.serviceLocator = NewServiceLocator(dataDir)

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

	db, err := kdb.serviceLocator.GetDatabase(name, createIfNotExists)
	if err != nil {
		return err
	}
	kdb.dbs[name] = db
	databasesOpen.Set(float64(len(kdb.dbs)))
	if defDB, ok := db.(*DefaultDatabase); ok {
		syncDatabaseStatGauges(defDB)
		syncDBPoolGauges(defDB)
	}

	return nil
}

// Delete delete the kdb database
func (kdb *KDB) Delete(name string) error {
	kdb.rwMutex.Lock()
	db, ok := kdb.dbs[name]
	if !ok {
		kdb.rwMutex.Unlock()
		return ErrDatabaseNotFound
	}
	// Unregister first so concurrent Vacuum/Open cannot start; Close waits on db.mutex
	// so an in-flight Vacuum finishes before files are removed.
	delete(kdb.dbs, name)
	databasesOpen.Set(float64(len(kdb.dbs)))
	clearDatabaseStatGauges(name)
	kdb.rwMutex.Unlock()

	_ = db.Close(true)

	kdb.rwMutex.Lock()
	fileName := kdb.localDB.GetDatabaseFileName(name)
	viewFileNames, _ := kdb.localDB.ListViewFiles(name)
	kdb.localDB.DeleteViews(name)
	kdb.localDB.DeleteDatabase(name)
	kdb.rwMutex.Unlock()

	kdb.deleteDBFiles(fileName, viewFileNames)
	return nil
}

// PutDocument insert a document
func (kdb *KDB) PutDocument(name string, newDoc *Document) (*Document, error) {
	if !ValidateDocumentID(newDoc.ID) {
		return nil, ErrDocumentInvalidID
	}

	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()

	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	if strings.HasPrefix(newDoc.ID, "_design/") && len(newDoc.Data) != 0 {
		if err := db.ValidateDesignDocument(*newDoc); err != nil {
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
	if fValues.GetArray("_docs") == nil {
		return nil, fmt.Errorf("%s:%w", "_docs is missing", ErrDocumentInvalidInput)
	}

	kdb.rwMutex.RLock()
	db, ok := kdb.dbs[name]
	kdb.rwMutex.RUnlock()
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	items := fValues.GetArray("_docs")
	docs := make([]*Document, len(items))
	parseErrs := make([]error, len(items))
	for idx, item := range items {
		raw := item.MarshalTo(nil)
		inputDoc, err := ParseDocument(raw)
		if err != nil {
			parseErrs[idx] = err
			continue
		}
		if !ValidateDocumentID(inputDoc.ID) {
			parseErrs[idx] = ErrDocumentInvalidID
			continue
		}
		if strings.HasPrefix(inputDoc.ID, "_design/") && len(inputDoc.Data) != 0 {
			if err := db.ValidateDesignDocument(*inputDoc); err != nil {
				parseErrs[idx] = err
				continue
			}
		}
		docs[idx] = inputDoc
	}

	defDB, isDefault := db.(*DefaultDatabase)
	var outs []*Document
	var putErrs []error
	if isDefault {
		outs, putErrs = defDB.BulkPutDocuments(docs)
	} else {
		outs = make([]*Document, len(docs))
		putErrs = make([]error, len(docs))
		for i, d := range docs {
			if d == nil {
				continue
			}
			outs[i], putErrs[i] = db.PutDocument(d)
		}
	}

	var b strings.Builder
	b.WriteByte('[')
	for idx := range items {
		if idx > 0 {
			b.WriteByte(',')
		}
		err := parseErrs[idx]
		if err == nil {
			err = putErrs[idx]
		}
		if err != nil {
			id := ""
			if docs[idx] != nil {
				id = docs[idx].ID
			}
			code, reason := errorString(err)
			b.WriteString(fmt.Sprintf(`{"_id":%s,"error":%s,"reason":%s}`, jsonEscapeString(id), jsonEscapeString(code), jsonEscapeString(reason)))
			continue
		}
		out := outs[idx]
		b.WriteString(formatDocumentString(out.ID, out.Version, out.Deleted))
	}
	b.WriteByte(']')
	return []byte(b.String()), nil
}

// BulkGetDocuments get multiple documents
func (kdb *KDB) BulkGetDocuments(name string, body []byte) ([]byte, error) {
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", err, ErrBadJSON)
	}
	if fValues.GetArray("_docs") == nil {
		return nil, fmt.Errorf("%s:%w", "_docs is missing", ErrDocumentInvalidInput)
	}

	var b strings.Builder
	b.WriteByte('[')
	for idx, item := range fValues.GetArray("_docs") {
		if idx > 0 {
			b.WriteByte(',')
		}
		raw := item.MarshalTo(nil)
		inputDoc, err := ParseDocument(raw)
		id := ""
		if inputDoc != nil {
			id = inputDoc.ID
		}
		if inputDoc == nil || inputDoc.ID == "" {
			err = fmt.Errorf("%s:%w", "id is missing", ErrDocumentInvalidInput)
		}
		var outputDoc *Document
		if err == nil {
			outputDoc, err = kdb.GetDocument(name, inputDoc, true)
		}
		if err != nil {
			code, reason := errorString(err)
			b.WriteString(fmt.Sprintf(`{"_id":%s,"error":%s,"reason":%s}`, jsonEscapeString(id), jsonEscapeString(code), jsonEscapeString(reason)))
			continue
		}
		b.Write(outputDoc.Data)
	}
	b.WriteByte(']')
	return []byte(b.String()), nil
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
	db, ok := kdb.dbs[name]
	kdb.rwMutex.RUnlock()
	if !ok {
		return ErrDatabaseNotFound
	}
	return db.Vacuum()
}

// Changes list changes
func (kdb *KDB) Changes(name string, since int64, limit int, desc bool) ([]byte, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	if limit == 0 {
		limit = 1000
	}
	return db.GetChanges(since, limit, desc)
}

// ChangesRows returns typed change rows.
func (kdb *KDB) ChangesRows(name string, since int64, limit int, desc bool) ([]Change, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	if limit == 0 {
		limit = 1000
	}
	return db.GetChangesRows(since, limit, desc)
}

// ChangesNotifyChan returns the DB notify channel for continuous feeds.
func (kdb *KDB) ChangesNotifyChan(name string) (<-chan struct{}, error) {
	kdb.rwMutex.RLock()
	db, ok := kdb.dbs[name]
	kdb.rwMutex.RUnlock()
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	return db.ChangesNotifyChan(), nil
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
func (kdb *KDB) SQL(dbName, designDocID, viewName string, fromSeq int64) ([]byte, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	rs, err := db.SQL(fromSeq, designDocID, viewName)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// DryRunView dry-runs draft view SQL without writing view files.
func (kdb *KDB) DryRunView(dbName, designDocID, viewName string, req ViewAtelierDryRunRequest) (*ViewAtelierDryRunResult, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	_ = designDocID
	return db.DryRunView(viewName, req)
}

// GetViewStatus returns view catch-up status for the atelier lag badge.
func (kdb *KDB) GetViewStatus(dbName, designDocID, viewName string) (*ViewStatus, error) {
	kdb.rwMutex.RLock()
	defer kdb.rwMutex.RUnlock()
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, ErrDatabaseNotFound
	}
	return db.GetViewStatus(designDocID, viewName)
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
		removeSQLiteFiles(filepath.Join(viewPath, vf+dbExt))
	}
	removeSQLiteFiles(filepath.Join(dbPath, dbname+dbExt))
}

var (
	dbNameRe = regexp.MustCompile(`^([a-z0-9_]+)$`)
	docIDRe  = regexp.MustCompile(`^([A-Za-z0-9_]+)$`)
)

// ValidateDatabaseName validate correctness of the name
func ValidateDatabaseName(name string) bool {
	if len(name) == 0 || len(name) > 50 || name[0] == '_' || !dbNameRe.MatchString(name) {
		return false
	}
	return true
}

// ValidateDocumentID validate correctness of the document id
func ValidateDocumentID(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return true // server may assign
	}
	if strings.HasPrefix(id, "_design/") {
		rest := strings.TrimPrefix(id, "_design/")
		return len(rest) > 0 && len(rest) <= 50 && docIDRe.MatchString(rest)
	}
	if len(id) > 50 || id[0] == '_' || !docIDRe.MatchString(id) {
		return false
	}
	return true
}
