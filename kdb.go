package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/valyala/fastjson"
)

var dbExt = ".db"

type KDBEngine struct {
	dbPath   string
	viewPath string

	dbs            map[string]*Database
	rwmux          sync.RWMutex
	serviceLocator ServiceLocator
	fileHandler    FileHandler
	localDB        *LocalDB
}

func NewKDB() (*KDBEngine, error) {
	kdb := new(KDBEngine)
	kdb.dbs = make(map[string]*Database)
	kdb.rwmux = sync.RWMutex{}
	kdb.dbPath = "./data/dbs"
	kdb.viewPath = "./data/mrviews"
	kdb.serviceLocator = NewServiceLocator()
	kdb.localDB = &LocalDB{}

	fileHandler := kdb.serviceLocator.GetFileHandler()

	if !fileHandler.IsFileExists(kdb.dbPath) {
		if err := fileHandler.MkdirAll(kdb.dbPath); err != nil {
			return nil, err
		}
	}

	if !fileHandler.IsFileExists(kdb.viewPath) {
		if err := fileHandler.MkdirAll(kdb.viewPath); err != nil {
			return nil, err
		}
	}

	if err := kdb.localDB.Open(kdb.dbPath); err != nil {
		return nil, err
	}

	list, err := kdb.ListDataBases()
	if err != nil {
		return nil, err
	}

	for idx := range list {
		name := list[idx]
		if err = kdb.Open(name, false); err != nil {
			return nil, err
		}
	}

	return kdb, nil
}

func (kdb *KDBEngine) ListDataBases() ([]string, error) {
	kdb.localDB.Begin()
	defer kdb.localDB.Commit()
	return kdb.localDB.List()
}

func (kdb *KDBEngine) Open(name string, createIfNotExists bool) error {
	if !validateDBName(name) {
		return ErrDBInvalidName
	}

	kdb.rwmux.Lock()
	defer kdb.rwmux.Unlock()

	if _, ok := kdb.dbs[name]; ok && !createIfNotExists {
		return nil
	}

	kdb.localDB.Begin()
	defer kdb.localDB.Rollback()

	fileName := name + "-1"

	if createIfNotExists {
		if err := kdb.localDB.Create(name, fileName); err != nil {
			if strings.HasPrefix(err.Error(), "UNIQUE constraint failed") {
				return ErrDBExists
			}
			return err
		}
	} else {
		fileName = kdb.localDB.GetFileName(name)
	}

	db, err := NewDatabase(name, fileName, kdb.dbPath, kdb.viewPath, createIfNotExists, kdb.serviceLocator)
	if err != nil {
		return err
	}

	kdb.dbs[name] = db

	kdb.localDB.Commit()

	return nil
}

func (kdb *KDBEngine) Delete(name string) error {
	kdb.rwmux.Lock()
	defer kdb.rwmux.Unlock()

	kdb.localDB.Begin()
	defer kdb.localDB.Rollback()
	fileName := kdb.localDB.GetFileName(name)
	kdb.localDB.Delete(name)

	db, ok := kdb.dbs[name]
	if !ok {
		return ErrDBNotFound
	}

	delete(kdb.dbs, name)
	db.Close()

	deleteDBFiles(kdb.dbPath, kdb.viewPath, fileName)

	kdb.localDB.Commit()

	return nil
}

func (kdb *KDBEngine) PutDocument(name string, newDoc *Document) (*Document, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDBNotFound
	}
	if !validateDocID(newDoc.ID) {
		return nil, ErrDocInvalidID
	}

	if strings.HasPrefix(newDoc.ID, "_design/") {
		newDoc.Kind = "design"
		err := db.ValidateDesignDocument(newDoc)
		if err != nil {
			return nil, err
		}
		if newDoc.Deleted {
			db.viewManager.UpdateDesignDocument(newDoc)
		}
	}

	return db.PutDocument(newDoc)
}

func (kdb *KDBEngine) DeleteDocument(name string, doc *Document) (*Document, error) {
	doc.Deleted = true
	return kdb.PutDocument(name, doc)
}

func (kdb *KDBEngine) GetDocument(name string, doc *Document, includeDoc bool) (*Document, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, errors.New("db_not_found")
	}

	return db.GetDocument(doc, includeDoc)
}

func (kdb *KDBEngine) BulkDocuments(name string, body []byte) ([]byte, error) {
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", err, ErrBadJSON)
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
			jsonb = []byte(formatDocString(outputDoc.ID, outputDoc.Version, outputDoc.Deleted))
		}
		v := fastjson.MustParse(string(jsonb))
		outputs.SetArrayItem(idx, v)
	}
	return []byte(outputs.String()), nil
}

func (kdb *KDBEngine) BulkGetDocuments(name string, body []byte) ([]byte, error) {
	fValues, err := fastjson.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", err, ErrBadJSON)
	}
	outputs, _ := fastjson.ParseBytes([]byte("[]"))
	for idx, item := range fValues.GetArray("_docs") {
		inputDoc, _ := ParseDocument([]byte(item.String()))
		var jsonb []byte
		outputDoc, err := kdb.GetDocument(name, inputDoc, true)
		if err != nil {
			code, reason := errorString(err)
			jsonb = []byte(fmt.Sprintf(`{"error":"%s","reason":"%s"}`, code, reason))
		} else {
			jsonb = outputDoc.Data
		}
		v := fastjson.MustParse(string(jsonb))
		outputs.SetArrayItem(idx, v)
	}
	return []byte(outputs.String()), nil
}

func (kdb *KDBEngine) DBStat(name string) (*DBStat, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDBNotFound
	}
	return db.GetStat(), nil
}

func (kdb *KDBEngine) Vacuum(name string) error {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return ErrDBNotFound
	}

	db.viewManager.Vacuum()
	return db.Vacuum()
}

func (kdb *KDBEngine) Changes(name string, since string, limit int) ([]byte, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDBNotFound
	}
	if limit == 0 {
		limit = 10000
	}
	return db.GetChanges(since, limit)
}

func (kdb *KDBEngine) SelectView(dbName, designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, ErrDBNotFound
	}

	rs, err := db.SelectView(designDocID, viewName, selectName, values, stale)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (kdb *KDBEngine) Info() []byte {
	var version, sqliteSourceID string
	con, _ := sql.Open("sqlite3", ":memory:")
	row := con.QueryRow("SELECT sqlite_version(), sqlite_source_id()")
	row.Scan(&version, &sqliteSourceID)
	con.Close()
	return []byte(fmt.Sprintf(`{"name":"kdb","version":{"sqlite_version":"%s","sqlite_source_id":"%s"}}`, version, sqliteSourceID))
}

func deleteDBFiles(dbPath, viewPath, dbname string) {
	list, _ := ioutil.ReadDir(viewPath)
	for idx := range list {
		name := list[idx].Name()
		if strings.HasPrefix(name, dbname+"$") && strings.HasSuffix(name, dbExt) {
			os.Remove(filepath.Join(viewPath, name))
		}
	}

	fileName := dbname + dbExt
	os.Remove(filepath.Join(dbPath, fileName+"-shm"))
	os.Remove(filepath.Join(dbPath, fileName+"-wal"))
	os.Remove(filepath.Join(dbPath, fileName))
}

func validateDBName(name string) bool {
	if len(name) <= 0 || strings.Contains(name, "$") || name[0] == '_' {
		return false
	}
	return true
}

func validateDocID(id string) bool {
	id = strings.Trim(id, " ")
	if len(id) > 0 && !strings.HasPrefix(id, "_design/") && id[0] == '_' {
		return false
	}
	return true
}
