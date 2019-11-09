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
)

var dbExt = ".db"

type KDBEngine struct {
	dbPath   string
	viewPath string

	dbs         map[string]*Database
	rwmux       sync.RWMutex
	fileHandler FileHandler
}

func NewKDB() (*KDBEngine, error) {
	kdb := new(KDBEngine)
	kdb.dbs = make(map[string]*Database)
	kdb.rwmux = sync.RWMutex{}
	kdb.dbPath = "./data/dbs"
	kdb.viewPath = "./data/mrviews"
	kdb.fileHandler = new(DefaultFileHandler)

	if !kdb.fileHandler.IsFileExists(kdb.dbPath) {
		if err := kdb.fileHandler.MkdirAll(kdb.dbPath); err != nil {
			return nil, err
		}
	}

	if !kdb.fileHandler.IsFileExists(kdb.dbPath) {
		if err := kdb.fileHandler.MkdirAll(kdb.dbPath); err != nil {
			return nil, err
		}
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
	list, err := ioutil.ReadDir(kdb.dbPath)
	if err != nil {
		return nil, err
	}
	var dbs []string
	for idx := range list {
		name := list[idx].Name()
		if strings.HasSuffix(name, dbExt) {
			dbs = append(dbs, strings.ReplaceAll(name, dbExt, ""))
		}
	}
	return dbs, nil
}

func ValidateDBName(name string) bool {
	if len(name) <= 0 || strings.Contains(name, "$") || name[0] == '_' {
		return false
	}
	return true
}

func ValidateDocId(id string) bool {
	id = strings.Trim(id, " ")
	if len(id) > 0 && !strings.HasPrefix(id, "_design/") && id[0] == '_' {
		return false
	}
	return true
}

func (kdb *KDBEngine) Open(name string, createIfNotExists bool) error {
	if !ValidateDBName(name) {
		return ErrDBInvalidName
	}

	kdb.rwmux.Lock()
	defer kdb.rwmux.Unlock()

	if _, ok := kdb.dbs[name]; ok && !createIfNotExists {
		return nil
	}

	db, err := NewDatabase(name, kdb.dbPath, kdb.viewPath, createIfNotExists)
	if err != nil {
		return err
	}
	kdb.dbs[name] = db

	return nil
}

func (kdb *KDBEngine) Delete(name string) error {
	kdb.rwmux.Lock()
	defer kdb.rwmux.Unlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return ErrDBNotFound
	}

	delete(kdb.dbs, name)
	db.Close()

	list, err := ioutil.ReadDir(kdb.viewPath)
	if err != nil {
		return err
	}

	for idx := range list {
		name := list[idx].Name()
		if strings.HasPrefix(name, db.name+"$") && strings.HasSuffix(name, dbExt) {
			os.Remove(filepath.Join(kdb.viewPath, name))
		}
	}

	fileName := name + dbExt
	os.Remove(filepath.Join(kdb.dbPath, fileName+"-shm"))
	os.Remove(filepath.Join(kdb.dbPath, fileName+"-wal"))
	os.Remove(filepath.Join(kdb.dbPath, name+dbExt))

	return nil
}

func (kdb *KDBEngine) PutDocument(name string, newDoc *Document) (*Document, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDBNotFound
	}
	if !ValidateDocId(newDoc.ID) {
		return nil, ErrDocInvalidID
	}

	if strings.HasPrefix(newDoc.ID, "_design/") {
		err := db.GetViewManager().ValidateDDoc(newDoc)
		if err != nil {
			return nil, err
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

func (kdb *KDBEngine) DBStat(name string) (*DBStat, error) {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, ErrDBNotFound
	}
	return db.Stat(), nil
}

func (kdb *KDBEngine) Vacuum(name string) error {
	kdb.rwmux.RLock()
	defer kdb.rwmux.RUnlock()
	db, ok := kdb.dbs[name]
	if !ok {
		return ErrDBNotFound
	}

	db.viewmgr.Vacuum()
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
	var version, sqlite_source_id string
	con, _ := sql.Open("sqlite3", ":memory:")
	row := con.QueryRow("SELECT sqlite_version(), sqlite_source_id()")
	row.Scan(&version, &sqlite_source_id)
	con.Close()
	return []byte(fmt.Sprintf(`{"name":"kdb", "version":{"sqlite_version":"%s", "sqlite_source_id":"%s"}}`, version, sqlite_source_id))
}
