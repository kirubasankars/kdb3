package main

import (
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var dbExt = ".db"

type KDBEngine struct {
	dbPath   string
	viewPath string

	dbs map[string]*Database
}

func NewKDB() (*KDBEngine, error) {
	kdb := new(KDBEngine)

	kdb.dbs = make(map[string]*Database)
	kdb.dbPath = "./data/dbs"
	kdb.viewPath = "./data/mrviews"

	if _, err := os.Stat(kdb.dbPath); os.IsNotExist(err) {
		if err = os.MkdirAll(kdb.dbPath, 0755); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(kdb.viewPath); os.IsNotExist(err) {
		if err = os.MkdirAll(kdb.viewPath, 0755); err != nil {
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

func validatename(name string) bool {
	if len(name) <= 0 || strings.Contains(name, "$") {
		return false
	}
	return true
}

func (kdb *KDBEngine) Open(name string, createIfNotExists bool) error {
	if !validatename(name) {
		return errors.New("invalid_db_name")
	}
	db := NewDatabase(name, kdb.dbPath, kdb.viewPath)
	err := db.Open(createIfNotExists)
	if err != nil {
		return err
	}
	kdb.dbs[name] = db

	return nil
}

func (kdb *KDBEngine) Delete(name string) error {
	db, ok := kdb.dbs[name]
	if !ok {
		return errors.New("db_not_found")
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

	os.Remove(filepath.Join(kdb.dbPath, name+".db-shm"))
	os.Remove(filepath.Join(kdb.dbPath, name+".db-wal"))
	os.Remove(filepath.Join(kdb.dbPath, name+dbExt))

	return nil
}

func (kdb *KDBEngine) PutDocument(name string, newDoc *Document) (*Document, error) {
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, errors.New("db_not_found")
	}
	return db.PutDocument(newDoc)
}

func (kdb *KDBEngine) DeleteDocument(name string, doc *Document) (*Document, error) {
	doc.Deleted = true
	return kdb.PutDocument(name, doc)
}

func (kdb *KDBEngine) GetDocument(name string, doc *Document, includeDoc bool) (*Document, error) {

	db, ok := kdb.dbs[name]
	if !ok {
		return nil, errors.New("db_not_found")
	}

	return db.GetDocument(&doc.Revision, includeDoc)
}

func (kdb *KDBEngine) DBStat(name string) (*DBStat, error) {
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, errors.New("db_not_found")
	}
	return db.Stat(), nil
}

func (kdb *KDBEngine) Vacuum(name string) error {
	db, ok := kdb.dbs[name]
	if !ok {
		return errors.New("db_not_found")
	}

	db.viewmgr.VacuumViews()
	return db.Vacuum()
}

func (kdb *KDBEngine) SelectView(dbName, designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	db, ok := kdb.dbs[dbName]
	if !ok {
		return nil, errors.New("db_not_found")
	}

	rs, err := db.SelectView(designDocID, viewName, selectName, values, stale)
	if err != nil {
		return nil, err
	}

	return rs, nil
}
