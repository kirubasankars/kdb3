package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var dbExt = ".db"

type KDB struct {
	dbPath   string
	viewPath string

	dbs map[string]*Database
}

func NewKDB() (*KDB, error) {
	kdb := new(KDB)

	kdb.dbs = make(map[string]*Database)
	kdb.dbPath = "./data"
	kdb.viewPath = "./views"

	if _, err := os.Stat(kdb.dbPath); os.IsNotExist(err) {
		if err = os.Mkdir(kdb.dbPath, 0755); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(kdb.viewPath); os.IsNotExist(err) {
		if err = os.Mkdir(kdb.viewPath, 0755); err != nil {
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

func (kdb *KDB) ListDataBases() ([]string, error) {

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
	if len(name) <= 0 {
		return false
	}
	return true
}

func (kdb *KDB) Open(name string, createIfNotExists bool) error {
	if !validatename(name) {
		return errors.New("invalid_db_name")
	}

	db := NewDatabase(kdb.dbPath, kdb.viewPath)

	err := db.Open(name, createIfNotExists)
	if err != nil {
		return err
	}
	kdb.dbs[name] = db

	return nil
}

func (kdb *KDB) Close(name string) {
	kdb.dbs[name].Close()
}

func (kdb *KDB) Delete(name string) error {
	db, ok := kdb.dbs[name]
	if !ok {
		return errors.New("db_not_found")
	}

	kdb.Close(name)

	delete(kdb.dbs, name)

	for _, x := range db.views {
		os.Remove(filepath.Join(kdb.viewPath, x.fileName))
	}

	return os.Remove(filepath.Join(kdb.dbPath, name+dbExt))
}

func (kdb *KDB) PutDocument(name string, newDoc *Document) error {

	db, ok := kdb.dbs[name]
	if !ok {
		return errors.New("db_not_found")
	}

	return db.PutDocument(newDoc)
}

func (kdb *KDB) GetDocument(name string, newDoc *Document, includeDoc bool) error {

	db, ok := kdb.dbs[name]
	if !ok {
		return errors.New("db_not_found")
	}

	return db.GetDocument(newDoc, includeDoc)
}

func (kdb *KDB) SelectView(dbName, designDocID, viewName, selectName string, values url.Values, stale bool) ([]byte, error) {
	fmt.Println(dbName, designDocID, viewName, selectName)
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

func (kdb *KDB) DeleteDocument(name string, newDoc *Document) error {
	newDoc.deleted = true
	return kdb.PutDocument(name, newDoc)
}

func (kdb *KDB) DBStat(name string) (*DBStat, error) {
	db, ok := kdb.dbs[name]
	if !ok {
		return nil, errors.New("db_not_found")
	}
	return db.Stat(), nil
}

func (kdb *KDB) Vacuum(name string) error {
	db, ok := kdb.dbs[name]
	if !ok {
		return errors.New("db_not_found")
	}
	for _, x := range db.views {
		err := x.Vacuum()
		if err != nil {
			return err
		}
	}
	return db.Vacuum()
}
