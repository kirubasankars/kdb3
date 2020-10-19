package main

import (
	"sync"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

// LocalDB interface
type LocalDB interface {
	Open(dbPath string) error
	Close() error

	CreateDatabase(name, filename string) error
	DeleteDatabase(name string) error
	GetDatabaseFileName(name string) string
	ListDatabases() ([]string, error)
	UpdateDatabaseFileName(name string, fileName string)

	UpdateView(dbname, name, hash, filename string) error
	GetViewFileName(dbname, name string) (string, string)
	DeleteViews(dbname string) error
	DeleteView(dbname, name string) error
	ListViewFiles(dbname string) ([]string, error)
}

// DefaultLocalDB Default implementatio of LocalDB
type DefaultLocalDB struct {
	con *sqlite3.Conn
	mux *sync.RWMutex
}

// Open localDB
func (db *DefaultLocalDB) Open(dbPath string) error {
	con, err := sqlite3.Open(dbPath + "/_local.db")
	if err != nil {
		return err
	}

	con.Begin()

	con.Exec(`
		CREATE TABLE IF NOT EXISTS dbs (name TEXT, filename TEXT, PRIMARY KEY(name));
		CREATE TABLE IF NOT EXISTS views (db TEXT, name TEXT, hash TEXT, filename TEXT, PRIMARY KEY(name, db));
		CREATE UNIQUE INDEX IF NOT EXISTS idx_filename ON dbs (filename);
	`)

	con.Commit()

	db.con = con

	return nil
}

// Close localDB
func (db *DefaultLocalDB) Close() error {
	return db.con.Close()
}

// CreateDatabase create database
func (db *DefaultLocalDB) CreateDatabase(name, filename string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	err := db.con.Exec("INSERT INTO dbs (name, filename) VALUES(?, ?)", name, filename)
	return err
}

// DeleteDatabase delete database
func (db *DefaultLocalDB) DeleteDatabase(name string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	err := db.con.Exec("DELETE FROM dbs WHERE name = ?", name)
	return err
}

// GetDatabaseFileName get database file name
func (db *DefaultLocalDB) GetDatabaseFileName(name string) string {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var fileName string
	stmt, _ := db.con.Prepare("SELECT filename FROM dbs WHERE name = ?", name)
	stmt.Step()
	stmt.Scan(&fileName)
	stmt.Close()
	return fileName
}

// GetDatabaseFileName get database file name
func (db *DefaultLocalDB) UpdateDatabaseFileName(name string, fileName string) {
	db.mux.RLock()
	defer db.mux.RUnlock()
	db.con.Exec("UPDATE dbs SET filename = ? WHERE name = ?", fileName, name)
}

// ListDatabases list all database names
func (db *DefaultLocalDB) ListDatabases() ([]string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var dbs []string
	stmt, err := db.con.Prepare("SELECT name FROM dbs")
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	hasRows, _ := stmt.Step()
	for hasRows {
		var name string
		stmt.Scan(&name)
		dbs = append(dbs, name)
		hasRows, _ = stmt.Step()
	}
	return dbs, nil
}

// UpdateView update view information
func (db *DefaultLocalDB) UpdateView(dbname, name, hash, filename string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	err := db.con.Exec("INSERT OR REPLACE INTO views (db, name, hash, filename) VALUES(?, ?, ?, ?)", dbname, name, hash, filename)
	return err
}

// DeleteViews delete all views for a databases
func (db *DefaultLocalDB) DeleteViews(dbname string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	err := db.con.Exec("DELETE FROM views WHERE db = ?", dbname)
	return err
}

// DeleteView delete a view
func (db *DefaultLocalDB) DeleteView(dbname, name string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	err := db.con.Exec("DELETE FROM views WHERE db = ? and name = ?", dbname, name)
	return err
}

// GetViewFileName get view file name
func (db *DefaultLocalDB) GetViewFileName(dbname, name string) (string, string) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var hash, fileName string
	stmt, _ := db.con.Prepare("SELECT hash, filename FROM views WHERE db = ? and name = ?", dbname, name)
	defer stmt.Close()

	hasRows, _ := stmt.Step()
	if hasRows {
		stmt.Scan(&hash, &fileName)
	}

	return hash, fileName
}

// ListViewFiles get all view file names
func (db *DefaultLocalDB) ListViewFiles(dbname string) ([]string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var views []string
	stmt, err := db.con.Prepare("SELECT filename FROM views where db = ?", dbname)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	hasRows, _ := stmt.Step()
	for hasRows {
		var name string
		stmt.Scan(&name)
		views = append(views, name)
		hasRows, _ = stmt.Step()
	}
	return views, nil
}

// NewLocalDB create new localDB instance
func NewLocalDB() LocalDB {
	localDB := new(DefaultLocalDB)
	localDB.mux = new(sync.RWMutex)
	return localDB
}
