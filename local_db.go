package main

import (
	"database/sql"
	"sync"
)

// LocalDB interface
type LocalDB interface {
	Open(dbPath string) error
	Close() error

	CreateDatabase(name, filename string) error
	DeleteDatabase(name string) error
	GetDatabaseFileName(name string) string
	ListDatabases() ([]string, error)

	UpdateView(dbname, name, hash, filename string) error
	GetViewFileName(dbname, name string) (string, string)
	DeleteViews(dbname string) error
	DeleteView(dbname, name string) error
	ListViewFiles(dbname string) ([]string, error)
}

// DefaultLocalDB Default implementatio of LocalDB
type DefaultLocalDB struct {
	con *sql.DB
	mux *sync.RWMutex
}

// Open localDB
func (db *DefaultLocalDB) Open(dbPath string) error {
	con, err := sql.Open("sqlite3", dbPath+"/_local.db")
	if err != nil {
		return err
	}

	tx, _ := con.Begin()

	tx.Exec(`
		CREATE TABLE IF NOT EXISTS dbs (name TEXT, filename TEXT, PRIMARY KEY(name));
		CREATE TABLE IF NOT EXISTS views (db TEXT, name TEXT, hash TEXT, filename TEXT, PRIMARY KEY(name, db));
		CREATE UNIQUE INDEX IF NOT EXISTS idx_filename ON dbs (filename);
	`)

	tx.Commit()

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
	_, err := db.con.Exec("INSERT INTO dbs (name, filename) VALUES(?, ?)", name, filename)
	return err
}

// DeleteDatabase delete database
func (db *DefaultLocalDB) DeleteDatabase(name string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	_, err := db.con.Exec("DELETE FROM dbs WHERE name = ?", name)
	return err
}

// GetDatabaseFileName get database file name
func (db *DefaultLocalDB) GetDatabaseFileName(name string) string {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var fileName string
	row := db.con.QueryRow("SELECT filename FROM dbs WHERE name = ?", name)
	row.Scan(&fileName)
	return fileName
}

// ListDatabases list all database names
func (db *DefaultLocalDB) ListDatabases() ([]string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var dbs []string
	rows, err := db.con.Query("SELECT name FROM dbs")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		dbs = append(dbs, name)
	}
	return dbs, nil
}

// UpdateView update view information
func (db *DefaultLocalDB) UpdateView(dbname, name, hash, filename string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	_, err := db.con.Exec("INSERT OR REPLACE INTO views (db, name, hash, filename) VALUES(?, ?, ?, ?)", dbname, name, hash, filename)
	return err
}

// DeleteViews delete all views for a databases
func (db *DefaultLocalDB) DeleteViews(dbname string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	_, err := db.con.Exec("DELETE FROM views WHERE db = ?", dbname)
	return err
}

// DeleteView delete a view
func (db *DefaultLocalDB) DeleteView(dbname, name string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	_, err := db.con.Exec("DELETE FROM views WHERE db = ? and name = ?", dbname, name)
	return err
}

// GetViewFileName get view file name
func (db *DefaultLocalDB) GetViewFileName(dbname, name string) (string, string) {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var hash, fileName string
	row := db.con.QueryRow("SELECT hash, filename FROM views WHERE db = ? and name = ?", dbname, name)
	row.Scan(&hash, &fileName)
	return hash, fileName
}

// ListViewFiles get all view file names
func (db *DefaultLocalDB) ListViewFiles(dbname string) ([]string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var views []string
	rows, err := db.con.Query("SELECT filename FROM views where db = ? ", dbname)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		views = append(views, name)
	}
	return views, nil
}

// NewLocalDB create new localDB instance
func NewLocalDB() LocalDB {
	localDB := new(DefaultLocalDB)
	localDB.mux = new(sync.RWMutex)
	return localDB
}
