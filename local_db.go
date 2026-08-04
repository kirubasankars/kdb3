package main

import (
	"sync"

	"kdb3/sqlite3"
)

// LocalDB interface
type LocalDB interface {
	Open(dbPath string) error
	Close() error

	CreateDatabase(name, filename string) error
	DeleteDatabase(name string) error
	GetDatabaseFileName(name string) string
	ListDatabases() ([]string, error)
	UpdateDatabaseFileName(name string, fileName string) error

	UpdateView(dbname, name, hash, filename string) error
	GetViewFileName(dbname, name string) (string, string)
	DeleteViews(dbname string) error
	DeleteView(dbname, name string) error
	ListViewFiles(dbname string) ([]string, error)
}

// DefaultLocalDB Default implementatio of LocalDB
type DefaultLocalDB struct {
	con *sqlite3.Conn
	// mux serializes all use of con. SQLite multi-thread mode still requires
	// exclusive access to a single connection (RLock is not safe here).
	mux sync.Mutex
}

// Open localDB
func (db *DefaultLocalDB) Open(dbPath string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	con, err := sqlite3.Open(dbPath + "/_local.db")
	if err != nil {
		return err
	}
	db.con = con
	_ = con.Exec("PRAGMA busy_timeout=5000;")

	err = con.WithTx(func() error {
		return con.Exec(`
			CREATE TABLE IF NOT EXISTS dbs (name TEXT, filename TEXT, PRIMARY KEY(name));
			CREATE TABLE IF NOT EXISTS views (db TEXT, name TEXT, hash TEXT, filename TEXT, PRIMARY KEY(name, db));
			CREATE UNIQUE INDEX IF NOT EXISTS idx_filename ON dbs (filename);
		`)
	})

	return err
}

// Close localDB
func (db *DefaultLocalDB) Close() error {
	db.mux.Lock()
	defer db.mux.Unlock()
	if db.con == nil {
		return nil
	}
	err := db.con.Close()
	db.con = nil
	return err
}

// CreateDatabase create database
func (db *DefaultLocalDB) CreateDatabase(name, filename string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.con.Exec("INSERT INTO dbs (name, filename) VALUES(?, ?)", name, filename)
}

// DeleteDatabase delete database
func (db *DefaultLocalDB) DeleteDatabase(name string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.con.Exec("DELETE FROM dbs WHERE name = ?", name)
}

// GetDatabaseFileName get database file name
func (db *DefaultLocalDB) GetDatabaseFileName(name string) string {
	db.mux.Lock()
	defer db.mux.Unlock()

	stmt, err := db.con.Prepare("SELECT filename FROM dbs WHERE name = ?", name)
	if err != nil {
		return ""
	}
	defer stmt.Close()

	_, _ = stmt.Step()
	var fileName string
	_ = stmt.Scan(&fileName)
	return fileName
}

// UpdateDatabaseFileName update database file name
func (db *DefaultLocalDB) UpdateDatabaseFileName(name string, fileName string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.con.Exec("UPDATE dbs SET filename = ? WHERE name = ?", fileName, name)
}

// ListDatabases list all database names
func (db *DefaultLocalDB) ListDatabases() ([]string, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dbs []string
	stmt, err := db.con.Prepare("SELECT name FROM dbs")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	hasRows, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	for hasRows {
		var name string
		_ = stmt.Scan(&name)
		dbs = append(dbs, name)

		hasRows, err = stmt.Step()
		if err != nil {
			return nil, err
		}
	}

	return dbs, nil
}

// UpdateView update view information
func (db *DefaultLocalDB) UpdateView(dbname, name, hash, filename string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.con.Exec("INSERT OR REPLACE INTO views (db, name, hash, filename) VALUES(?, ?, ?, ?)", dbname, name, hash, filename)
}

// DeleteViews delete all views for a databases
func (db *DefaultLocalDB) DeleteViews(dbname string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.con.Exec("DELETE FROM views WHERE db = ?", dbname)
}

// DeleteView delete a view
func (db *DefaultLocalDB) DeleteView(dbname, name string) error {
	db.mux.Lock()
	defer db.mux.Unlock()
	return db.con.Exec("DELETE FROM views WHERE db = ? and name = ?", dbname, name)
}

// GetViewFileName get view file name
func (db *DefaultLocalDB) GetViewFileName(dbname, name string) (string, string) {
	db.mux.Lock()
	defer db.mux.Unlock()

	stmt, err := db.con.Prepare("SELECT hash, filename FROM views WHERE db = ? and name = ?", dbname, name)
	if err != nil {
		return "", ""
	}
	defer stmt.Close()

	hasRows, _ := stmt.Step()
	var hash, fileName string
	if hasRows {
		_ = stmt.Scan(&hash, &fileName)
	}

	return hash, fileName
}

// ListViewFiles get all view file names
func (db *DefaultLocalDB) ListViewFiles(dbname string) ([]string, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	stmt, err := db.con.Prepare("SELECT filename FROM views where db = ?", dbname)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var views []string
	hasRows, _ := stmt.Step()
	for hasRows {
		var name string
		_ = stmt.Scan(&name)
		views = append(views, name)
		hasRows, _ = stmt.Step()
	}
	return views, nil
}

// NewLocalDB create new localDB instance
func NewLocalDB() LocalDB {
	return new(DefaultLocalDB)
}
