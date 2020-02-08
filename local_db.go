package main

import (
	"database/sql"
)

type LocalDB interface {
	Open(dbPath string) error
	Close() error
	Begin() error
	Commit() error
	Rollback() error

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

type DefaultLocalDB struct {
	con *sql.DB
	tx  *sql.Tx
}

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

func (db *DefaultLocalDB) Close() error {
	return db.con.Close()
}

func (db *DefaultLocalDB) Begin() error {
	var err error
	db.tx, err = db.con.Begin()
	return err
}

func (db *DefaultLocalDB) Commit() error {
	return db.tx.Commit()
}

func (db *DefaultLocalDB) Rollback() error {
	return db.tx.Rollback()
}

func (db *DefaultLocalDB) CreateDatabase(name, filename string) error {
	_, err := db.tx.Exec("INSERT INTO dbs (name, filename) VALUES(?, ?)", name, filename)
	return err
}

func (db *DefaultLocalDB) DeleteDatabase(name string) error {
	_, err := db.tx.Exec("DELETE FROM dbs WHERE name = ?", name)
	return err
}

func (db *DefaultLocalDB) GetDatabaseFileName(name string) string {
	var fileName string
	row := db.tx.QueryRow("SELECT filename FROM dbs WHERE name = ?", name)
	row.Scan(&fileName)
	return fileName
}

func (db *DefaultLocalDB) ListDatabases() ([]string, error) {
	var dbs []string
	rows, err := db.tx.Query("SELECT name FROM dbs")
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

func (db *DefaultLocalDB) UpdateView(dbname, name, hash, filename string) error {
	_, err := db.tx.Exec("INSERT OR REPLACE INTO views (db, name, hash, filename) VALUES(?, ?, ?, ?)", dbname, name, hash, filename)
	return err
}

func (db *DefaultLocalDB) DeleteViews(dbname string) error {
	_, err := db.tx.Exec("DELETE FROM views WHERE db = ?", dbname)
	return err
}

func (db *DefaultLocalDB) DeleteView(dbname, name string) error {
	_, err := db.tx.Exec("DELETE FROM views WHERE db = ? and name = ?", dbname, name)
	return err
}

func (db *DefaultLocalDB) GetViewFileName(dbname, name string) (string, string) {
	var hash, fileName string
	row := db.tx.QueryRow("SELECT hash, filename FROM views WHERE db = ? and name = ?", dbname, name)
	row.Scan(&hash, &fileName)
	return hash, fileName
}

func (db *DefaultLocalDB) ListViewFiles(dbname string) ([]string, error) {
	var views []string
	rows, err := db.tx.Query("SELECT filename FROM views where db = ? ", dbname)
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
