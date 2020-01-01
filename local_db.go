package main

import (
	"database/sql"
)

type LocalDB struct {
	con *sql.DB
	tx  *sql.Tx
}

func (db *LocalDB) Open(dbPath string) error {
	con, err := sql.Open("sqlite3", dbPath+"/_local.db")
	if err != nil {
		return err
	}

	tx, _ := con.Begin()

	tx.Exec(`
		CREATE TABLE IF NOT EXISTS dbs (name TEXT, filename TEXT, row_count INT, PRIMARY KEY(name));
		CREATE UNIQUE INDEX IF NOT EXISTS idx_filename (filename);
	`)

	tx.Commit()

	db.con = con

	return nil
}

func (db *LocalDB) Close() error {
	return db.con.Close()
}

func (db *LocalDB) Begin() error {
	var err error
	db.tx, err = db.con.Begin()
	return err
}

func (db *LocalDB) Commit() error {
	return db.tx.Commit()
}

func (db *LocalDB) Rollback() error {
	return db.tx.Rollback()
}

func (db *LocalDB) Create(name, filename string) error {
	_, err := db.tx.Exec("INSERT INTO dbs (name, filename) VALUES(?, ?)", name, filename)
	return err
}

func (db *LocalDB) Delete(name string) error {
	_, err := db.tx.Exec("DELETE FROM dbs WHERE name = ?", name)
	return err
}

func (db *LocalDB) GetFileName(name string) string {
	var fileName string
	row := db.tx.QueryRow("SELECT filename FROM dbs WHERE name = ?", name)
	row.Scan(&fileName)
	return fileName
}

func (db *LocalDB) List() ([]string, error) {
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

func (db *LocalDB) UpdateRowCount(name string, rowcount int) error {
	_, err := db.tx.Exec("UPDATE dbs SET row_count = ? WHERE name = ?", rowcount, name)
	return err
}
