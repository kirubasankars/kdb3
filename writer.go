package main

import (
	"database/sql"
)

type DataBaseWriter struct {
	DataBaseReader
	conn *sql.DB
	tx   *sql.Tx
}

func (writer *DataBaseWriter) Open(path string) error {
	con, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	writer.conn = con
	writer.DataBaseReader.conn = con
	return nil
}

func (writer *DataBaseWriter) Begin() error {
	var err error
	writer.tx, err = writer.conn.Begin()
	return err
}

func (writer *DataBaseWriter) Commit() error {
	return writer.tx.Commit()
}

func (writer *DataBaseWriter) Rollback() error {
	return writer.tx.Rollback()
}

func (writer *DataBaseWriter) DeleteDocumentByID(ID string) error {
	tx := writer.tx
	if _, err := tx.Exec("DELETE FROM documents WHERE doc_id = ?", ID); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) InsertDocument(ID string, data []byte) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, data) VALUES(?, ?)", ID, string(data)); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) InsertRevision(ID string, RevNumber int, RevID string, Deleted bool) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT INTO revisions (doc_id, rev_number, rev_id, deleted) VALUES(?, ?, ?, ?)", ID, RevNumber, RevID, Deleted); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) InsertChange(UpdateSeqNumber int, UpdateSeqID string, ID string, RevNumber int, RevID string, Deleted bool) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT INTO changes (seq_number, seq_id, doc_id, rev_number, rev_id) VALUES(?, ?, ?, ?, ?)", UpdateSeqNumber, UpdateSeqID, ID, RevNumber, RevID); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) ExecBuildScript() error {
	tx := writer.tx

	buildSQL := `
	CREATE TABLE IF NOT EXISTS documents (
		doc_id 		TEXT,
		data 		TEXT,
		PRIMARY KEY (doc_id)
	) WITHOUT ROWID;

	CREATE TABLE IF NOT EXISTS changes (
		seq_number  INTEGER,
		seq_id 		TEXT, 
		doc_id 		TEXT, 
		rev_number  INTEGER, 
		rev_id 		TEXT, 
		PRIMARY KEY (seq_number, seq_id)
	) WITHOUT ROWID;

	CREATE TABLE IF NOT EXISTS revisions (
		doc_id 		TEXT,
		rev_number  INTEGER,
		rev_id 		TEXT,
		deleted 	BOOL,
		PRIMARY KEY (doc_id, rev_number DESC)
	) WITHOUT ROWID;`

	if _, err := tx.Exec(buildSQL); err != nil {
		return err
	}

	return nil
}

func (writer *DataBaseWriter) Vacuum() error {
	_, err := writer.conn.Exec("VACUUM")
	return err
}
