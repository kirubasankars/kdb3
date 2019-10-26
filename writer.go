package main

import (
	"database/sql"
	"fmt"
)

type DatabaseWriter interface {
	Open(path string) error
	Begin() error
	Commit() error
	Rollback() error
	Close() error
	Vacuum() error

	DeleteDocumentByID(docID string) error
	InsertDocument(docID string, version int, data []byte) error
	InsertChange(updateSeqNumber int, updateSeqID string, docID string, version int, deleted bool) error
	DeleteChange(docID string, version int) error
	GetDocumentRevisionByID(docID string) (*Document, error)
	ExecBuildScript() error
}

type DefaultDatabaseWriter struct {
	reader DefaultDatabaseReader
	conn   *sql.DB
	tx     *sql.Tx
}

func (writer *DefaultDatabaseWriter) Open(path string) error {
	con, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	writer.conn = con
	writer.reader.conn = con
	return nil
}

func (writer *DefaultDatabaseWriter) Close() error {
	err := writer.conn.Close()
	return err
}

func (writer *DefaultDatabaseWriter) Begin() error {
	var err error
	writer.tx, err = writer.conn.Begin()
	return err
}

func (writer *DefaultDatabaseWriter) Commit() error {
	return writer.tx.Commit()
}

func (writer *DefaultDatabaseWriter) Rollback() error {
	return writer.tx.Rollback()
}

func (writer *DefaultDatabaseWriter) GetDocumentRevisionByID(docID string) (*Document, error) {
	return writer.reader.GetDocumentRevisionByID(docID)
}

func (writer *DefaultDatabaseWriter) DeleteDocumentByID(ID string) error {
	tx := writer.tx
	if _, err := tx.Exec("DELETE FROM documents WHERE doc_id = ?", ID); err != nil {
		return err
	}
	return nil
}

func (writer *DefaultDatabaseWriter) InsertDocument(ID string, Version int, Data []byte) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, version, data) VALUES(?, ?, ?)", ID, Version, Data); err != nil {
		return err
	}
	return nil
}

func (writer *DefaultDatabaseWriter) InsertChange(UpdateSeqNumber int, UpdateSeqID string, ID string, Version int, Deleted bool) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT INTO changes (seq_number, seq_id, doc_id, version, deleted) VALUES(?, ?, ?, ?, ?)", UpdateSeqNumber, UpdateSeqID, ID, Version, Deleted); err != nil {
		return err
	}
	return nil
}

func (writer *DefaultDatabaseWriter) DeleteChange(ID string, Version int) error {
	tx := writer.tx
	if _, err := tx.Exec("DELETE FROM changes WHERE doc_id = ? AND version = ?", ID, Version); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (writer *DefaultDatabaseWriter) ExecBuildScript() error {
	tx := writer.tx

	buildSQL := `
	CREATE TABLE IF NOT EXISTS documents (
		doc_id 		TEXT,
		version  INTEGER, 
		data 		TEXT,
		PRIMARY KEY (doc_id)
	) WITHOUT ROWID;

	CREATE TABLE IF NOT EXISTS changes (
		seq_number  INTEGER,
		seq_id 		TEXT, 
		doc_id 		TEXT, 
		version  INTEGER, 
		deleted     BOOL,
		PRIMARY KEY (seq_number, seq_id)
	) WITHOUT ROWID;
	
	CREATE INDEX IF NOT EXISTS idx_revisions ON changes 
		(doc_id, version, deleted);
		
	CREATE UNIQUE INDEX IF NOT EXISTS idx_uniq_version ON changes 
		(doc_id, version);`

	if _, err := tx.Exec(buildSQL); err != nil {
		return err
	}

	return nil
}

func (writer *DefaultDatabaseWriter) Vacuum() error {
	_, err := writer.conn.Exec("VACUUM")
	return err
}
