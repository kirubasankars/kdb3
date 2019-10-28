package main

import (
	"database/sql"
	"errors"
)

type DatabaseWriter interface {
	Open(path string) error
	Close() error

	Begin() error
	Commit() error
	Rollback() error

	ExecBuildScript() error
	Vacuum() error

	GetDocumentRevisionByID(docID string) (*Document, error)
	PutDocument(updateSeqNumber int, updateSeqID string, newDoc *Document, currentDoc *Document) error
}

type DefaultDatabaseWriter struct {
	conn *sql.DB
	tx   *sql.Tx
}

func (writer *DefaultDatabaseWriter) Open(path string) error {
	con, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	writer.conn = con
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

func (writer *DefaultDatabaseWriter) GetDocumentRevisionByID(docID string) (*Document, error) {
	doc := Document{}

	row := writer.tx.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", docID)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Deleted)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	if doc.Deleted == true {
		return &doc, errors.New("doc_not_found")
	}

	return &doc, nil
}

func (writer *DefaultDatabaseWriter) PutDocument(updateSeqNumber int, updateSeqID string, newDoc *Document, currentDoc *Document) error {
	tx := writer.tx

	if _, err := tx.Exec("INSERT INTO changes (seq_number, seq_id, doc_id, version, deleted) VALUES(?, ?, ?, ?, ?)", updateSeqNumber, updateSeqID, newDoc.ID, newDoc.Version, newDoc.Deleted); err != nil {
		if err.Error() == "UNIQUE constraint failed: changes.doc_id, changes.rev_number" {
			return errors.New("doc_conflict")
		}
		return err
	}

	if newDoc.Deleted {
		if _, err := tx.Exec("DELETE FROM documents WHERE doc_id = ?", newDoc.ID); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, version, data) VALUES(?, ?, ?)", newDoc.ID, newDoc.Version, newDoc.Data); err != nil {
			return err
		}
	}

	if currentDoc != nil {
		if _, err := tx.Exec("DELETE FROM changes WHERE doc_id = ? AND version = ?", currentDoc.ID, currentDoc.Version); err != nil {
			return err
		}
	}

	return nil
}
