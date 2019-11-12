package main

import (
	"database/sql"
)

type DatabaseWriter interface {
	Open() error
	Close() error

	Begin() error
	Commit() error
	Rollback() error

	ExecBuildScript() error
	Vacuum() error

	GetDocumentRevisionByID(docID string) (*Document, error)
	PutDocument(updateSeqID string, newDoc *Document, currentDoc *Document) error
}

type DefaultDatabaseWriter struct {
	connectionString string
	reader           *DefaultDatabaseReader
	conn             *sql.DB
	tx               *sql.Tx
}

func (writer *DefaultDatabaseWriter) Open() error {
	con, err := sql.Open("sqlite3", writer.connectionString)
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
	writer.reader.tx = writer.tx
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
			seq_id 		TEXT, 
			doc_id 		TEXT, 
			version  INTEGER, 
			deleted     BOOL,
			PRIMARY KEY (seq_id)
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
	return writer.reader.GetDocumentRevisionByID(docID)
}

func (writer *DefaultDatabaseWriter) PutDocument(updateSeqID string, newDoc *Document, currentDoc *Document) error {
	tx := writer.tx

	if _, err := tx.Exec("INSERT INTO changes (seq_id, doc_id, version, deleted) VALUES(?, ?, ?, ?)", updateSeqID, newDoc.ID, newDoc.Version, newDoc.Deleted); err != nil {
		if err.Error() == "UNIQUE constraint failed: changes.doc_id, changes.version" {
			return ErrDocConflict
		}
		if err.Error() == "UNIQUE constraint failed: changes.seq_id" {
			return ErrInternalError
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
