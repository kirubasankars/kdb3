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

	GetDocumentRevisionByID(docID string) (*Document, error)
	PutDocument(updateSeqID string, newDoc *Document) error
}

func SetupDatabaseScript() string {
	buildSQL := `
		CREATE TABLE IF NOT EXISTS documents (
			doc_id 		TEXT, 
			version     INTEGER, 
			kind	    TEXT,
			deleted     BOOL,
			data        TEXT,
			seq_id 		TEXT,
			PRIMARY KEY (doc_id)
		) WITHOUT ROWID;
		
		CREATE INDEX IF NOT EXISTS idx_metadata ON documents 
			(doc_id, version, kind, deleted);

		CREATE INDEX IF NOT EXISTS idx_changes ON documents 
			(doc_id, seq_id, deleted);

		CREATE INDEX IF NOT EXISTS idx_kind ON documents 
			(doc_id, kind) WHERE kind IS NOT NULL;
		`
	return buildSQL
}

type DefaultDatabaseWriter struct {
	connectionString string

	reader *DefaultDatabaseReader
	conn   *sql.DB
	tx     *sql.Tx
}

func (writer *DefaultDatabaseWriter) Open() error {
	con, err := sql.Open("sqlite3", writer.connectionString)
	if err != nil {
		return err
	}
	err = con.Ping()
	if err != nil {
		return err
	}
	writer.conn = con
	writer.reader.conn = con
	return nil
}

// Close connection
func (writer *DefaultDatabaseWriter) Close() error {
	err := writer.conn.Close()
	return err
}

// Begin begin transaction
func (writer *DefaultDatabaseWriter) Begin() error {
	var err error
	writer.tx, err = writer.conn.Begin()
	writer.reader.tx = writer.tx
	return err
}

// Commit commit transaction
func (writer *DefaultDatabaseWriter) Commit() error {
	return writer.tx.Commit()
}

// Rollback rollback transaction
func (writer *DefaultDatabaseWriter) Rollback() error {
	return writer.tx.Rollback()
}

// ExecBuildScript build tables
func (writer *DefaultDatabaseWriter) ExecBuildScript() error {
	tx := writer.tx

	buildSQL := SetupDatabaseScript()

	if _, err := tx.Exec(buildSQL); err != nil {
		return err
	}

	return nil
}

// GetDocumentRevisionByID get document revision by id
func (writer *DefaultDatabaseWriter) GetDocumentRevisionByID(docID string) (*Document, error) {
	return writer.reader.GetDocumentRevisionByID(docID)
}

// PutDocument put document
func (writer *DefaultDatabaseWriter) PutDocument(updateSeqID string, newDoc *Document) error {
	tx := writer.tx
	var kind []byte
	if newDoc.Kind != "" {
		kind = []byte(newDoc.Kind)
	}
	if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, version, kind, deleted, seq_id, data) VALUES(?, ?, CAST(? as TEXT), ?, ?, JSON(?))", newDoc.ID, newDoc.Version, kind, newDoc.Deleted, updateSeqID, newDoc.Data); err != nil {
		return err
	}
	return nil
}
