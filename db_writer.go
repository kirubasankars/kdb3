package main

import (
	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type DatabaseWriter interface {
	Open(createIfNotExists bool) error
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
	conn   *sqlite3.Conn
}

func (writer *DefaultDatabaseWriter) Open(createIfNotExists bool) error {
	con, err := sqlite3.Open(writer.connectionString)
	if err != nil {
		return err
	}
	con.Exec("PRAGMA journal_mode=WAL;")

	writer.conn = con
	writer.reader.conn = con

	if createIfNotExists {
		writer.Begin()
		if err := writer.ExecBuildScript(); err != nil {
			return err
		}
		writer.Commit()
	}

	err = writer.reader.Prepare()
	if err != nil {
		return err
	}

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
	err = writer.conn.Begin()
	return err
}

// Commit commit transaction
func (writer *DefaultDatabaseWriter) Commit() error {
	return writer.conn.Commit()
}

// Rollback rollback transaction
func (writer *DefaultDatabaseWriter) Rollback() error {
	return writer.conn.Rollback()
}

// ExecBuildScript build tables
func (writer *DefaultDatabaseWriter) ExecBuildScript() error {
	buildSQL := SetupDatabaseScript()

	if err := writer.conn.Exec(buildSQL); err != nil {
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
	var kind []byte
	if newDoc.Kind != "" {
		kind = []byte(newDoc.Kind)
	}
	if err := writer.conn.Exec("INSERT OR REPLACE INTO documents (doc_id, version, kind, deleted, seq_id, data) VALUES(?, ?, CAST(? as TEXT), ?, ?, JSON(?))", newDoc.ID, newDoc.Version, kind, newDoc.Deleted, updateSeqID, newDoc.Data); err != nil {
		return err
	}
	return nil
}
