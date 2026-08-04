package main

import (
	"kdb3/sqlite3"
)

type DatabaseWriter interface {
	Open(createIfNotExists bool) error
	Close() error

	Begin() error
	Commit() error
	Rollback() error

	ExecBuildScript() error

	GetDocumentMetadataByID(docID string) (*Document, error)
	PutDocument(updateSeq int64, newDoc *Document) error
}

func SetupDatabaseScript() string {
	buildSQL := `
		CREATE TABLE IF NOT EXISTS documents (
			doc_id 		TEXT,
			version     INTEGER,
			deleted     BOOL,
			data        TEXT,
			update_seq	INT,
			PRIMARY KEY (doc_id)
		) WITHOUT ROWID;

		CREATE INDEX IF NOT EXISTS idx_metadata ON documents
			(doc_id, version, deleted);

		CREATE INDEX IF NOT EXISTS idx_changes ON documents
			(doc_id, update_seq, deleted);
		`
	return buildSQL
}

type DefaultDatabaseWriter struct {
	connectionString string

	reader          *DefaultDatabaseReader
	conn            *sqlite3.Conn
	stmtPutDocument *sqlite3.Stmt
}

func (writer *DefaultDatabaseWriter) Open(createIfNotExists bool) error {
	con, err := sqlite3.Open(writer.connectionString)
	if err != nil {
		return err
	}
	writer.conn = con
	writer.reader.conn = con

	for _, p := range []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA cache_size=-64000;",
	} {
		if err = con.Exec(p); err != nil {
			return err
		}
	}

	if createIfNotExists {
		writer.Begin()
		if err := writer.ExecBuildScript(); err != nil {
			return err
		}
		writer.Commit()
	}

	writer.stmtPutDocument, err = con.Prepare("INSERT OR REPLACE INTO documents (doc_id, version, deleted, update_seq, data) VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}

	err = writer.reader.Prepare()
	if err != nil {
		return err
	}

	return nil
}

// Close connection
func (writer *DefaultDatabaseWriter) Close() error {
	if writer.stmtPutDocument != nil {
		_ = writer.stmtPutDocument.Close()
	}
	// Checkpoint WAL so the main file is self-contained before rename/delete.
	if writer.conn != nil {
		_ = writer.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	}
	return writer.reader.Close()
}

// Begin begin transaction
func (writer *DefaultDatabaseWriter) Begin() error {
	return writer.conn.Begin()
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
	return writer.conn.Exec(SetupDatabaseScript())
}

// GetDocumentRevisionByID get document revision by id
func (writer *DefaultDatabaseWriter) GetDocumentMetadataByID(docID string) (*Document, error) {
	return writer.reader.GetDocumentMetadataByID(docID)
}

// PutDocument put document
func (writer *DefaultDatabaseWriter) PutDocument(updateSeq int64, newDoc *Document) error {
	defer writer.stmtPutDocument.Reset()
	return writer.stmtPutDocument.Exec(newDoc.ID, newDoc.Version, newDoc.Deleted, updateSeq, newDoc.Data)
}
