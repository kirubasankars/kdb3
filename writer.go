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

func (writer *DataBaseWriter) InsertDocument(ID string, Version int, Deleted bool, Data []byte) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, version, deleted, data) VALUES(?, ?, ?, ?)", ID, Version, Deleted, string(Data)); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) InsertChanges(UpdateSeqNumber int, UpdateSeqID string, ID string, Version int, Deleted bool) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT INTO changes (seq_number, seq_id, doc_id, version, deleted) VALUES(?, ?, ?, ?, ?)", UpdateSeqNumber, UpdateSeqID, ID, Version, Deleted); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) ExecBuildScript() error {
	tx := writer.tx

	buildSQL := `
	CREATE TABLE IF NOT EXISTS documents (
		doc_id 		TEXT,
		version  INTEGER, 
		deleted		BOOL,
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

func (writer *DataBaseWriter) Vacuum() error {
	_, err := writer.conn.Exec("VACUUM")
	return err
}
