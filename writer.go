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

func (writer *DataBaseWriter) InsertDocument(ID string, revNumber int, revID string, deleted bool, data []byte) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT OR REPLACE INTO documents (doc_id, rev_number, rev_id, deleted, data) VALUES(?, ?, ?, ?, ?)", ID, revNumber, revID, deleted, string(data)); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) InsertChanges(UpdateSeqNumber int, UpdateSeqID string, ID string, RevNumber int, RevID string, Deleted bool) error {
	tx := writer.tx
	if _, err := tx.Exec("INSERT INTO changes (seq_number, seq_id, doc_id, rev_number, rev_id, deleted) VALUES(?, ?, ?, ?, ?, ?)", UpdateSeqNumber, UpdateSeqID, ID, RevNumber, RevID, Deleted); err != nil {
		return err
	}
	return nil
}

func (writer *DataBaseWriter) ExecBuildScript() error {
	tx := writer.tx

	buildSQL := `
	CREATE TABLE IF NOT EXISTS documents (
		doc_id 		TEXT,
		rev_number  INTEGER, 
		rev_id 		TEXT,
		deleted		BOOL,
		data 		TEXT,
		PRIMARY KEY (doc_id)
	) WITHOUT ROWID;

	CREATE TABLE IF NOT EXISTS changes (
		seq_number  INTEGER,
		seq_id 		TEXT, 
		doc_id 		TEXT, 
		rev_number  INTEGER, 
		rev_id 		TEXT, 
		deleted     BOOL,
		PRIMARY KEY (seq_number, seq_id)
	) WITHOUT ROWID;
	
	CREATE INDEX IF NOT EXISTS idx_revisions ON changes 
		(doc_id, rev_number, rev_id, deleted);
		
	CREATE VIEW IF NOT EXISTS latest_documents (seq_number, seq_id, doc_id, rev_number, rev_id, rev, deleted, data) AS 
		WITH 
			latest_revs (doc_id, rev_number, rev_id, seq_number, seq_id) AS
			(	
				select doc_id, max(rev_number) as rev_number, rev_id, seq_number, seq_id from changes group by doc_id
			),
			latest_docs (seq_number, seq_id, doc_id, rev_number, rev_id, rev, deleted, data) AS
			(
				SELECT seq_number, seq_id, doc_id, rev_number, r.rev_id, printf('%d-%s',rev_number, r.rev_id) as rev, deleted, data FROM latest_revs r LEFT JOIN documents d USING (doc_id, rev_number)  WHERE d.deleted IS NOT NULL 
			)
			SELECT * FROM latest_docs;`

	if _, err := tx.Exec(buildSQL); err != nil {
		return err
	}

	return nil
}

func (writer *DataBaseWriter) Vacuum() error {
	_, err := writer.conn.Exec("VACUUM")
	return err
}
