package main

import (
	"database/sql"
	"errors"
)

type DataBaseReader struct {
	conn *sql.DB
}

func (reader *DataBaseReader) Open(path string) error {
	con, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	reader.conn = con
	return nil
}

func (reader *DataBaseReader) GetDocumentRevisionByIDandRev(ID string, revNumber int, revID string) (*Document, error) {
	doc := Document{}

	row := reader.conn.QueryRow("SELECT doc_id, rev_number, rev_id, deleted FROM revisions WHERE doc_id = ? AND rev_number = ? AND rev_id = ? LIMIT 1", ID, revNumber, revID)
	err := row.Scan(&doc.ID, &doc.RevNumber, &doc.RevID, &doc.Deleted)
	if err != nil {
		return nil, err
	}

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	return &doc, nil
}

func (reader *DataBaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {
	doc := Document{}

	row := reader.conn.QueryRow("SELECT doc_id, rev_number, rev_id, deleted FROM revisions WHERE doc_id = ? LIMIT 1", ID)
	row.Scan(&doc.ID, &doc.RevNumber, &doc.RevID, &doc.Deleted)

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	return &doc, nil
}

func (reader *DataBaseReader) GetDocumentByID(ID string) (*Document, error) {
	doc := &Document{}

	row := reader.conn.QueryRow("SELECT doc_id, rev_number, rev_id, deleted, (SELECT data FROM documents WHERE doc_id = ?) FROM revisions WHERE doc_id = ? LIMIT 1", ID, ID)
	err := row.Scan(&doc.ID, &doc.RevNumber, &doc.RevID, &doc.Deleted, &doc.Data)
	if err != nil {
		return nil, err
	}

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	return doc, nil
}

func (reader *DataBaseReader) GetDocumentByIDandRev(ID string, revNumber int, revID string) (*Document, error) {
	doc := &Document{}

	row := reader.conn.QueryRow("SELECT doc_id, rev_number, rev_id, deleted, (SELECT data FROM documents WHERE doc_id = ?) as data FROM revisions WHERE doc_id = ? AND rev_number = ? AND rev_id = ? LIMIT 1", ID, ID, revNumber, revID)
	err := row.Scan(&doc.ID, &doc.RevNumber, &doc.RevID, &doc.Deleted, &doc.Data)
	if err != nil {
		return nil, err
	}

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	return doc, nil
}

func (reader *DataBaseReader) GetAllDesignDocuments() ([]*Document, error) {

	var docs []*Document
	rows, err := reader.conn.Query("SELECT doc_id FROM documents WHERE doc_id like '_design/%'")
	if err != nil {
		return nil, err
	}

	for {
		if !rows.Next() {
			break
		}

		var id string
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}

		doc, _ := reader.GetDocumentByID(id)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func (db *DataBaseReader) GetLastUpdateSequence() (int, string) {
	sqlGetMaxSeq := "SELECT seq_number, seq_id FROM (SELECT MAX(seq_number) as seq_number, MAX(seq_id)  as seq_id FROM changes WHERE seq_id = (SELECT MAX(seq_id) FROM changes) UNION ALL SELECT 0, '') WHERE seq_number IS NOT NULL LIMIT 1"
	row := db.conn.QueryRow(sqlGetMaxSeq)
	var (
		maxSeqNumber int
		maxSeqID     string
	)

	err := row.Scan(&maxSeqNumber, &maxSeqID)
	if err != nil {
		panic(err)
	}

	return maxSeqNumber, maxSeqID
}

func (db *DataBaseReader) GetDocumentCount() int {
	row := db.conn.QueryRow("SELECT COUNT(1) FROM documents")
	count := 0
	row.Scan(&count)
	return count
}

func (reader *DataBaseReader) Close() error {
	return reader.conn.Close()
}
