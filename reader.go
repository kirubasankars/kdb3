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

func (reader *DataBaseReader) GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error) {
	doc := Document{}

	row := reader.conn.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? AND version = ? LIMIT 1", ID, Version)
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

func (reader *DataBaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {
	doc := Document{}

	row := reader.conn.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", ID)
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

func (reader *DataBaseReader) GetDocumentByID(ID string) (*Document, error) {
	doc := &Document{}

	row := reader.conn.QueryRow("SELECT doc_id, version, deleted, (SELECT data FROM documents WHERE doc_id = ?) FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", ID, ID)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Deleted, &doc.Data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	if doc.Deleted == true {
		return doc, errors.New("doc_not_found")
	}

	return doc, nil
}

func (reader *DataBaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {
	doc := &Document{}

	row := reader.conn.QueryRow("SELECT doc_id, version, deleted, (SELECT data FROM documents WHERE doc_id = ?) as data FROM changes WHERE doc_id = ? AND version = ? LIMIT 1", ID, ID, Version)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Deleted, &doc.Data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, errors.New("doc_not_found")
	}

	if doc.Deleted == true {
		return doc, errors.New("doc_not_found")
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

func (db *DataBaseReader) GetChanges() []byte {
	sqlGetChanges := `
		SELECT 
		JSON_OBJECT("results",JSON_GROUP_ARRAY(json(v))) 
		FROM (
				SELECT 
				JSON_OBJECT(
								"seq",printf("%d-%s",seq_number,max(seq_id)),
								"id", doc_id, 
								"changes", (SELECT JSON_GROUP_ARRAY(JSON(version)) from (SELECT JSON_OBJECT('version', version) as version FROM changes where doc_id = c.doc_id ORDER by seq_id DESC))
							)  as v FROM changes c GROUP BY doc_id ORDER by seq_id DESC
			 )`
	row := db.conn.QueryRow(sqlGetChanges)
	var (
		changes []byte
	)

	err := row.Scan(&changes)
	if err != nil {
		panic(err)
	}

	return changes
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
