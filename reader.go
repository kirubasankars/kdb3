package main

import (
	"database/sql"
	"errors"
)

type DatabaseReader interface {
	Open(path string) error
	Close() error
	Begin() error
	Commit() error

	GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error)
	GetDocumentRevisionByID(ID string) (*Document, error)

	GetDocumentByID(ID string) (*Document, error)
	GetDocumentByIDandVersion(ID string, Version int) (*Document, error)

	GetAllDesignDocuments() ([]*Document, error)
	GetChanges(since string) []byte

	GetLastUpdateSequence() string
	GetDocumentCount() int
}

type DefaultDatabaseReader struct {
	conn *sql.DB
	tx   *sql.Tx
}

func (reader *DefaultDatabaseReader) Open(path string) error {
	con, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	reader.conn = con
	return nil
}

func (reader *DefaultDatabaseReader) Begin() error {
	var err error
	reader.tx, err = reader.conn.Begin()
	return err
}

func (reader *DefaultDatabaseReader) Commit() error {
	return reader.tx.Commit()
}

func (reader *DefaultDatabaseReader) GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error) {
	doc := Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? AND version = ? LIMIT 1", ID, Version)
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

func (reader *DefaultDatabaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {
	doc := Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", ID)
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

func (reader *DefaultDatabaseReader) GetDocumentByID(ID string) (*Document, error) {
	doc := &Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, deleted, (SELECT data FROM documents WHERE doc_id = ?) FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", ID, ID)
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

func (reader *DefaultDatabaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {
	doc := &Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, deleted, (SELECT data FROM documents WHERE doc_id = ?) as data FROM changes WHERE doc_id = ? AND version = ? LIMIT 1", ID, ID, Version)
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

func (reader *DefaultDatabaseReader) GetAllDesignDocuments() ([]*Document, error) {

	var docs []*Document
	rows, err := reader.tx.Query("SELECT doc_id FROM documents WHERE doc_id like '_design/%'")
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

func (db *DefaultDatabaseReader) GetChanges(since string) []byte {
	sqlGetChanges := `WITH all_changes(seq, version, doc_id, deleted) as
	(
		SELECT seq_id as seq, version, doc_id, deleted FROM changes c WHERE (? IS NULL OR seq_id > ?) ORDER by seq_id DESC
	),
	changes_object (obj) as
	(
		SELECT (CASE WHEN deleted != 1 THEN JSON_OBJECT('seq', seq, 'version', version, 'id', doc_id) ELSE JSON_OBJECT('seq', seq, 'version', version, 'id', doc_id, 'deleted', true)  END) as obj FROM all_changes
	)
	SELECT JSON_OBJECT('results',JSON_GROUP_ARRAY(obj)) FROM changes_object`
	row := db.tx.QueryRow(sqlGetChanges, since, since)
	var (
		changes []byte
	)

	err := row.Scan(&changes)
	if err != nil {
		panic(err)
	}

	return changes
}

func (db *DefaultDatabaseReader) GetLastUpdateSequence() string {
	var maxSeqID string
	sqlGetMaxSeq := "SELECT IFNULL(seq_id, '') FROM (SELECT MAX(seq_id) as seq_id FROM changes)"

	row := db.tx.QueryRow(sqlGetMaxSeq)
	err := row.Scan(&maxSeqID)
	if err != nil && err.Error() != "sql: no rows in result set" {
		panic(err)
	}

	return maxSeqID
}

func (db *DefaultDatabaseReader) GetDocumentCount() int {
	row := db.tx.QueryRow("SELECT COUNT(1) FROM documents")
	count := 0
	row.Scan(&count)
	return count
}

func (reader *DefaultDatabaseReader) Close() error {
	return reader.conn.Close()
}

type DatabaseReaderPool struct {
	path string
	pool chan DatabaseReader
}

func NewDatabaseReaderPool(path string, limit int) DatabaseReaderPool {
	readers := DatabaseReaderPool{
		path: path,
		pool: make(chan DatabaseReader, limit),
	}
	for x := 0; x < limit; x++ {
		r := &DefaultDatabaseReader{}
		_ = r.Open(path)
		readers.pool <- r
	}
	return readers
}

func (p *DatabaseReaderPool) Borrow() DatabaseReader {
	return <-p.pool
}

func (p *DatabaseReaderPool) Return(r DatabaseReader) {
	p.pool <- r
}

func (p *DatabaseReaderPool) Close() error {
	var err error
	for {
		var r DatabaseReader
		select {
		case r = <-p.pool:
			err = r.Close()
		default:
		}
		if r == nil {
			break
		}
	}

	return err
}
