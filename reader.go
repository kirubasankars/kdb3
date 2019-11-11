package main

import (
	"database/sql"
)

type DatabaseReader interface {
	Open(path string) error
	Close() error

	GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error)
	GetDocumentRevisionByID(ID string) (*Document, error)
	GetDocumentByID(ID string) (*Document, error)
	GetDocumentByIDandVersion(ID string, Version int) (*Document, error)

	GetAllDesignDocuments() ([]*Document, error)
	GetChanges(since string, limit int) ([]byte, error)
	GetLastUpdateSequence() (string, error)
	GetDocumentCount() (int, error)
}

type DefaultDatabaseReader struct {
	conn *sql.DB
}

func (reader *DefaultDatabaseReader) Open(path string) error {
	con, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	reader.conn = con
	return nil
}

func (reader *DefaultDatabaseReader) GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error) {
	doc := &Document{}
	tx, err := reader.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	row := tx.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? AND version = ? LIMIT 1", ID, Version)
	err = row.Scan(&doc.ID, &doc.Version, &doc.Deleted)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, ErrDocNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (reader *DefaultDatabaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {
	doc := &Document{}
	tx, err := reader.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	row := tx.QueryRow("SELECT doc_id, version, deleted FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", ID)
	err = row.Scan(&doc.ID, &doc.Version, &doc.Deleted)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, ErrDocNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (reader *DefaultDatabaseReader) GetDocumentByID(ID string) (*Document, error) {
	doc := &Document{}
	tx, err := reader.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	row := tx.QueryRow("SELECT doc_id, version, deleted, (SELECT data FROM documents WHERE doc_id = ?) FROM changes WHERE doc_id = ? ORDER BY version DESC LIMIT 1", ID, ID)
	err = row.Scan(&doc.ID, &doc.Version, &doc.Deleted, &doc.Data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, ErrDocNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (reader *DefaultDatabaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {
	doc := &Document{}
	tx, err := reader.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	row := tx.QueryRow("SELECT doc_id, version, deleted, (SELECT data FROM documents WHERE doc_id = ?) as data FROM changes WHERE doc_id = ? AND version = ? LIMIT 1", ID, ID, Version)
	err = row.Scan(&doc.ID, &doc.Version, &doc.Deleted, &doc.Data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, ErrDocNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (reader *DefaultDatabaseReader) GetAllDesignDocuments() ([]*Document, error) {
	tx, err := reader.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	var docs []*Document
	rows, err := tx.Query("SELECT doc_id FROM documents WHERE doc_id like '_design/%'")
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

func (reader *DefaultDatabaseReader) GetChanges(since string, limit int) ([]byte, error) {
	tx, err := reader.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	sqlGetChanges := `WITH all_changes(seq, version, doc_id, deleted) as
	(
		SELECT * FROM (SELECT seq_id as seq, version, doc_id, deleted FROM changes c WHERE (? IS NULL OR seq_id > ?) ORDER by seq_id ASC LIMIT ?)  ORDER BY seq DESC
	),
	changes_object (obj) as
	(
		SELECT (CASE WHEN deleted != 1 THEN JSON_OBJECT('seq', seq, 'version', version, 'id', doc_id) ELSE JSON_OBJECT('seq', seq, 'version', version, 'id', doc_id, 'deleted', true)  END) as obj FROM all_changes
	)
	SELECT JSON_OBJECT('results',JSON_GROUP_ARRAY(obj)) FROM changes_object`
	row := tx.QueryRow(sqlGetChanges, since, since, limit)
	var (
		changes []byte
	)

	err = row.Scan(&changes)
	if err != nil {
		return nil, err
	}

	return changes, nil
}

func (reader *DefaultDatabaseReader) GetLastUpdateSequence() (string, error) {
	tx, err := reader.conn.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Commit()

	var lastUpdateSeq string
	sqlGetMaxSeq := "SELECT IFNULL(seq_id, '') FROM (SELECT MAX(seq_id) as seq_id FROM changes)"

	row := tx.QueryRow(sqlGetMaxSeq)
	err = row.Scan(&lastUpdateSeq)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return "", ErrInternalError
	}
	return lastUpdateSeq, nil
}

func (reader *DefaultDatabaseReader) GetDocumentCount() (int, error) {
	tx, err := reader.conn.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Commit()
	row := tx.QueryRow("SELECT COUNT(1) FROM documents")
	count := 0
	row.Scan(&count)
	return count, nil
}

func (reader *DefaultDatabaseReader) Close() error {
	return reader.conn.Close()
}

type DatabaseReaderPool interface {
	Borrow() DatabaseReader
	Return(r DatabaseReader)
	Close() error
}

type DefaultDatabaseReaderPool struct {
	path string
	pool chan DatabaseReader
}

func NewDatabaseReaderPool(path string, limit int) DatabaseReaderPool {
	readers := DefaultDatabaseReaderPool{
		path: path,
		pool: make(chan DatabaseReader, limit),
	}
	for x := 0; x < limit; x++ {
		r := &DefaultDatabaseReader{}
		_ = r.Open(path)
		readers.pool <- r
	}
	return &readers
}

func (p *DefaultDatabaseReaderPool) Borrow() DatabaseReader {
	return <-p.pool
}

func (p *DefaultDatabaseReaderPool) Return(r DatabaseReader) {
	p.pool <- r
}

func (p *DefaultDatabaseReaderPool) Close() error {
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
