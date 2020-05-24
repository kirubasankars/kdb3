package main

import (
	"database/sql"
	"fmt"
)

// DatabaseReader DatabaseReader interface
type DatabaseReader interface {
	Open(connectionString string) error
	Close() error
	Begin() error
	Commit() error

	GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error)
	GetDocumentRevisionByID(ID string) (*Document, error)

	GetDocumentByID(ID string) (*Document, error)
	GetDocumentByIDandVersion(ID string, Version int) (*Document, error)

	GetAllDesignDocuments() ([]*Document, error)
	GetChanges(since string, limit int) ([]byte, error)

	GetLastUpdateSequence() string
	GetDocumentCount() (int, int)
}

// DatabaseReaderPool DatabaseReader Pool
type DatabaseReaderPool interface {
	Open(connectionString string) error
	Borrow() DatabaseReader
	Return(r DatabaseReader)
	Close() error
}

// DefaultDatabaseReader default implmentation database interface
type DefaultDatabaseReader struct {
	connectionString string
	conn             *sql.DB
	tx               *sql.Tx
}

// Open open database reader with connectionString
func (reader *DefaultDatabaseReader) Open(connectionString string) error {
	reader.connectionString = connectionString
	con, err := sql.Open("sqlite3", connectionString)
	if err != nil {
		return err
	}
	reader.conn = con
	return nil
}

// Begin begin transaction
func (reader *DefaultDatabaseReader) Begin() error {
	var err error
	reader.tx, err = reader.conn.Begin()
	return err
}

// Commit commit transaction
func (reader *DefaultDatabaseReader) Commit() error {
	return reader.tx.Commit()
}

// GetDocumentRevisionByIDandVersion get document info with id and version
func (reader *DefaultDatabaseReader) GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error) {
	doc := &Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, ifnull(kind, '') as kind, deleted FROM documents INDEXED BY idx_metadata WHERE doc_id = ? AND version = ? LIMIT 1", ID, Version)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}

	if doc.ID == "" {
		return nil, ErrDocumentNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocumentNotFound
	}

	return doc, nil
}

// GetDocumentRevisionByID get document info with id
func (reader *DefaultDatabaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {
	doc := &Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, ifnull(kind, '') as kind, deleted FROM documents INDEXED BY idx_metadata WHERE doc_id = ?", ID)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, ErrDocumentNotFound
	}

	if doc.ID == "" {
		return nil, ErrDocumentNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocumentNotFound
	}

	return doc, nil
}

// GetDocumentByID get document id
func (reader *DefaultDatabaseReader) GetDocumentByID(ID string) (*Document, error) {
	doc := &Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, ifnull(kind, '') as kind, deleted, data as data FROM documents WHERE doc_id = ?", ID, ID)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted, &doc.Data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, ErrDocumentNotFound
	}

	var meta string = fmt.Sprintf(`{"_id":"%s","_version":%d`, doc.ID, doc.Version)
	if doc.Kind != "" {
		meta = fmt.Sprintf(`%s,"_kind":"%s"`, meta, doc.Kind)
	}
	if len(doc.Data) != 2 {
		meta = meta + ","
	}
	data := make([]byte, len(meta))
	copy(data, meta)
	if len(doc.Data) > 0 {
		data = append(data, doc.Data[1:]...)
	}
	doc.Data = data

	if doc.ID == "" {
		return nil, ErrDocumentNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocumentNotFound
	}

	return doc, nil
}

// GetDocumentByIDandVersion get document id and version
func (reader *DefaultDatabaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {
	doc := &Document{}

	row := reader.tx.QueryRow("SELECT doc_id, version, ifnull(kind, '') as kind, deleted, data FROM documents WHERE doc_id = ? AND version = ?", ID, Version)
	err := row.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted, &doc.Data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, err
	}
	var meta string = fmt.Sprintf(`{"_id":"%s","_version":%d`, doc.ID, doc.Version)
	if doc.Kind != "" {
		meta = fmt.Sprintf(`%s,"_kind":"%s"`, meta, doc.Kind)
	}
	if len(doc.Data) != 2 {
		meta = meta + ","
	}
	data := make([]byte, len(meta))
	copy(data, meta)
	if len(doc.Data) > 0 {
		data = append(data, doc.Data[1:]...)
	}
	doc.Data = data

	if doc.ID == "" {
		return nil, ErrDocumentNotFound
	}

	if doc.Deleted == true {
		return doc, ErrDocumentNotFound
	}

	return doc, nil
}

// GetAllDesignDocuments get all design documents
func (reader *DefaultDatabaseReader) GetAllDesignDocuments() ([]*Document, error) {

	var docs []*Document
	rows, err := reader.tx.Query("SELECT doc_id FROM documents WHERE doc_id like '_design/%' AND deleted != 1")
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

// GetChanges get document changes
func (reader *DefaultDatabaseReader) GetChanges(since string, limit int) ([]byte, error) {
	sqlGetChanges := `WITH all_changes(doc_id) as
	(
		SELECT doc_id FROM documents INDEXED BY idx_changes WHERE (? IS NULL OR seq_id > ?) ORDER by seq_id ASC LIMIT ?
	),
	all_changes_metadata (seq, doc_id, version, deleted) AS 
	(
		SELECT d.seq_id, d.doc_id, d.version, d.deleted FROM documents d INDEXED BY idx_metadata JOIN all_changes c USING (doc_id) ORDER BY d.seq_id DESC
	),
	changes_object (obj) as
	(
		SELECT (CASE WHEN deleted != 1 THEN JSON_OBJECT('seq', seq, 'version', version, 'id', doc_id) ELSE JSON_OBJECT('seq', seq, 'version', version, 'id', doc_id, 'deleted', JSON('true'))  END) as obj FROM all_changes_metadata
	)
	SELECT JSON_OBJECT('results',JSON_GROUP_ARRAY(obj)) FROM changes_object`
	row := reader.tx.QueryRow(sqlGetChanges, since, since, limit)
	var (
		changes []byte
	)

	err := row.Scan(&changes)
	if err != nil {
		return nil, err
	}

	return changes, nil
}

// GetLastUpdateSequence get document changes
func (reader *DefaultDatabaseReader) GetLastUpdateSequence() string {
	var maxUpdateSeq string
	sqlGetMaxSeq := "SELECT IFNULL(seq_id, '') FROM (SELECT MAX(seq_id) as seq_id FROM documents INDEXED BY idx_changes)"
	row := reader.tx.QueryRow(sqlGetMaxSeq)
	err := row.Scan(&maxUpdateSeq)
	if err != nil && err.Error() != "sql: no rows in result set" {
		panic(err)
	}
	return maxUpdateSeq
}

// GetDocumentCount get document count
func (reader *DefaultDatabaseReader) GetDocumentCount() (int, int) {
	rows, _ := reader.tx.Query("SELECT deleted, COUNT(1) as count FROM documents GROUP BY deleted")
	deleted, count, docCount, deletedDocCount := 0, 0, 0, 0
	for rows.Next() {
		rows.Scan(&deleted, &count)
		if deleted == 0 {
			docCount = count
		} else {
			deletedDocCount = count
		}
	}
	return docCount, deletedDocCount
}

// Close close the database reader
func (reader *DefaultDatabaseReader) Close() error {
	return reader.conn.Close()
}

// DefaultDatabaseReaderPool default implementation of reader pool
type DefaultDatabaseReaderPool struct {
	pool           chan DatabaseReader
	limit          int
	serviceLocator ServiceLocator
}

// Open open reader pool
func (p *DefaultDatabaseReaderPool) Open(connectionString string) error {
	for x := 0; x < p.limit; x++ {
		r := p.serviceLocator.GetDatabaseReader()
		err := r.Open(connectionString)
		if err != nil {
			panic(err)
		}
		p.pool <- r
	}
	return nil
}

// Borrow borrow a reader
func (p *DefaultDatabaseReaderPool) Borrow() DatabaseReader {
	return <-p.pool
}

// Return return a reader
func (p *DefaultDatabaseReaderPool) Return(r DatabaseReader) {
	p.pool <- r
}

// TODO: close all connections (may be wait) by limit
// Close reader pool
func (p *DefaultDatabaseReaderPool) Close() error {
	var err error
	count := 0
	for {
		var r DatabaseReader
		select {
		case r = <-p.pool:
			err = r.Close()
			count++
		default:
		}
		if count == p.limit {
			break
		}
	}
	return err
}

// NewDatabaseReaderPool create new reader pool
func NewDatabaseReaderPool(limit int, serviceLocator ServiceLocator) DatabaseReaderPool {
	readers := DefaultDatabaseReaderPool{
		pool:           make(chan DatabaseReader, limit),
		limit:          limit,
		serviceLocator: serviceLocator,
	}
	return &readers
}
