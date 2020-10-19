package main

import "C"
import (
	"fmt"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

// DatabaseReader DatabaseReader interface
type DatabaseReader interface {
	Open() error
	Close() error
	Begin() error
	Commit() error

	GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error)
	GetDocumentRevisionByID(ID string) (*Document, error)

	GetDocumentByID(ID string) (*Document, error)
	GetDocumentByIDandVersion(ID string, Version int) (*Document, error)

	GetAllDesignDocuments() ([]Document, error)
	GetChanges(since string, limit int) ([]byte, error)

	GetLastUpdateSequence() string
	GetDocumentCount() (int, int)
}

// DefaultDatabaseReader default implementation database interface
type DefaultDatabaseReader struct {
	connectionString string

	conn *sqlite3.Conn

	stmtDocumentRevisionByIDandVersion *sqlite3.Stmt
	stmtDocumentRevisionByID           *sqlite3.Stmt
	stmtDocumentByID                   *sqlite3.Stmt
	stmtDocumentByIDandVersion         *sqlite3.Stmt
	stmtAllDesignDocuments             *sqlite3.Stmt
	stmtChanges                        *sqlite3.Stmt
	stmtLastUpdateSequence             *sqlite3.Stmt
	stmtDocumentCount                  *sqlite3.Stmt
}

// Open open database reader with connectionString
func (reader *DefaultDatabaseReader) Open() error {
	con, err := sqlite3.Open(reader.connectionString)
	if err != nil {
		return err
	}

	reader.conn = con

	err = reader.Prepare()
	if err != nil {
		return err
	}

	return nil
}

func (reader *DefaultDatabaseReader) Prepare() error {
	con := reader.conn
	var err error
	reader.stmtDocumentCount, err = con.Prepare("SELECT deleted, COUNT(1) as count FROM documents GROUP BY deleted")
	if err != nil {
		return err
	}
	reader.stmtDocumentRevisionByIDandVersion, err = con.Prepare("SELECT doc_id, version, IFNULL(kind, '') as kind, deleted FROM documents INDEXED BY idx_metadata WHERE doc_id = ? AND version = ? LIMIT 1")
	if err != nil {
		return err
	}
	reader.stmtDocumentRevisionByID, err = con.Prepare("SELECT doc_id, version, IFNULL(kind, '') as kind, deleted FROM documents INDEXED BY idx_metadata WHERE doc_id = ?")
	if err != nil {
		return err
	}
	reader.stmtDocumentByID, err = con.Prepare("SELECT doc_id, version, IFNULL(kind, '') as kind, deleted, data as data FROM documents WHERE doc_id = ?")
	if err != nil {
		return err
	}
	reader.stmtDocumentByIDandVersion, err = con.Prepare("SELECT doc_id, version, IFNULL(kind, '') as kind, deleted, data FROM documents WHERE doc_id = ? AND version = ?")
	if err != nil {
		return err
	}
	reader.stmtAllDesignDocuments, err = con.Prepare("SELECT doc_id FROM documents WHERE doc_id like '_design/%' AND deleted != 1")
	if err != nil {
		return err
	}
	reader.stmtChanges, err = con.Prepare(`
		WITH all_changes(doc_id) as
		(
			SELECT doc_id FROM documents INDEXED BY idx_changes WHERE (? IS NULL OR seq_id > ?) ORDER by seq_id ASC LIMIT ?
		),
		all_changes_metadata (seq, doc_id, version, deleted) AS 
		(
			SELECT d.seq_id, d.doc_id, d.version, d.deleted FROM documents d INDEXED BY idx_metadata JOIN all_changes c USING (doc_id) ORDER BY d.seq_id DESC
		),
		changes_object (obj) as
		(
			SELECT (CASE WHEN deleted != 1 THEN JSON_OBJECT('seq', seq, 'id', doc_id, 'version', version) ELSE JSON_OBJECT('seq', seq, 'id', doc_id, 'version', version, 'deleted', JSON('true'))  END) as obj FROM all_changes_metadata
		)
		SELECT JSON_OBJECT('results',JSON_GROUP_ARRAY(obj)) FROM changes_object
	`)
	if err != nil {
		return err
	}
	reader.stmtLastUpdateSequence, err = con.Prepare("SELECT IFNULL(seq_id, '') FROM (SELECT MAX(seq_id) as seq_id FROM documents INDEXED BY idx_changes)")
	if err != nil {
		return err
	}
	return nil
}

// Begin begin transaction
func (reader *DefaultDatabaseReader) Begin() error {
	var err error
	err = reader.conn.Begin()
	return err
}

// Commit commit transaction
func (reader *DefaultDatabaseReader) Commit() error {
	return reader.conn.Commit()
}

// GetDocumentRevisionByIDandVersion get document info with id and version
func (reader *DefaultDatabaseReader) GetDocumentRevisionByIDandVersion(ID string, Version int) (*Document, error) {

	reader.stmtDocumentRevisionByIDandVersion.Bind(ID, Version)
	hasRow, _ := reader.stmtDocumentRevisionByIDandVersion.Step()
	defer reader.stmtDocumentRevisionByIDandVersion.Reset()

	if hasRow {
		doc := &Document{}
		err := reader.stmtDocumentRevisionByIDandVersion.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted)
		if err != nil {
			return nil, err
		}

		if doc.Deleted == true {
			return doc, ErrDocumentNotFound
		}

		return doc, nil
	}

	return nil, ErrDocumentNotFound
}

// GetDocumentRevisionByID get document info with id
func (reader *DefaultDatabaseReader) GetDocumentRevisionByID(ID string) (*Document, error) {

	err := reader.stmtDocumentRevisionByID.Bind(ID)
	if err != nil {
		return nil, err
	}
	hasRow, err := reader.stmtDocumentRevisionByID.Step()
	if err != nil {
		return nil, err
	}
	defer reader.stmtDocumentRevisionByID.Reset()

	if hasRow {
		doc := &Document{}
		err := reader.stmtDocumentRevisionByID.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted)
		if err != nil {
			return nil, err
		}
		if doc.Deleted == true {
			return doc, ErrDocumentNotFound
		}

		return doc, nil
	}

	return nil, nil
}

// GetDocumentByID get document id
func (reader *DefaultDatabaseReader) GetDocumentByID(ID string) (*Document, error) {
	reader.stmtDocumentByID.Bind(ID)
	hasRow, _ := reader.stmtDocumentByID.Step()
	defer reader.stmtDocumentByID.Reset()

	if hasRow {
		doc := &Document{}
		err := reader.stmtDocumentByID.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted, &doc.Data)
		if err != nil {
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

		if doc.Deleted == true {
			return doc, ErrDocumentNotFound
		}
		return doc, nil
	}

	return nil, nil
}

// GetDocumentByIDandVersion get document id and version
func (reader *DefaultDatabaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {
	reader.stmtDocumentByIDandVersion.Bind(ID, Version)
	hasRow, _ := reader.stmtDocumentByIDandVersion.Step()
	defer reader.stmtDocumentByIDandVersion.Reset()

	if hasRow {
		doc := &Document{}
		err := reader.stmtDocumentByIDandVersion.Scan(&doc.ID, &doc.Version, &doc.Kind, &doc.Deleted, &doc.Data)
		if err != nil {
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

		if doc.Deleted == true {
			return doc, ErrDocumentNotFound
		}

		return doc, nil
	}

	return nil, nil
}

// GetAllDesignDocuments get all design documents
func (reader *DefaultDatabaseReader) GetAllDesignDocuments() ([]Document, error) {

	hasRows, _ := reader.stmtAllDesignDocuments.Step()
	defer reader.stmtAllDesignDocuments.Reset()

	if hasRows {
		var docs []Document

		for hasRows {

			var id string
			err := reader.stmtAllDesignDocuments.Scan(&id)
			if err != nil {
				return nil, err
			}

			doc, err := reader.GetDocumentByID(id)
			if err != nil {
				return nil, err
			}
			docs = append(docs, *doc)

			hasRows, _ = reader.stmtAllDesignDocuments.Step()
		}

		return docs, nil
	}

	return nil, nil
}

// GetChanges get document changes
func (reader *DefaultDatabaseReader) GetChanges(since string, limit int) ([]byte, error) {

	reader.stmtChanges.Bind(since, since, limit)
	hasRow, _ := reader.stmtAllDesignDocuments.Step()
	defer reader.stmtAllDesignDocuments.Reset()
	var (
		changes []byte
	)

	if hasRow {
		err := reader.stmtChanges.Scan(&changes)
		if err != nil {
			return nil, err
		}
	}

	return changes, nil
}

// GetLastUpdateSequence get document changes
func (reader *DefaultDatabaseReader) GetLastUpdateSequence() string {

	hasRow, _ := reader.stmtLastUpdateSequence.Step()
	defer reader.stmtLastUpdateSequence.Reset()
	if hasRow {
		var maxUpdateSeq string
		reader.stmtLastUpdateSequence.Scan(&maxUpdateSeq)
		return maxUpdateSeq
	}

	panic(fmt.Errorf("No row found"))
}

// GetDocumentCount get document count
func (reader *DefaultDatabaseReader) GetDocumentCount() (int, int) {

	hasRow, _ := reader.stmtDocumentCount.Step()
	defer reader.stmtDocumentCount.Reset()

	deleted, count, docCount, deletedDocCount := 0, 0, 0, 0
	for hasRow {
		reader.stmtLastUpdateSequence.Scan(&deleted, &count)
		if deleted == 0 {
			docCount = count
		} else {
			deletedDocCount = count
		}
		hasRow, _ = reader.stmtLastUpdateSequence.Step()
	}

	return docCount, deletedDocCount
}

// Close close the database reader
func (reader *DefaultDatabaseReader) Close() error {
	return reader.conn.Close()
}
