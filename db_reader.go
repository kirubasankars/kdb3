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

	GetDocumentMetadataByIDandVersion(ID string, Version int) (*Document, error)
	GetDocumentMetadataByID(ID string) (*Document, error)

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

	stmtDocumentMetadataByIDandVersion *sqlite3.Stmt
	stmtDocumentMetadataByID           *sqlite3.Stmt
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

	return reader.Prepare()
}

// Close close the database reader
func (reader *DefaultDatabaseReader) Close() error {
	reader.stmtDocumentCount.Close()
	reader.stmtDocumentMetadataByIDandVersion.Close()
	reader.stmtDocumentMetadataByID.Close()
	reader.stmtDocumentByID.Close()
	reader.stmtDocumentByIDandVersion.Close()
	reader.stmtAllDesignDocuments.Close()
	reader.stmtChanges.Close()
	reader.stmtLastUpdateSequence.Close()
	return reader.conn.Close()
}

func (reader *DefaultDatabaseReader) Prepare() error {
	con := reader.conn
	var err error
	reader.stmtDocumentCount, err = con.Prepare("SELECT deleted, COUNT(1) as count FROM documents GROUP BY deleted")
	if err != nil {
		return err
	}
	reader.stmtDocumentMetadataByIDandVersion, err = con.Prepare("SELECT doc_id, version, hash, deleted FROM documents INDEXED BY idx_metadata WHERE doc_id = ? AND version = ? LIMIT 1")
	if err != nil {
		return err
	}
	reader.stmtDocumentMetadataByID, err = con.Prepare("SELECT doc_id, version, hash, deleted FROM documents INDEXED BY idx_metadata WHERE doc_id = ?")
	if err != nil {
		return err
	}
	reader.stmtDocumentByID, err = con.Prepare("SELECT doc_id, version, hash, deleted, data FROM documents WHERE doc_id = ?")
	if err != nil {
		return err
	}
	reader.stmtDocumentByIDandVersion, err = con.Prepare("SELECT doc_id, version, hash, deleted, data FROM documents WHERE doc_id = ? AND version = ?")
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
		all_changes_metadata (seq, doc_id, version, hash, deleted) AS 
		(
			SELECT d.seq_id, d.doc_id, d.version, d.hash, d.deleted FROM documents d INDEXED BY idx_metadata JOIN all_changes c USING (doc_id) ORDER BY d.seq_id
		),
		changes_object (obj) as
		(
			SELECT (CASE WHEN deleted != 1 THEN JSON_OBJECT('seq', seq, 'id', doc_id, 'rev', printf('%d-%s', version, hash)) ELSE JSON_OBJECT('seq', seq, 'id', doc_id, 'rev', printf('%d-%s', version, hash), 'deleted', JSON('true'))  END) as obj FROM all_changes_metadata
		)
		SELECT JSON_OBJECT('results', JSON_GROUP_ARRAY(obj)) FROM changes_object
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
	return reader.conn.Begin()
}

// Commit commit transaction
func (reader *DefaultDatabaseReader) Commit() error {
	return reader.conn.Commit()
}

// GetDocumentRevisionByIDandVersion get document info with id and version
func (reader *DefaultDatabaseReader) GetDocumentMetadataByIDandVersion(ID string, Version int) (*Document, error) {

	defer reader.stmtDocumentMetadataByIDandVersion.Reset()
	if err := reader.stmtDocumentMetadataByIDandVersion.Bind(ID, Version); err != nil {
		return nil, err
	}
	hasRow, err := reader.stmtDocumentMetadataByIDandVersion.Step()
	if err != nil {
		return nil, err
	}

	if hasRow {
		doc := &Document{}
		if err := reader.stmtDocumentMetadataByIDandVersion.Scan(&doc.ID, &doc.Version, &doc.Hash, &doc.Deleted); err != nil {
			return nil, err
		}
		if doc.Deleted {
			return doc, ErrDocumentNotFound
		}
		return doc, nil
	}

	return nil, ErrDocumentNotFound
}

// GetDocumentRevisionByID get document info with id
func (reader *DefaultDatabaseReader) GetDocumentMetadataByID(ID string) (*Document, error) {

	defer reader.stmtDocumentMetadataByID.Reset()
	if err := reader.stmtDocumentMetadataByID.Bind(ID); err != nil {
		return nil, err
	}

	hasRow, err := reader.stmtDocumentMetadataByID.Step()
	if err != nil {
		return nil, err
	}

	if hasRow {
		doc := &Document{}
		if err := reader.stmtDocumentMetadataByID.Scan(&doc.ID, &doc.Version, &doc.Hash, &doc.Deleted); err != nil {
			return nil, err
		}
		if doc.Deleted {
			return doc, ErrDocumentNotFound
		}
		return doc, nil
	}

	return nil, ErrDocumentNotFound
}

// GetDocumentByID get document id
func (reader *DefaultDatabaseReader) GetDocumentByID(ID string) (*Document, error) {

	defer reader.stmtDocumentByID.Reset()
	if err := reader.stmtDocumentByID.Bind(ID); err != nil {
		return nil, err
	}

	hasRow, err := reader.stmtDocumentByID.Step()
	if err != nil {
		return nil, err
	}

	if hasRow {
		doc := &Document{}
		if err := reader.stmtDocumentByID.Scan(&doc.ID, &doc.Version, &doc.Hash, &doc.Deleted, &doc.Data); err != nil {
			return nil, err
		}

		var meta = fmt.Sprintf(`{"_id":"%s","_rev":"%d-%s"`, doc.ID, doc.Version, doc.Hash)
		if len(doc.Data) != 2 {
			meta = meta + ","
		}

		data := make([]byte, len(meta))
		copy(data, meta)
		if len(doc.Data) > 0 {
			data = append(data, doc.Data[1:]...)
		}
		doc.Data = data

		if doc.Deleted {
			return doc, ErrDocumentNotFound
		}

		return doc, nil
	}

	return nil, ErrDocumentNotFound
}

// GetDocumentByIDandVersion get document id and version
func (reader *DefaultDatabaseReader) GetDocumentByIDandVersion(ID string, Version int) (*Document, error) {

	defer reader.stmtDocumentByIDandVersion.Reset()
	if err := reader.stmtDocumentByIDandVersion.Bind(ID, Version); err != nil {
		return nil, err
	}

	hasRow, err := reader.stmtDocumentByIDandVersion.Step()
	if err != nil {
		return nil, err
	}

	if hasRow {
		doc := &Document{}
		err := reader.stmtDocumentByIDandVersion.Scan(&doc.ID, &doc.Version, &doc.Hash, &doc.Deleted, &doc.Data)
		if err != nil {
			return nil, err
		}

		var meta = fmt.Sprintf(`{"_id":"%s","_rev":"%d-%s"`, doc.ID, doc.Version, doc.Hash)
		if len(doc.Data) != 2 {
			meta = meta + ","
		}

		data := make([]byte, len(meta))
		copy(data, meta)
		if len(doc.Data) > 0 {
			data = append(data, doc.Data[1:]...)
		}
		doc.Data = data

		if doc.Deleted {
			return doc, ErrDocumentNotFound
		}

		return doc, nil
	}

	return nil, ErrDocumentNotFound
}

// GetAllDesignDocuments get all design documents
func (reader *DefaultDatabaseReader) GetAllDesignDocuments() ([]Document, error) {

	defer reader.stmtAllDesignDocuments.Reset()
	hasRows, err := reader.stmtAllDesignDocuments.Step()
	if err != nil {
		return nil, err
	}

	if hasRows {
		var docs []Document

		for hasRows {
			var id string
			if err := reader.stmtAllDesignDocuments.Scan(&id); err != nil {
				return nil, err
			}

			doc, err := reader.GetDocumentByID(id)
			if err != nil {
				return nil, err
			}
			docs = append(docs, *doc)

			hasRows, err = reader.stmtAllDesignDocuments.Step()
			if err != nil {
				return nil, err
			}
		}

		return docs, nil
	}

	return nil, ErrDocumentNotFound
}

// GetChanges get document changes
func (reader *DefaultDatabaseReader) GetChanges(since string, limit int) ([]byte, error) {

	defer reader.stmtChanges.Reset()
	if err := reader.stmtChanges.Bind(since, since, limit); err != nil {
		return nil, err
	}

	hasRow, err := reader.stmtChanges.Step()
	if err != nil {
		return nil, err
	}

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

	defer reader.stmtLastUpdateSequence.Reset()
	hasRow, err := reader.stmtLastUpdateSequence.Step()
	if err != nil {
		panic(err)
	}

	if hasRow {
		var maxUpdateSeq string
		reader.stmtLastUpdateSequence.Scan(&maxUpdateSeq)
		return maxUpdateSeq
	}

	panic("No row found")
}

// GetDocumentCount get document count
func (reader *DefaultDatabaseReader) GetDocumentCount() (int, int) {

	defer reader.stmtDocumentCount.Reset()
	hasRow, err := reader.stmtDocumentCount.Step()
	if err != nil {
		panic(err)
	}

	deleted, count, docCount, deletedDocCount := 0, 0, 0, 0
	for hasRow {
		reader.stmtDocumentCount.Scan(&deleted, &count)
		if deleted == 0 {
			docCount = count
		} else {
			deletedDocCount = count
		}
		hasRow, err = reader.stmtDocumentCount.Step()
		if err != nil {
			panic(err)
		}
	}

	return docCount, deletedDocCount
}
