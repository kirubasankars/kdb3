package main

import (
	"database/sql"
)

type ViewWriter interface {
	Open() error
	Close() error
	Build(nextSeqID string) error
}

type DefaultViewWriter struct {
	connectionString string
	setupScripts     []Query
	deleteScripts    []Query
	updateScripts    []Query

	setupDatabase func(db *sql.DB) error
	con           *sql.DB
}

func (vw *DefaultViewWriter) Open() error {
	db, err := sql.Open("sqlite3", vw.connectionString)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	buildSQL := `CREATE TABLE IF NOT EXISTS view_meta (
		Id						INTEGER PRIMARY KEY,
		current_seq_id		  	TEXT,
		next_seq_id		  		TEXT
	) WITHOUT ROWID;

	INSERT INTO view_meta (Id, current_seq_id, next_seq_id) 
		SELECT 1,"", "" WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`

	if _, err = tx.Exec(buildSQL); err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	err = vw.setupDatabase(db)
	if err != nil {
		return err
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}

	for _, x := range vw.setupScripts {
		if _, err = tx.Exec(x.text); err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	vw.con = db

	return nil
}

func (vw *DefaultViewWriter) Close() error {
	return vw.con.Close()
}

func (vw *DefaultViewWriter) Build(nextSeqID string) error {
	db := vw.con
	tx, err := db.Begin()
	defer tx.Rollback()
	if err != nil {
		panic(err)
	}

	sqlUpdateViewMeta := "UPDATE view_meta SET current_seq_id = next_seq_id, next_seq_id = ? "
	if _, err := tx.Exec(sqlUpdateViewMeta, nextSeqID); err != nil {
		panic(err)
	}

	for _, x := range vw.deleteScripts {
		if _, err = tx.Exec(x.text); err != nil {
			return err
		}
	}

	for _, x := range vw.updateScripts {
		if _, err = tx.Exec(x.text); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func NewViewWriter(connectionString string, setupScripts, deleteScripts, updateScripts []Query) (*DefaultViewWriter, error) {
	viewWriter := new(DefaultViewWriter)
	viewWriter.connectionString = connectionString
	viewWriter.setupScripts = setupScripts
	viewWriter.deleteScripts = deleteScripts
	viewWriter.updateScripts = updateScripts
	return viewWriter, nil
}
