package main

import (
	"path/filepath"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type ViewWriter interface {
	Open() error
	Close() error
	Build(nextSeqID string) error
}

type DefaultViewWriter struct {
	connectionString string
	con              *sqlite3.Conn

	absoluteDatabasePath string
	setupScripts         []Query
	scripts              []Query
}

func (vw *DefaultViewWriter) Open() error {
	db, err := sqlite3.Open(vw.connectionString)
	if err != nil {
		return err
	}

	buildSQL := `
		CREATE TABLE IF NOT EXISTS view_meta (
			Id						INTEGER PRIMARY KEY,
			current_seq_id		  	TEXT,
			next_seq_id		  		TEXT
		) WITHOUT ROWID;
	
		INSERT INTO view_meta (Id, current_seq_id, next_seq_id) 
			SELECT 1,"", "" WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`

	err = db.WithTx(func() error {
		return db.Exec(buildSQL)
	})
	if err != nil {
		return err
	}

	err = setupViewDatabase(db, vw.absoluteDatabasePath)
	if err != nil {
		return err
	}

	err = db.WithTx(func() error {
		for _, x := range vw.setupScripts {
			if err = db.Exec(x.text); err != nil {
				return err
			}
		}
		return nil
	})
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

	err := db.WithTx(func() error {
		sqlUpdateViewMeta := "UPDATE view_meta SET current_seq_id = next_seq_id, next_seq_id = ? "
		if err := db.Exec(sqlUpdateViewMeta, nextSeqID); err != nil {
			return err
		}
		for _, x := range vw.scripts {
			if err := db.Exec(x.text); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func NewViewWriter(DBName, DBPath, connectionString string, setupScripts, scripts []Query) *DefaultViewWriter {
	viewWriter := new(DefaultViewWriter)
	viewWriter.connectionString = connectionString
	viewWriter.setupScripts = setupScripts
	viewWriter.scripts = scripts

	absoluteDatabasePath, err := filepath.Abs(DBPath)
	if err != nil {
		panic(err)
	}
	viewWriter.absoluteDatabasePath = absoluteDatabasePath

	return viewWriter
}
