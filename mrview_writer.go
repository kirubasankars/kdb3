package main

import (
	"path/filepath"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type ViewWriter interface {
	Open() error
	Close() error
	Build(nextSeqID int) error
}

type DefaultViewWriter struct {
	connectionString string
	dbName           string
	con              *sqlite3.Conn

	absoluteDatabasePath string
	setupScripts         []Query
	scripts              []Query

	stmtUpdateViewMeta *sqlite3.Stmt
}

func (vw *DefaultViewWriter) Open() error {
	db, err := sqlite3.Open(vw.connectionString)
	if err != nil {
		return err
	}
	vw.con = db

	err = db.Exec("PRAGMA journal_mode=MEMORY;")
	if err != nil {
		return err
	}

	buildSQL := `
		CREATE TABLE IF NOT EXISTS view_meta (
			Id						INTEGER PRIMARY KEY,
			current_update_seq		INT,
			next_update_seq		  	INT
		) WITHOUT ROWID;

		INSERT INTO view_meta (Id, current_update_seq, next_update_seq)
			SELECT 1,0,0 WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`

	err = db.WithTx(func() error {
		if err := db.Exec(buildSQL); err != nil {
			return err
		}
		if err = setupViewDatabase(db, vw.absoluteDatabasePath); err != nil {
			return err
		}
		for _, x := range vw.setupScripts {
			if err = db.Exec(x.text); err != nil {
				return err
			}
		}

		vw.stmtUpdateViewMeta, err = db.Prepare("UPDATE view_meta SET current_update_seq = next_update_seq, next_update_seq = ?")

		return err
	})

	return err
}

func (vw *DefaultViewWriter) Close() error {
	vw.stmtUpdateViewMeta.Close()
	return vw.con.Close()
}

func (vw *DefaultViewWriter) Build(nextSeqID int) error {
	db := vw.con

	err := db.WithTx(func() error {
		defer vw.stmtUpdateViewMeta.Reset()
		if err := vw.stmtUpdateViewMeta.Exec(nextSeqID); err != nil {
			return err
		}
		//TODO: use complied stmt
		for _, x := range vw.scripts {
			if err := db.Exec(x.text); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func NewViewWriter(DBName, DBPath, connectionString string, setupScripts, scripts []Query) *DefaultViewWriter {
	viewWriter := new(DefaultViewWriter)
	viewWriter.connectionString = connectionString
	viewWriter.dbName = DBName
	viewWriter.setupScripts = setupScripts
	viewWriter.scripts = scripts

	absoluteDatabasePath, err := filepath.Abs(DBPath)
	if err != nil {
		panic(err)
	}
	viewWriter.absoluteDatabasePath = absoluteDatabasePath

	return viewWriter
}
