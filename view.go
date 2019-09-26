package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
)

type Query struct {
	text   string
	params []string
}

type View struct {
	name                string
	fileName            string
	dbName              string
	lastUpdateSeqNumber int
	lastUpdateSeqID     string
	designDocID         string

	setupScripts  []Query
	deleteScripts []Query
	updateScripts []Query
	selectScripts map[string]Query

	viewPath string
	dbPath   string
	db       *sql.DB
}

func NewView(dbPath, viewPath, dbName, viewName string, designDoc *DesignDocument) *View {
	view := &View{}

	if _, ok := designDoc.Views[viewName]; !ok {
		return nil
	}

	view.viewPath = viewPath
	view.dbPath = dbPath
	view.name = viewName
	view.dbName = dbName
	view.designDocID = designDoc.ID

	view.setupScripts = *new([]Query)
	view.deleteScripts = *new([]Query)
	view.updateScripts = *new([]Query)
	view.selectScripts = make(map[string]Query)
	designDocView := designDoc.Views[viewName]

	for _, x := range designDocView.Setup {
		text, params := ParseQuery(x)
		view.setupScripts = append(view.setupScripts, Query{text: text, params: params})
	}
	for _, x := range designDocView.Delete {
		text, params := ParseQuery(x)
		view.deleteScripts = append(view.deleteScripts, Query{text: text, params: params})
	}
	for _, x := range designDocView.Update {
		text, params := ParseQuery(x)
		view.updateScripts = append(view.updateScripts, Query{text: text, params: params})
	}

	for k, v := range designDocView.Select {
		text, params := ParseQuery(v)
		view.selectScripts[k] = Query{text: text, params: params}
	}

	return view
}

func ParseQuery(query string) (string, []string) {
	re := regexp.MustCompile(`\$\{(.*?)\}`)
	o := re.FindAllStringSubmatch(query, -1)
	var params []string
	for _, x := range o {
		params = append(params, x[1])
	}
	text := re.ReplaceAllString(query, "?")
	return text, params
}

func (view *View) Open() error {
	view.fileName = view.dbName + "$" + view.name + dbExt
	viewFilePath := filepath.Join(view.viewPath, view.fileName)

	db, err := sql.Open("sqlite3", viewFilePath+"?_journal=MEMORY")
	if err != nil {
		return err
	}

	buildSQL := `CREATE TABLE IF NOT EXISTS view_meta (
		Id					INTEGER PRIMARY KEY,
		seq_number			INTEGER,
		seq_id		  		TEXT,
		design_doc_updated  INTEGER
	) WITHOUT ROWID;

	INSERT INTO view_meta (Id, seq_number, seq_id, design_doc_updated) 
		SELECT 1, 0, "", false WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`

	if _, err = db.Exec(buildSQL); err != nil {
		return err
	}

	dbFilePath := filepath.Join(view.dbPath, view.dbName)
	absoluteDBPath, err := filepath.Abs(dbFilePath)
	if err != nil {
		return err
	}

	_, err = db.Exec("ATTACH DATABASE '" + absoluteDBPath + ".db' as docsdb;")
	if err != nil {
		return err
	}

	for _, x := range view.setupScripts {
		if _, err = db.Exec(x.text); err != nil {
			return err
		}
	}

	sqlGetViewLastSeq := "SELECT seq_number, seq_id FROM view_meta WHERE id = 1"
	row := db.QueryRow(sqlGetViewLastSeq)
	row.Scan(&view.lastUpdateSeqNumber, &view.lastUpdateSeqID)

	view.db = db

	return err
}

func (view *View) Close() error {
	return view.db.Close()
}

func (view *View) Build(maxSeqNumber int, maxSeqID string) error {

	if view.lastUpdateSeqID == maxSeqID && view.lastUpdateSeqNumber == maxSeqNumber {
		return nil
	}

	db := view.db

	tx, err := db.Begin()
	defer tx.Rollback()
	if err != nil {
		panic(err)
	}

	for _, x := range view.deleteScripts {
		values := make([]interface{}, len(x.params))
		for i, p := range x.params {
			if p == "begin_seq_number" {
				values[i] = view.lastUpdateSeqNumber
			}
			if p == "end_seq_number" {
				values[i] = maxSeqNumber
			}
			if p == "end_seq_id" {
				values[i] = maxSeqID
			}
			if p == "end_seq_id" {
				values[i] = maxSeqID
			}
		}
		if _, err = tx.Exec(x.text, values...); err != nil {
			fmt.Println(err)
			return err
		}
	}

	for _, x := range view.updateScripts {
		values := make([]interface{}, len(x.params))
		for i, p := range x.params {
			if p == "begin_seq_number" {
				values[i] = view.lastUpdateSeqNumber
			}
			if p == "end_seq_number" {
				values[i] = maxSeqNumber
			}
			if p == "begin_seq_id" {
				values[i] = view.lastUpdateSeqID
			}
			if p == "end_seq_id" {
				values[i] = maxSeqID
			}
		}
		if _, err = tx.Exec(x.text, values...); err != nil {
			fmt.Println(err)
			return err
		}
	}

	sqlUpdateViewMeta := "UPDATE view_meta SET seq_number = ?, seq_id = ? "
	if _, err := tx.Exec(sqlUpdateViewMeta, maxSeqNumber, maxSeqID); err != nil {
		panic(err)
	}

	view.lastUpdateSeqNumber = maxSeqNumber
	view.lastUpdateSeqID = maxSeqID

	tx.Commit()

	return nil
}

func (view *View) Select(name string, values url.Values) []byte {

	var rs string
	selectStmt := view.selectScripts[name]
	pValues := make([]interface{}, len(selectStmt.params))
	for i, p := range selectStmt.params {
		pv := values.Get(p)
		if pv != "" {
			pValues[i] = values.Get(p)
		}
	}

	row := view.db.QueryRow(selectStmt.text, pValues...)
	err := row.Scan(&rs)
	if err != nil {
		panic(err)
	}
	return []byte(rs)
}

func (view *View) MarkUpdated() {
	if _, err := view.db.Exec("UPDATE view_meta SET design_doc_updated = true"); err != nil {
		panic(err)
	}
}

func (view *View) Vacuum() error {
	if _, err := view.db.Exec("VACUUM"); err != nil {
		return err
	}
	return nil
}
