package main

type DBStat struct {
	DBName    string `json:"db_name"`
	UpdateSeq string `json:"update_seq"`
	DocCount  int    `json:"doc_count"`
}

type DesignDocumentView struct {
	Setup  []string          `json:"setup"`
	Delete []string          `json:"delete"`
	Update []string          `json:"update"`
	Select map[string]string `json:"select"`
}

type DesignDocument struct {
	ID    string                         `json:"_id"`
	Rev   string                         `json:"_rev,omitempty"`
	Views map[string]*DesignDocumentView `json:"views"`
}

type Query struct {
	text   string
	params []string
}
