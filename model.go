package main

type DBStat struct {
	DBName          string `json:"db_name"`
	UpdateSeq       string `json:"update_seq"`
	DocCount        int    `json:"doc_count"`
	DeletedDocCount int    `json:"deleted_doc_count"`
}

type DesignDocumentView struct {
	Setup  []string          `json:"setup,omitempty"`
	Run    []string          `json:"run,omitempty"`
	Select map[string]string `json:"select,omitempty"`
}

type DesignDocument struct {
	ID      string                         `json:"_id"`
	Version int                            `json:"_version,omitempty"`
	Kind    string                         `json:"_kind"`
	Views   map[string]*DesignDocumentView `json:"views"`
}

type Query struct {
	text   string
	params []string
}
