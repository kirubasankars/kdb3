package main

// DatabaseStat stat
type DatabaseStat struct {
	DBName          string `json:"name"`
	UpdateSeq       string `json:"update_seq"`
	DocCount        int    `json:"doc_count"`
	DeletedDocCount int    `json:"deleted_doc_count"`
}

// DesignDocumentView design document view
type DesignDocumentView struct {
	Setup  []string          `json:"setup,omitempty"`
	Run    []string          `json:"run,omitempty"`
	Select map[string]string `json:"select,omitempty"`
}

// DesignDocument design document
type DesignDocument struct {
	ID      string                         `json:"_id"`
	Version int                            `json:"-"`
	Hash    string                         `json:"-"`
	Rev     string                         `json:"_rev"`
	Views   map[string]*DesignDocumentView `json:"views"`
}

// Query query
type Query struct {
	text   string
	params []string
}

// DesignDocument design document
type DesignDocumentValidator struct {
	ID      string                 `json:"_id"`
	Version int                    `json:"-"`
	Hash    string                 `json:"-"`
	Rev     string                 `json:"_rev"`
	Schema  map[string]interface{} `json:"schema"`
}
