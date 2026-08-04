package main

// DatabaseStat stat
type DatabaseStat struct {
	DBName          string `json:"name"`
	UpdateSeq       int64  `json:"update_seq"`
	DocCount        int    `json:"doc_count"`
	DeletedDocCount int    `json:"deleted_doc_count"`
}

// Change is one row from the _changes feed.
type Change struct {
	UpdateSeq int64  `json:"update_seq"`
	ID        string `json:"id"`
	Rev       int    `json:"rev"`
	Deleted   bool   `json:"deleted,omitempty"`
}

// ChangesResult is the one-shot _changes JSON response.
type ChangesResult struct {
	Results []Change `json:"results"`
	LastSeq int64    `json:"last_seq"`
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
	Rev     int                            `json:"_rev"`
	Views   map[string]*DesignDocumentView `json:"views"`
}

// Query query
type Query struct {
	text   string
	params []string
}

// DesignDocument design document
type DesignDocumentValidator struct {
	ID      string `json:"_id"`
	Version int    `json:"-"`
	Rev     int    `json:"_rev"`
}
