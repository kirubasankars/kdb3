package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

var parserPool fastjson.ParserPool

type Document struct {
	ID      string
	Version int
	Kind    string
	Deleted bool
	Data    []byte
}

func (doc *Document) CalculateNextVersion() {
	doc.Version = doc.Version + 1
	var meta string
	meta = fmt.Sprintf(`{"_id":"%s","_version":%d`, doc.ID, doc.Version)
	if doc.Kind != "" {
		meta = fmt.Sprintf(`%s,"_kind":"%s"`, meta, doc.Kind)
	}
	if len(doc.Data) != 2 {
		meta = meta + ","
	}
	data := make([]byte, len(meta))
	copy(data, meta)
	data = append(data, doc.Data[1:]...)
	doc.Data = data
}

func ParseDocument(value []byte) (*Document, error) {
	parser := parserPool.Get()
	v, err := parser.ParseBytes(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", err, ErrBadJSON)
	}
	parserPool.Put(parser)

	obj := v.GetObject()
	if obj == nil {
		return nil, fmt.Errorf("%s: %w", "payload expected as json object", ErrDocInvalidInput)
	}

	var (
		id      string
		version int = 0
		deleted bool
		kind    string
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
	}

	if v.Exists("_version") {
		version, _ = strconv.Atoi(v.Get("_version").String())
	}

	if v.Exists("_kind") {
		kind = strings.ReplaceAll(v.Get("_kind").String(), "\"", "")
	}

	if v.Exists("_deleted") {
		deleted = v.Get("_deleted").GetBool()
	} else {
		deleted = false
	}

	if id == "" && version != 0 {
		return nil, fmt.Errorf("%s: %w", "document can't have version without _id", ErrDocInvalidInput)
	}

	if v.Exists("_id") {
		v.Del("_id")
	}
	if v.Exists("_version") {
		v.Del("_version")
	}
	if v.Exists("_kind") {
		v.Del("_kind")
	}
	if v.Exists("_deleted") {
		v.Del("_deleted")
	}

	var b []byte
	value = v.MarshalTo(b)

	doc := &Document{}
	doc.ID = id
	doc.Version = version
	doc.Kind = kind
	doc.Deleted = deleted
	doc.Data = value

	return doc, nil
}
