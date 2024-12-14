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
	Deleted bool
	Kind    string
	Data    []byte
}

func (doc *Document) CalculateNextVersion() {
	doc.Version = doc.Version + 1
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
		return nil, fmt.Errorf("%s: %w", "payload is not a object", ErrDocumentInvalidInput)
	}

	var (
		id      string
		version int = 0
		kind    string
		deleted bool
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
		v.Del("_id")
	}

	if v.Exists("_rev") {
		rev := strings.ReplaceAll(v.Get("_rev").String(), "\"", "")
		v.Del("_rev")
		version, err = strconv.Atoi(rev)
		if err != nil {
			return &Document{ID: id}, ErrDocumentInvalidRev
		}
	}

	if v.Exists("_deleted") {
		deleted = v.Get("_deleted").GetBool()
		v.Del("_deleted")
	} else {
		deleted = false
	}

	if v.Exists("_kind") {
		kind = strings.ReplaceAll(v.Get("_kind").String(), "\"", "")
	}

	if id == "" && (version != 0 || deleted) {
		return &Document{ID: id}, fmt.Errorf("%s: %w", "document missing _id", ErrDocumentInvalidInput)
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
