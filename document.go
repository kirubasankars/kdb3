package main

import (
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

var parserPool fastjson.ParserPool

type Document struct {
	ID      string
	Version int
	Hash    string
	Kind    string
	Deleted bool
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
		return nil, fmt.Errorf("%s: %w", "payload expected as json object", ErrDocumentInvalidInput)
	}

	var (
		id      string
		version int = 0
		hash    string
		deleted bool
		kind    string
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
		v.Del("_id")
	}

	if v.Exists("_rev") {
		rev := strings.ReplaceAll(v.Get("_rev").String(), "\"", "")
		v.Del("_rev")

		segments := strings.Split(rev, "-")
		if len(segments) == 2 {
			version, err = strconv.Atoi(segments[0])
			if err != nil {
				return nil, fmt.Errorf("%s", "invalid _rev")
			}
			hash = segments[1]
			if len(hash) != 32 {
				return nil, fmt.Errorf("%s", "invalid _rev")
			}
		} else {
			return nil, fmt.Errorf("%s", "invalid _rev")
		}
	}

	if v.Exists("_kind") {
		kind = strings.ReplaceAll(v.Get("_kind").String(), "\"", "")
		v.Del("_kind")
	}

	if v.Exists("_deleted") {
		deleted = v.Get("_deleted").GetBool()
		v.Del("_deleted")
	} else {
		deleted = false
	}

	if id == "" && (version != 0 || hash != ""){
		return nil, fmt.Errorf("%s: %w", "document can't have _rev without _id", ErrDocumentInvalidInput)
	}

	var b []byte
	value = v.MarshalTo(b)

	doc := &Document{}
	doc.ID = id
	doc.Version = version
	doc.Hash = fmt.Sprintf("%x",md5.Sum(value))
	doc.Kind = kind
	doc.Deleted = deleted
	doc.Data = value

	return doc, nil
}
