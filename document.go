package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"strings"

	"github.com/valyala/fastjson"
)

var parserPool fastjson.ParserPool

type Document struct {
	ID      string
	Version int
	Hash    string
	Deleted bool
	Data    []byte
}

func (doc *Document) CalculateNextVersion() {
	doc.Version = doc.Version + 1
}

func (doc *Document) GetRev() string {
	return fmt.Sprintf("%d-%s", doc.Version, doc.Hash)
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
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
		v.Del("_id")
	}

	if v.Exists("_rev") {
		rev := strings.ReplaceAll(v.Get("_rev").String(), "\"", "")
		v.Del("_rev")
		version, hash, err = SplitRev(rev)
		if err != nil {
			return nil, ErrDocumentInvalidRev
		}
	}

	if v.Exists("_deleted") {
		deleted = v.Get("_deleted").GetBool()
		v.Del("_deleted")
	} else {
		deleted = false
	}

	if id == "" && (version != 0 || hash != "") {
		return nil, fmt.Errorf("%s: %w", "document can't have _rev without _id", ErrDocumentInvalidInput)
	}

	var b []byte
	value = v.MarshalTo(b)
	doc := &Document{}
	doc.ID = id
	doc.Version = version

	var buf bytes.Buffer
	buf.Write(value)
	doc.Hash = fmt.Sprintf("%x", md5.Sum(buf.Bytes()))

	doc.Deleted = deleted
	doc.Data = value

	return doc, nil
}
