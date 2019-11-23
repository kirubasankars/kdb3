package main

import (
	"crypto/md5"
	"fmt"
	"strings"

	"github.com/valyala/fastjson"
)

var parserPool fastjson.ParserPool

type Document struct {
	ID        string
	Version   int
	Signature string
	Kind      string
	Deleted   bool
	Data      []byte
}

func (doc *Document) CalculateNextVersion() {
	doc.Version = doc.Version + 1
	doc.Signature = fmt.Sprintf("%x", md5.Sum(doc.Data))

	var meta string
	meta = fmt.Sprintf(`{"_id":"%s","_rev":"%s"`, doc.ID, formatRev(doc.Version, doc.Signature))
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
		id        string
		version   int = 0
		signature string
		deleted   bool
		kind      string
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
		v.Del("_id")
	}

	if v.Exists("_rev") {
		version, signature = getVersionAndSignature(v.Get("_rev").String())
		v.Del("_rev")
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

	if id == "" && version != 0 {
		return nil, fmt.Errorf("%s: %w", "document can't have version without _id", ErrDocInvalidInput)
	}

	var b []byte
	value = v.MarshalTo(b)

	doc := &Document{}
	doc.ID = id
	doc.Version = version
	doc.Signature = signature
	doc.Kind = kind
	doc.Deleted = deleted
	doc.Data = value

	return doc, nil
}
