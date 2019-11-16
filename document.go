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
	ID        string
	Version   int
	Signature string
	Deleted   bool
	Data      []byte
}

func (doc *Document) CalculateNextVersion() {
	doc.Version = doc.Version + 1
	doc.Signature = fmt.Sprintf("%x", md5.Sum(doc.Data))

	var meta string
	if len(doc.Data) == 2 {
		meta = fmt.Sprintf(`{"_id":"%s","_rev":"%s"`, doc.ID, formatRev(doc.Version, doc.Signature))
	} else {
		meta = fmt.Sprintf(`{"_id":"%s","_rev":"%s",`, doc.ID, formatRev(doc.Version, doc.Signature))
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
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
	}

	if v.Exists("_rev") {
		rev := v.Get("_rev").String()
		fields := strings.Split(strings.ReplaceAll(rev, "\"", ""), "-")
		version, err = strconv.Atoi(fields[0])
		if err != nil {
			return nil, ErrDocInvalidInput
		}
		signature = fields[1]
	}

	if v.Exists("_deleted") {
		deleted = v.Get("_deleted").GetBool()
	} else {
		deleted = false
	}

	if id == "" && version != 0 {
		return nil, fmt.Errorf("%s: %w", "document can't have _rev without _id", ErrDocInvalidInput)
	}

	if v.Exists("_id") {
		v.Del("_id")
	}
	if v.Exists("_rev") {
		v.Del("_rev")
	}
	if v.Exists("_deleted") {
		v.Del("_deleted")
	}

	var b []byte
	value = v.MarshalTo(b)
	doc := &Document{}
	doc.ID = id
	doc.Version = version
	doc.Signature = signature
	doc.Deleted = deleted
	doc.Data = value

	return doc, nil
}
