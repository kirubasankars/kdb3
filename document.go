package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/valyala/fastjson"
)

var documentPool DocumentPool
var parserPool fastjson.ParserPool

type Document struct {
	ID      string
	Version int
	Deleted bool
	Data    []byte
}

func (doc *Document) CalculateVersion() {

	doc.Version = doc.Version + 1
	var meta string
	if len(doc.Data) == 2 {
		meta = fmt.Sprintf(`{"_id":"%s","_version":%d`, doc.ID, doc.Version)
	} else {
		meta = fmt.Sprintf(`{"_id":"%s","_version":%d,`, doc.ID, doc.Version)
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
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
	}

	if v.Exists("_version") {
		version, _ = strconv.Atoi(v.Get("_version").String())
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

	var b []byte
	value = v.MarshalTo(b)

	doc := documentPool.Get()
	doc.ID = id
	doc.Version = version
	doc.Deleted = deleted
	doc.Data = value

	return doc, nil
}

func (doc *Document) Reset() {
	doc.ID = ""
	doc.Version = 0
	doc.Deleted = false
	doc.Data = []byte("")
}

type DocumentPool struct {
	pool sync.Pool
}

func (pp DocumentPool) Get() *Document {
	return &Document{}
	/*v := pp.pool.Get()
	if v == nil {
		return &Document{}
	}
	doc := v.(*Document)
	doc.Reset()
	return doc*/
}
func (pp DocumentPool) Put(p *Document) {
	//pp.pool.Put(p)
}
