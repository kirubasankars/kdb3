package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

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

var parser fastjson.Parser

func ParseDocument(value []byte) (*Document, error) {
	v, err := parser.ParseBytes(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", err, ErrBadJSON)
	}

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

	doc := &Document{ID: id, Version: version, Deleted: deleted, Data: value}
	return doc, nil
}
