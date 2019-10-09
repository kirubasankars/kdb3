package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

type Document struct {
	Revision
	Data []byte
}

type Revision struct {
	ID        string
	RevNumber int
	RevID     string
	Deleted   bool
}

func (doc *Document) CalculateRev() {

	doc.RevNumber = doc.RevNumber + 1
	doc.RevID = RandomUUID()
	var meta string
	if len(doc.Data) == 2 {
		meta = fmt.Sprintf(`{"_id":"%s","_rev":"%s"`, doc.ID, formatRev(doc.RevNumber, doc.RevID))
	} else {
		meta = fmt.Sprintf(`{"_id":"%s","_rev":"%s",`, doc.ID, formatRev(doc.RevNumber, doc.RevID))
	}
	data := make([]byte, len(meta))
	copy(data, meta)
	data = append(data, doc.Data[1:]...)
	doc.Data = data
}

func ParseDocument(value []byte) (*Document, error) {
	v, err := fastjson.ParseBytes(value)
	if err != nil {
		return nil, err
	}

	var (
		id        string
		rev       string
		revNumber int
		revID     string
		deleted   bool
	)

	if v.Exists("_id") {
		id = strings.ReplaceAll(v.Get("_id").String(), "\"", "")
	}

	/* 	else {
		id = SequentialUUID()
		v.Set("_id", fastjson.MustParse(fmt.Sprintf(`"%s"`, id)))
		var b []byte
		value = v.MarshalTo(b)
	} */

	if v.Exists("_rev") {
		rev = v.Get("_rev").String()
	} else {
		rev = ""
		revNumber = 0
		revID = ""
	}

	if v.Exists("_deleted") {
		deleted = v.Get("_deleted").GetBool()
	} else {
		deleted = false
	}

	if id == "" && rev != "" {
		return nil, errors.New("invalid_rev")
	}

	if len(rev) > 0 {
		fields := strings.Split(strings.ReplaceAll(rev, "\"", ""), "-")
		revNumber, err = strconv.Atoi(fields[0])
		if err != nil {
			return nil, errors.New("invalid_rev")
		}
		revID = fields[1]
	}

	if v.Exists("_id") {
		v.Del("_id")
	}
	if v.Exists("_rev") {
		v.Del("_rev")
	}

	var b []byte
	value = v.MarshalTo(b)

	doc := &Document{Revision: Revision{ID: id, RevNumber: revNumber, RevID: revID, Deleted: deleted}, Data: value}

	return doc, nil
}
