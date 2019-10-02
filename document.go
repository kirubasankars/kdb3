package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

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
	} else {
		id = SequentialUUID()
		v.Set("_id", fastjson.MustParse("\""+id+"\""))
		var b []byte
		value = v.MarshalTo(b)
	}
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

	doc := &Document{}
	doc.id = id
	doc.revNumber = revNumber
	doc.revID = revID
	doc.value = value
	doc.deleted = deleted
	doc.jval = v
	return doc, nil
}

type Document struct {
	id        string
	revNumber int
	revID     string
	value     []byte
	deleted   bool
	jval      *fastjson.Value
}

func (doc *Document) CalculateRev() {

	doc.revNumber = doc.revNumber + 1
	doc.revID = RandomUUID()
	meta := fmt.Sprintf(`{"_id":"%s","_rev":"%s",`, doc.id, formatRev(doc.revNumber, doc.revID))

	data := make([]byte, len(meta))
	copy(data, meta)
	data = append(data, doc.value[1:]...)
	doc.value = data
}
