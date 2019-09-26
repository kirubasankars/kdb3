package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

type Document struct {
	id        string
	revNumber int
	revID     string
	value     []byte
	deleted   bool
	jval      *fastjson.Value
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

	doc := &Document{}
	doc.id = id
	doc.revNumber = revNumber
	doc.revID = revID
	doc.value = value
	doc.deleted = deleted
	doc.jval = v
	return doc, nil
}

func (doc *Document) CalculateRev() {
	doc.revNumber = doc.revNumber + 1
	doc.revID = RandomUUID()
	doc.jval.Set("_rev", fastjson.MustParse("\""+strconv.Itoa(doc.revNumber)+"-"+doc.revID+"\""))
	var b []byte
	doc.value = doc.jval.MarshalTo(b)
}

func (doc Document) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintln(&b, doc.id, doc.revNumber, doc.revID, doc.deleted)
	return b.Bytes(), nil
}

func (doc *Document) UnmarshalBinary(data []byte) error {
	b := bytes.NewBuffer(data)
	_, err := fmt.Fscanln(b, &doc.id, &doc.revNumber, &doc.revID, &doc.deleted)
	return err
}

func (doc Document) Encode() ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(doc)
	if err != nil {
		return []byte(""), err
	}
	return data.Bytes(), nil
}

func (doc *Document) Decode(value []byte) error {
	data := bytes.NewBuffer(value)
	dec := gob.NewDecoder(data)
	doc.value = []byte("")
	return dec.Decode(&doc)
}
