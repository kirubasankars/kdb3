package main

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxAttachmentSize = 16 * 1024 * 1024

// AttachmentMeta is CouchDB-style stub metadata for one attachment.
type AttachmentMeta struct {
	Name        string
	ContentType string
	Length      int64
	Digest      string
	RevPos      int
}

// Attachment is stored binary plus stub metadata.
type Attachment struct {
	AttachmentMeta
	Data []byte
}

func ValidateAttachmentName(name string) bool {
	if name == "" || !utf8.ValidString(name) || len(name) > 200 {
		return false
	}
	if name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		if r == '/' || r == '\\' || r == 0 {
			return false
		}
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' || r == ' ' {
			continue
		}
		return false
	}
	return true
}

func attachmentDigest(data []byte) string {
	sum := md5.Sum(data)
	return "md5-" + base64.StdEncoding.EncodeToString(sum[:])
}

func attachmentContentMD5(digest string) string {
	return strings.TrimPrefix(digest, "md5-")
}

func formatAttachmentStubs(metas []AttachmentMeta) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, m := range metas {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(jsonEscapeString(m.Name))
		b.WriteString(`:{"content_type":`)
		b.WriteString(jsonEscapeString(m.ContentType))
		b.WriteString(`,"revpos":`)
		b.WriteString(strconv.Itoa(m.RevPos))
		b.WriteString(`,"digest":`)
		b.WriteString(jsonEscapeString(m.Digest))
		b.WriteString(`,"length":`)
		b.WriteString(strconv.FormatInt(m.Length, 10))
		b.WriteString(`,"stub":true}`)
	}
	b.WriteByte('}')
	return b.String()
}

func injectAttachmentStubs(docJSON []byte, metas []AttachmentMeta) []byte {
	if len(metas) == 0 {
		return docJSON
	}
	stubs := formatAttachmentStubs(metas)
	if len(docJSON) == 0 || docJSON[0] != '{' {
		return docJSON
	}
	var b strings.Builder
	b.Grow(len(docJSON) + len(stubs) + 20)
	b.WriteString(`{"_attachments":`)
	b.WriteString(stubs)
	if len(docJSON) == 2 {
		b.WriteByte('}')
		return []byte(b.String())
	}
	b.WriteByte(',')
	b.Write(docJSON[1:])
	return []byte(b.String())
}

func readAttachmentBody(r io.Reader, max int) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > max {
		return nil, ErrAttachmentTooLarge
	}
	return body, nil
}
