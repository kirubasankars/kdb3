package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// removeSQLiteFiles deletes a database file and its WAL/SHM sidecars.
// Leaving -wal/-shm behind after rename/delete causes SQLITE_READONLY_DBMOVED (1032).
func removeSQLiteFiles(dbPath string) {
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")
}

func jsonEscapeString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				b.WriteString(`\u00`)
				const hex = "0123456789abcdef"
				b.WriteByte(hex[c>>4])
				b.WriteByte(hex[c&0xf])
			} else {
				b.WriteByte(c)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func prependDocMeta(id string, version int, data []byte) []byte {
	meta := `{"_id":` + jsonEscapeString(id) + `,"_rev":` + strconv.Itoa(version)
	if len(data) != 2 {
		meta += ","
	}
	out := make([]byte, 0, len(meta)+len(data))
	out = append(out, meta...)
	if len(data) > 0 {
		out = append(out, data[1:]...)
	}
	return out
}

func formatDocumentString(id string, version int, deleted bool) string {
	var b strings.Builder
	b.Grow(64 + len(id))
	b.WriteString(`{"_id":`)
	b.WriteString(jsonEscapeString(id))
	b.WriteString(`,"_rev":`)
	b.WriteString(strconv.Itoa(version))
	if deleted {
		b.WriteString(`,"_deleted":true`)
	}
	b.WriteByte('}')
	return b.String()
}

func OK(ok bool, json string) string {
	if ok {
		return fmt.Sprintf(`{"ok":true,%s`, json[1:])
	}
	return fmt.Sprintf(`{"ok":false,%s`, json[1:])
}

func randomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, _ = rand.Read(bytes)
	return bytes
}

func SplitRev(rev string) (int, string, error) {
	if rev != "" {
		segments := strings.Split(rev, "-")
		if len(segments) == 2 {
			version, err := strconv.Atoi(segments[0])
			if err != nil {
				return 0, "", fmt.Errorf("%s", "invalid _rev")
			}
			hash := segments[1]
			if len(hash) != 32 {
				return 0, "", fmt.Errorf("%s", "invalid _rev")
			}
			return version, hash, nil
		}
	}
	return 0, "", fmt.Errorf("%s", "invalid _rev")
}
