package main

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
)

func formatDocString(id string, version int, hash string, deleted bool) string {
	var item []string
	item = append(item, fmt.Sprintf(`"_id":"%s"`, id))
	if version != 0 {
		item = append(item, fmt.Sprintf(`"_rev":"%s"`, formatRev(version, hash)))
	}
	if deleted {
		item = append(item, fmt.Sprintf(`"_deleted":true`))
	}
	return fmt.Sprintf(`{%s}`, strings.Join(item, ","))
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

func formatRev(version int, hash string) string {
	return fmt.Sprintf("%d-%s", version, hash)
}

func getVersionAndSignature(rev string) (int, string) {
	fields := strings.Split(strings.ReplaceAll(rev, `"`, ""), "-")
	version, _ := strconv.Atoi(fields[0])
	return version, fields[1]
}
