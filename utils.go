package main

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
)

func formatDocString(id string, version int, signature string, deleted bool) string {
	if version != 0 {
		if deleted {
			return fmt.Sprintf(`{"_id":"%s","_rev":"%s","_deleted":true}`, id, formatRev(version, signature))
		}
		return fmt.Sprintf(`{"_id":"%s","_rev":"%s"}`, id, formatRev(version, signature))
	}
	if deleted {
		return fmt.Sprintf(`{"_id":"%s","_deleted":true}`, id)
	}
	return fmt.Sprintf(`{"_id":"%s"}`, id)
}

func formatRev(version int, hash string) string {
	return fmt.Sprintf("%d-%s", version, hash)
}

func getRev(rev string) (int, string) {
	fields := strings.Split(strings.ReplaceAll(rev, `"`, ""), "-")
	version, _ := strconv.Atoi(fields[0])
	return version, fields[1]
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
