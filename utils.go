package main

import (
	"crypto/rand"
	"fmt"
)

func formatDocString(id string, version int, deleted bool) string {
	if version != 0 {
		if deleted {
			return fmt.Sprintf(`{"_id":"%s","_version":%d,"_deleted":true}`, id, version)
		}
		return fmt.Sprintf(`{"_id":"%s","_version":%d}`, id, version)
	}
	if deleted {
		return fmt.Sprintf(`{"_id":"%s","_deleted":true}`, id)
	}
	return fmt.Sprintf(`{"_id":"%s"}`, id)
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
