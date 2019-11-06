package main

import (
	"crypto/rand"
	"fmt"
)

func formatDocString(id string, version int, deleted bool) string {
	if version != 0 {
		if deleted {
			return fmt.Sprintf(`{"_id" :"%s","_version":%d,"deleted":true}`, id, version)
		}
		return fmt.Sprintf(`{"_id":"%s","_version":%d}`, id, version)
	}
	if deleted {
		return fmt.Sprintf(`{"_id" :"%s","deleted":true}`, id)
	}
	return fmt.Sprintf(`{"_id":"%s"}`, id)
}

func randomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, _ = rand.Read(bytes)
	return bytes
}
