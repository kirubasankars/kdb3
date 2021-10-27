package main

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
)

func formatDocumentString(id string, version int, hash string, deleted bool) string {
	var item []string
	item = append(item, fmt.Sprintf(`"_id":"%s"`, id))
	item = append(item, fmt.Sprintf(`"_rev":"%d-%s"`, version, hash))
	if deleted {
		item = append(item, `"_deleted":true`)
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
