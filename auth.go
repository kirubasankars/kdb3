package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// TokenAuthMiddleware requires Authorization: Bearer <token> when token is non-empty.
// Paths under /_utils and /_docs are left public so the admin UI and Swagger UI can load;
// those UIs send the token for API calls (Authorize in Swagger).
func TokenAuthMiddleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_utils") ||
			strings.HasPrefix(r.URL.Path, "/_docs") ||
			r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) || !subtleConstEq(auth[len(prefix):], token) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("WWW-Authenticate", `Bearer realm="kdb3"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized","reason":"missing or invalid bearer token"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// subtleConstEq compares tokens in constant time via SHA-256 digests (length-independent).
func subtleConstEq(a, b string) bool {
	ah := sha256.Sum256([]byte(a))
	bh := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ah[:], bh[:]) == 1
}
