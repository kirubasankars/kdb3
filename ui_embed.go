package main

import "embed"

// Admin UI assets served at /_utils/. Edit share/www then rebuild.
//
//go:embed all:share/www
var embeddedAdminFS embed.FS

// OpenAPI + Swagger UI assets served at /_docs/. Edit share/openapi then rebuild.
//
//go:embed all:share/openapi
var embeddedDocsFS embed.FS
