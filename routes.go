package main

import (
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Route struct {
	Name        string
	Methods     string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

func NewRouter(kdb *KDB, token string) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	kdbHandler := NewKDBHandler(kdb)

	mime.AddExtensionType(".js", "application/javascript; charset=utf-8")
	mime.AddExtensionType(".svg", "image/svg+xml")
	mime.AddExtensionType(".yaml", "application/yaml")
	mime.AddExtensionType(".yml", "application/yaml")

	adminFS, err := fs.Sub(embeddedAdminFS, "share/www")
	if err != nil {
		panic("embedded admin UI missing: " + err.Error())
	}
	router.PathPrefix("/_utils").Handler(http.StripPrefix("/_utils", spaFSHandler(adminFS)))

	docsFS, err := fs.Sub(embeddedDocsFS, "share/openapi")
	if err != nil {
		panic("embedded API docs missing: " + err.Error())
	}
	router.PathPrefix("/_docs").Handler(http.StripPrefix("/_docs", spaFSHandler(docsFS)))

	var routes = Routes{
		Route{"Info", "GET", "/", kdbHandler.GetInfo},
		Route{"AllDatabases", "GET", "/_cat/dbs", kdbHandler.AllDatabases},
		Route{"UUID", "GET", "/_uuids", kdbHandler.GetUUIDs},
		Route{"Metrics", "GET", "/metrics", promhttp.Handler().ServeHTTP},
		Route{"GetDatabase", "GET", "/{db}", kdbHandler.GetDatabase},
		Route{"PutDatabase", "PUT", "/{db}", kdbHandler.PutDatabase},
		Route{"DeleteDatabase", "DELETE", "/{db}", kdbHandler.DeleteDatabase},
		Route{"DatabaseAllDocs", "GET", "/{db}/_all_docs", kdbHandler.DatabaseAllDocs},
		Route{"BulkPutDocuments", "POST", "/{db}/_bulk_docs", kdbHandler.BulkPutDocuments},
		Route{"BulkGetDocuments", "POST", "/{db}/_bulk_gets", kdbHandler.BulkGetDocuments},
		Route{"DatabaseChanges", "GET", "/{db}/_changes", kdbHandler.DatabaseChanges},
		Route{"GetDocument", "GET", "/{db}/{docid}", kdbHandler.GetDocument},
		Route{"HeadDocument", "HEAD", "/{db}/{docid}", kdbHandler.HeadDocument},
		Route{"PostDocument", "POST", "/{db}", kdbHandler.PutDocument},
		Route{"PutDocument", "PUT", "/{db}/{docid}", kdbHandler.PutDocument},
		Route{"DeleteDocument", "DELETE", "/{db}/{docid}", kdbHandler.DeleteDocument},
		Route{"GetDDocument", "GET", "/{db}/_design/{docid}", kdbHandler.GetDDocument},
		Route{"PostDDocument", "POST", "/{db}/_design/{docid}", kdbHandler.PutDDocument},
		Route{"PutDDocument", "PUT", "/{db}/_design/{docid}", kdbHandler.PutDDocument},
		Route{"DeleteDDocument", "DELETE", "/{db}/_design/{docid}", kdbHandler.DeleteDDocument},
		Route{"DryRunView", "POST", "/{db}/_design/{docid}/{view}/_dry_run", kdbHandler.DryRunView},
		Route{"ViewStatus", "GET", "/{db}/_design/{docid}/{view}/_status", kdbHandler.ViewStatus},
		Route{"SelectView", "GET", "/{db}/_design/{docid}/{view}", kdbHandler.SelectView},
		Route{"SelectViewSelect", "GET", "/{db}/_design/{docid}/{view}/{select}", kdbHandler.SelectView},
		Route{"VacuumDatabase", "POST", "/{db}/_vacuum", kdbHandler.Vacuum},
	}

	for _, route := range routes {
		router.
			Methods(route.Methods).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	router.Use(metricsMiddleware)
	router.Use(func(next http.Handler) http.Handler {
		return TokenAuthMiddleware(token, next)
	})

	return router
}

// spaFSHandler serves files from an fs.FS and falls back to index.html for SPA routes.
func spaFSHandler(root fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := path.Clean("/" + r.URL.Path)
		if upath == "/" {
			serveFSFile(w, r, root, "index.html")
			return
		}
		rel := strings.TrimPrefix(upath, "/")
		if rel == "" || strings.Contains(rel, "..") {
			http.NotFound(w, r)
			return
		}
		info, err := fs.Stat(root, rel)
		if err == nil && !info.IsDir() {
			r.URL.Path = "/" + rel
			fileServer.ServeHTTP(w, r)
			return
		}
		serveFSFile(w, r, root, "index.html")
	})
}

func serveFSFile(w http.ResponseWriter, r *http.Request, root fs.FS, name string) {
	f, err := root.Open(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		http.Error(w, "file not seekable", http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, name, stat.ModTime(), rs)
}
