package main

import (
	"expvar"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"

	"github.com/gorilla/mux"
)

type Route struct {
	Name        string
	Methods     string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

func NewRouter(kdb *KDB) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	kdbHandler := NewKDBHandler(kdb)
	router.Handle("/_debug/vars", expvar.Handler())
	router.HandleFunc("/_debug/pprof", pprof.Index)
	router.Handle("/_debug/allocs", pprof.Handler("allocs"))
	router.Handle("/_debug/block", pprof.Handler("block"))
	router.Handle("/_debug/cmdline", pprof.Handler("cmdline"))
	router.Handle("/_debug/goroutine", pprof.Handler("goroutine"))
	router.Handle("/_debug/heap", pprof.Handler("heap"))
	router.Handle("/_debug/mutex", pprof.Handler("mutex"))
	router.Handle("/_debug/profile", pprof.Handler("profile"))
	router.Handle("/_debug/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("/_debug/trace", pprof.Handler("trace"))

	router.PathPrefix("/_utils").
		Handler(http.StripPrefix("/_utils", http.FileServer(http.Dir("./share/www/"))))

	var routes = Routes{
		Route{
			"Info",
			"GET",
			"/",
			kdbHandler.GetInfo,
		},
		Route{
			"AllDatabases",
			"GET",
			"/_all_dbs",
			kdbHandler.AllDatabases,
		},
		Route{
			"UUID",
			"GET",
			"/_uuids",
			kdbHandler.GetUUIDs,
		},
		Route{
			"GetDatabase",
			"GET",
			"/{db}",
			kdbHandler.GetDatabase,
		},
		Route{
			"PutDatabase",
			"PUT",
			"/{db}",
			kdbHandler.PutDatabase,
		},
		Route{
			"PostDatabase",
			"POST",
			"/{db}",
			kdbHandler.PutDocument,
		},
		Route{
			"DeleteDatabase",
			"DELETE",
			"/{db}",
			kdbHandler.DeleteDatabase,
		},
		Route{
			"DatabaseAllDocs",
			"GET",
			"/{db}/_all_docs",
			kdbHandler.DatabaseAllDocs,
		},
		Route{
			"BulkPutDocuments",
			"POST",
			"/{db}/_bulk_docs",
			kdbHandler.BulkPutDocuments,
		},
		Route{
			"BulkGetDocuments",
			"POST",
			"/{db}/_bulk_gets",
			kdbHandler.BulkGetDocuments,
		},
		Route{
			"DatabaseChanges",
			"GET",
			"/{db}/_changes",
			kdbHandler.DatabaseChanges,
		},
		Route{
			"DatabaseCompact",
			"POST",
			"/{db}/_compact",
			kdbHandler.DatabaseCompact,
		},
		Route{
			"GetDocument",
			"GET",
			"/{db}/{docid}",
			kdbHandler.GetDocument,
		},
		Route{
			"HeadDocument",
			"HEAD",
			"/{db}/{docid}",
			kdbHandler.HeadDocument,
		},
		Route{
			"PutDocument",
			"PUT",
			"/{db}/{docid}",
			kdbHandler.PutDocument,
		},
		Route{
			"DeleteDocument",
			"DELETE",
			"/{db}/{docid}",
			kdbHandler.DeleteDocument,
		},
		Route{
			"GetDDocument",
			"GET",
			"/{db}/_design/{docid}",
			kdbHandler.GetDDocument,
		},
		Route{
			"PutDDocument",
			"PUT",
			"/{db}/_design/{docid}",
			kdbHandler.PutDDocument,
		},
		Route{
			"DeleteDDocument",
			"DELETE",
			"/{db}/_design/{docid}",
			kdbHandler.DeleteDDocument,
		},
		Route{
			"SelectView",
			"GET",
			"/{db}/_design/{docid}/{view}",
			kdbHandler.SelectView,
		},
		Route{
			"SelectView",
			"GET",
			"/{db}/_design/{docid}/{view}/{select}",
			kdbHandler.SelectView,
		},
	}

	for _, route := range routes {
		router.
			Methods(route.Methods).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return router
}
