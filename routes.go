package main

import (
	"net/http"

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
			"/_cat/dbs",
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
			"PostDocument",
			"POST",
			"/{db}",
			kdbHandler.PutDocument,
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
			"PostDDocument",
			"POST",
			"/{db}/_design/{docid}",
			kdbHandler.PutDDocument,
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
			"SelectViewSelect",
			"GET",
			"/{db}/_design/{docid}/{view}/{select}",
			kdbHandler.SelectView,
		},
		Route{
			"VacuumDatabase",
			"POST",
			"/{db}/_vacuum",
			kdbHandler.Vacuum,
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
