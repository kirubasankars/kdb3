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

func NewRouter() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)

	router.PathPrefix("/_utils").
		Handler(http.StripPrefix("/_utils", http.FileServer(http.Dir("./share/www/"))))

	for _, route := range routes {
		router.
			Methods(route.Methods).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return router
}

var routes = Routes{
	Route{
		"AllDatabases",
		"GET",
		"/_all_dbs",
		AllDatabases,
	},
	Route{
		"GetDatabase",
		"GET",
		"/{db}",
		GetDatabase,
	},
	Route{
		"PutDatabase",
		"PUT",
		"/{db}",
		PutDatabase,
	},
	Route{
		"PostDatabase",
		"POST",
		"/{db}",
		PutDocument,
	},
	Route{
		"DeleteDatabase",
		"DELETE",
		"/{db}",
		DeleteDatabase,
	},
	Route{
		"DatabaseAllDocs",
		"GET",
		"/{db}/_all_docs",
		DatabaseAllDocs,
	},
	Route{
		"DatabaseChanges",
		"GET",
		"/{db}/_changes",
		DatabaseChanges,
	},
	Route{
		"DatabaseCompact",
		"POST",
		"/{db}/_compact",
		DatabaseCompact,
	},
	Route{
		"GetDocument",
		"GET",
		"/{db}/{docid}",
		GetDocument,
	},
	Route{
		"PutDocument",
		"PUT",
		"/{db}/{docid}",
		PutDocument,
	},
	Route{
		"DeleteDocument",
		"DELETE",
		"/{db}/{docid}",
		DeleteDocument,
	},
	Route{
		"GetDDocument",
		"GET",
		"/{db}/_design/{docid}",
		GetDDocument,
	},
	Route{
		"PutDDocument",
		"PUT",
		"/{db}/_design/{docid}",
		PutDDocument,
	},
	Route{
		"DeleteDDocument",
		"DELETE",
		"/{db}/_design/{docid}",
		DeleteDDocument,
	},
	Route{
		"SelectView",
		"GET",
		"/{db}/_design/{docid}/{view}/{select}",
		SelectView,
	},
}
