package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

var kdb *KDBEngine

func main() {
	var err error
	kdb, err = NewKDB()
	if err != nil {
		panic(err)
	}

	router := NewRouter()

	srv := &http.Server{
		Handler:      router,
		Addr:         "127.0.0.1:8001",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())

	fmt.Println("Started")
}
