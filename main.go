package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	kdb, err := NewKDB()
	if err != nil {
		panic(err)
	}
	router := NewRouter(kdb)

	srv := &http.Server{
		Handler:      router,
		Addr:         "0.0.0.0:8001",
		WriteTimeout: 1 * time.Hour,
		ReadTimeout:  1 * time.Hour,
	}

	log.Fatal(srv.ListenAndServe())

	fmt.Println("Started")
}
