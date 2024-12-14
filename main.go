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

	fmt.Println("Listening on port 8001")

	log.Fatal(srv.ListenAndServe())
}
