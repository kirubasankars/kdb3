package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"time"
)

func main() {
	var a int64 = math.MaxInt64
	if a+1 < 0 {
		fmt.Print("dadsa")
	}

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
