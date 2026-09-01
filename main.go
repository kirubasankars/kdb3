package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	cfg := LoadConfig()
	if cfg.ShowVersion {
		fmt.Printf("kdb3 %s (%s)\n", Version, GitHash)
		os.Exit(0)
	}

	kdb, err := NewKDBWithDataDir(cfg.DataDir)
	if err != nil {
		log.Fatalf("failed to start kdb3: %v", err)
	}
	router := NewRouter(kdb, cfg.Token)

	srv := &http.Server{
		Handler:      router,
		Addr:         cfg.Addr,
		WriteTimeout: cfg.WriteTimeout,
		ReadTimeout:  cfg.ReadTimeout,
	}

	fmt.Printf("Listening on %s (data=%s)\n", cfg.Addr, cfg.DataDir)
	log.Fatal(srv.ListenAndServe())
}
