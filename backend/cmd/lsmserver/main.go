package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ashmitsharp/lsm-tree/backend/internal/api"
	"github.com/ashmitsharp/lsm-tree/backend/internal/lsm"
	"github.com/gorilla/mux"
)

func main() {
	lsmTree, err := lsm.NewLSMTree()
	if err != nil {
		log.Fatalf("Failed to create LSM-tree: %v", err)
	}

	// // Check WAL file permissions
	// if err := lsmTree.w; err != nil {
	//     log.Printf("Warning: Failed to check WAL permissions: %v", err)
	// }

	log.Println("Attempting to recover from WAL...")
	if err := lsmTree.Recover(); err != nil {
		log.Fatalf("Failed to recover from WAL: %v", err)
	}
	log.Println("WAL recovery completed successfully")

	server := api.NewServer(lsmTree)

	r := mux.NewRouter()
	r.HandleFunc("/get/{key}", server.HandleGet).Methods("GET")
	r.HandleFunc("/put", server.HandlePut).Methods("POST")
	r.HandleFunc("/delete/{key}", server.HandleDelete).Methods("DELETE")

	go func() {
		log.Println("Starting Server on :8080")
		if err := http.ListenAndServe(":8080", r); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	if err := lsmTree.Close(); err != nil {
		log.Printf("Error closing LSM-tree: %v", err)
	}

	log.Println("Server stopped")
}
