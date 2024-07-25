package api

import (
	"encoding/json"
	"net/http"

	"github.com/ashmitsharp/lsm-tree/backend/internal/lsm"
	"github.com/gorilla/mux"
)

type Server struct {
	lsmTree *lsm.LSMTree
}

func NewServer(lsmTree *lsm.LSMTree) *Server {
	return &Server{lsmTree: lsmTree}
}

func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, found := s.lsmTree.Get(key)
	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"value": value})
}

func (s *Server) HandlePut(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	key, ok := data["key"]
	if !ok {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	value, ok := data["value"]
	if !ok {
		http.Error(w, "Value is required", http.StatusBadRequest)
		return
	}

	if err := s.lsmTree.Put(key, value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) HandleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if err := s.lsmTree.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
