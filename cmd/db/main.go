package main

import (
  "encoding/json"
  "log"
  "net/http"
  "strings"

  "github.com/DmytroHalai/achitecture-practice-5/datastore"
)

var db *datastore.Db

type putRequest struct {
    Value string `json:"value"`
}

type getResponse struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

func main() {
    var err error
    db, err = datastore.Open("out/db")
    if err != nil {
        log.Fatalf("failed to open db: %v", err)
    }
    defer db.Close()

    http.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
        key := strings.TrimPrefix(r.URL.Path, "/db/")
        if key == "" {
            http.Error(w, "missing key", http.StatusBadRequest)
            return
        }
        switch r.Method {
        case http.MethodGet:
            value, err := db.Get(key)
            if err != nil {
                http.NotFound(w, r)
                return
            }
            resp := getResponse{Key: key, Value: value}
            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(resp)
        case http.MethodPost:
            var req putRequest
            if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                http.Error(w, "bad request", http.StatusBadRequest)
                return
            }
            if err := db.Put(key, req.Value); err != nil {
                http.Error(w, "db error", http.StatusInternalServerError)
                return
            }
            w.WriteHeader(http.StatusNoContent)
        default:
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        }
    })

    log.Println("DB HTTP server started on :8083")
    log.Fatal(http.ListenAndServe(":8083", nil))
}
