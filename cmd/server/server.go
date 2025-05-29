package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/DmytroHalai/achitecture-practice-5/httptools"
	"github.com/DmytroHalai/achitecture-practice-5/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

const teamKey = "object261"
const dbAddr = "http://db:8083"

func main() {
	now := time.Now().Format("2006-01-02")
	postBody, _ := json.Marshal(map[string]string{"value": now})
	_, _ = http.Post(fmt.Sprintf("%s/db/%s", dbAddr, teamKey), "application/json", bytes.NewReader(postBody))

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			key = teamKey
		}

		resp, err := http.Get(fmt.Sprintf("%s/db/%s", dbAddr, key))
		if err != nil {
			http.Error(rw, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		if resp.StatusCode != http.StatusOK {
			http.Error(rw, "db error", http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		rw.Write(body)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
