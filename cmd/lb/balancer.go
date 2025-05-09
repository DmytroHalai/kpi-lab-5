package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/DmytroHalai/achitecture-practice-4/httptools"
	"github.com/DmytroHalai/achitecture-practice-4/signal"
)

var mu sync.RWMutex
var healthyServers []string

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()
	go monitorHealth()
	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server, ok := getServerIndex(r.URL.Path)
		if !ok {
			http.Error(rw, "No healthy servers", http.StatusServiceUnavailable)
			return
		}
		forward(server, rw, r)
	}))
	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

// monitorHealth This method monitors the status of all servers every 10 seconds.
func monitorHealth() {
	log.Println("Starting health monitor...")
	for {
		var newHealthy []string
		for _, server := range serversPool {
			isHealthy := health(server)
			log.Println(server, "healthy:", isHealthy)
			if isHealthy {
				newHealthy = append(newHealthy, server)
			}
		}
		mu.Lock()
		healthyServers = newHealthy
		mu.Unlock()
		time.Sleep(10 * time.Second)
	}
}

// hash This method hashes the string
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// getServerIndex This method gets a path URL as a param and returns the server, which is to serve the user
func getServerIndex(path string) (string, bool) {
	mu.RLock()
	defer mu.RUnlock()
	if len(healthyServers) == 0 {
		return "", false
	}
	index := int(hash(path)) % len(healthyServers)
	return healthyServers[index], true
}
