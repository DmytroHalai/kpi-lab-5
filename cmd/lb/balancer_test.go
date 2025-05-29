package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Tests for hash()

func TestHashDeterministic(t *testing.T) {
	input := "/user/42"
	if hash(input) != hash(input) {
		t.Error("Hash is not deterministic")
	}
}

func TestHashCollisionAvoidance(t *testing.T) {
	paths := []string{"/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i"}
	hashes := make(map[uint32]string)

	for _, path := range paths {
		h := hash(path)
		if existing, ok := hashes[h]; ok {
			t.Errorf("Hash collision: %s and %s have same hash %d", path, existing, h)
		}
		hashes[h] = path
	}
}

// Tests for getServerIndex()

func TestGetServerIndexSingleServer(t *testing.T) {
	mu.Lock()
	healthyServers = []string{"server1:8080"}
	mu.Unlock()

	server, ok := getServerIndex("/test")
	if !ok || server != "server1:8080" {
		t.Errorf("Expected server1:8080, got %s", server)
	}
}

func TestGetServerIndexEmptyList(t *testing.T) {
	mu.Lock()
	healthyServers = []string{}
	mu.Unlock()

	_, ok := getServerIndex("/test")
	if ok {
		t.Error("Expected failure with empty healthyServers")
	}
}

func TestGetServerIndexDistribution(t *testing.T) {
	mu.Lock()
	healthyServers = []string{"s1:8080", "s2:8080", "s3:8080"}
	mu.Unlock()

	pathToServer := make(map[string]string)
	paths := []string{"/a", "/b", "/c", "/d", "/e", "/f", "/g"}

	for _, path := range paths {
		server, ok := getServerIndex(path)
		if !ok {
			t.Fatalf("No server for path %s", path)
		}
		pathToServer[path] = server
	}

	if len(uniqueValues(pathToServer)) < 2 {
		t.Errorf("Expected paths to distribute across multiple servers, got %v", pathToServer)
	}
}

func TestGetServerIndexConsistency(t *testing.T) {
	mu.Lock()
	healthyServers = []string{"s1:8080", "s2:8080", "s3:8080"}
	mu.Unlock()

	path := "/user/profile"
	s1, ok1 := getServerIndex(path)
	s2, ok2 := getServerIndex(path)

	if !ok1 || !ok2 {
		t.Fatal("Expected valid server index")
	}
	if s1 != s2 {
		t.Errorf("Expected consistent result, got %s and %s", s1, s2)
	}
}

func TestGetServerIndexConcurrency(t *testing.T) {
	mu.Lock()
	healthyServers = []string{"s1:8080", "s2:8080"}
	mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			getServerIndex("/path/" + string(rune(i)))
		}(i)
	}
	wg.Wait()
}

// Tests for forward() with mock-server

func TestForwardSuccess(t *testing.T) {
	mockBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test", "true")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("mock ok"))
	}))
	defer mockBackend.Close()

	dst := strings.TrimPrefix(mockBackend.URL, "http://")

	req := httptest.NewRequest("GET", "http://lb/", nil)
	rr := httptest.NewRecorder()

	err := forward(dst, rr, req)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", rr.Code)
	}

	body, _ := io.ReadAll(rr.Body)
	if string(body) != "mock ok" {
		t.Errorf("Expected body 'mock ok', got %s", string(body))
	}

	if rr.Header().Get("X-Test") != "true" {
		t.Error("Expected X-Test header to be forwarded")
	}
}

func TestForwardUnavailable(t *testing.T) {
	dst := "localhost:9999"

	req := httptest.NewRequest("GET", "http://lb/", nil)
	rr := httptest.NewRecorder()

	err := forward(dst, rr, req)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rr.Code)
	}
}

// Helper function to extract unique values from a map

func uniqueValues(m map[string]string) []string {
	set := make(map[string]struct{})
	for _, v := range m {
		set[v] = struct{}{}
	}
	unique := make([]string, 0, len(set))
	for v := range set {
		unique = append(unique, v)
	}
	return unique
}
