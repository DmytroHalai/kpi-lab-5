package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
    Timeout: 3 * time.Second,
}

func TestBalancer_DistributesRequests(t *testing.T) {
    if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
        t.Skip("Integration test is not enabled")
    }

    const requests = 15
    serverHits := make(map[string]int)

    for i := 0; i < requests; i++ {
        // Меняем query, чтобы балансировщик выбирал разные сервера
        url := fmt.Sprintf("%s/api/v1/some-data?i=%d", baseAddress, i)
        resp, err := client.Get(url)
        if err != nil {
            t.Fatalf("request %d failed: %v", i, err)
        }
        lbFrom := resp.Header.Get("lb-from")
        if lbFrom == "" {
            t.Errorf("request %d: missing lb-from header", i)
        }
        serverHits[lbFrom]++
        resp.Body.Close()
    }

    if len(serverHits) < 2 {
        t.Errorf("expected requests to be distributed to at least 2 servers, got: %v", serverHits)
    }
    t.Logf("Distribution: %v", serverHits)
}

func BenchmarkBalancer(b *testing.B) {
    if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
        b.Skip("Integration test is not enabled")
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?bench=%d", baseAddress, i))
        if err != nil {
            b.Fatalf("request failed: %v", err)
        }
        resp.Body.Close()
    }
}