package s3client

import (
	"fmt"
	"net/http"
	"testing"
)

func TestSharedTransportIsReusedAndCapped(t *testing.T) {
	builds := 0
	build := func() (*http.Transport, error) { builds++; return &http.Transport{}, nil }
	a, _ := sharedTransport("test|one", build)
	b, _ := sharedTransport("test|one", build)
	if a != b || builds != 1 {
		t.Fatalf("one destination built %d transports", builds)
	}
	for i := 0; i < maxTransports*2; i++ {
		if _, err := sharedTransport(fmt.Sprintf("test|%d", i), build); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(transports); n > maxTransports {
		t.Fatalf("%d transports cached, cap is %d", n, maxTransports)
	}
}
