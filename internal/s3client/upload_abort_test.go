package s3client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A canceled upload must not leave its parts in the bucket.
func TestUpload_abortsTheMultipartWhenCanceled(t *testing.T) {
	var (
		created  atomic.Bool
		aborted  = make(chan string, 4)
		partSeen = make(chan struct{}, 1)
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			created.Store(true)
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult><Bucket>deplo-test-bucket</Bucket><Key>%s</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`,
				strings.TrimPrefix(r.URL.Path, "/deplo-test-bucket/"))

		case r.Method == http.MethodGet && q.Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult><Bucket>deplo-test-bucket</Bucket><IsTruncated>false</IsTruncated>`+
				`<Upload><Key>%s</Key><UploadId>upload-1</UploadId></Upload></ListMultipartUploadsResult>`,
				q.Get("prefix"))

		case r.Method == http.MethodDelete && q.Get("uploadId") != "":
			aborted <- q.Get("uploadId")
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPut && q.Get("uploadId") != "":
			select {
			case partSeen <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}

		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := Config{
		Endpoint:             srv.URL,
		Region:               "us-east-1",
		Bucket:               "deplo-test-bucket",
		AccessKey:            "a",
		SecretKey:            "s",
		PathStyle:            true,
		AllowPrivateEndpoint: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Upload(ctx, cfg, "deplo/team/app/x.tar.gz.age", io.LimitReader(zeros{}, 40<<20))
		done <- err
	}()

	select {
	case <-partSeen:
	case err := <-done:
		t.Fatalf("upload ended before any part was sent: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the fake bucket never saw an UploadPart")
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a canceled upload must report an error")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Upload did not return after the cancel")
	}

	if !created.Load() {
		t.Fatal("the upload never became multipart, so this proves nothing")
	}
	select {
	case id := <-aborted:
		if id != "upload-1" {
			t.Errorf("aborted the wrong upload: %q", id)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the canceled upload left its parts in the bucket: no abort arrived")
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
