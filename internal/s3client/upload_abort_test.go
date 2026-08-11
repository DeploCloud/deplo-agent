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
//
// This is the guarantee a canceled BACKUP rests on. minio-go does try to abort a
// multipart upload that failed, but it issues that abort on the SAME context
// that just failed - so when the cause was a cancellation, the abort is
// cancelled too and the parts stay: invisible to an object listing, billed all
// the same, and for a backup that is gigabytes of an artifact nobody wanted.
//
// The fake bucket below answers the three calls a multipart upload makes and
// records whether the abort ever arrived. Without the sweep in Upload it never
// does, and this test fails.
func TestUpload_abortsTheMultipartWhenCanceled(t *testing.T) {
	var (
		created  atomic.Bool
		aborted  = make(chan string, 4)
		partSeen = make(chan struct{}, 1)
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		// CreateMultipartUpload: POST /bucket/key?uploads=
		case r.Method == http.MethodPost && q.Has("uploads"):
			created.Store(true)
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult><Bucket>deplo-test-bucket</Bucket><Key>%s</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`,
				strings.TrimPrefix(r.URL.Path, "/deplo-test-bucket/"))

		// ListMultipartUploads: GET /bucket/?uploads=&prefix=key
		// RemoveIncompleteUpload finds the pending upload this way before aborting
		// it, so the sweep is list-then-delete rather than a blind DELETE.
		case r.Method == http.MethodGet && q.Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult><Bucket>deplo-test-bucket</Bucket><IsTruncated>false</IsTruncated>`+
				`<Upload><Key>%s</Key><UploadId>upload-1</UploadId></Upload></ListMultipartUploadsResult>`,
				q.Get("prefix"))

		// AbortMultipartUpload: DELETE /bucket/key?uploadId=...
		case r.Method == http.MethodDelete && q.Get("uploadId") != "":
			aborted <- q.Get("uploadId")
			w.WriteHeader(http.StatusNoContent)

		// UploadPart: hold it open so the caller's cancel lands mid-flight, which
		// is the situation a stopped backup produces.
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
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		Bucket:    "deplo-test-bucket",
		AccessKey: "a",
		SecretKey: "s",
		PathStyle: true,
		// httptest listens on loopback, which the SSRF guard rejects by design.
		AllowPrivateEndpoint: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Unknown length (-1) is what a backup sends: it is a live dump, not a
		// file whose size anyone knows. That is also what forces multipart.
		_, err := Upload(ctx, cfg, "deplo/team/app/x.tar.gz.age", io.LimitReader(zeros{}, 40<<20))
		done <- err
	}()

	// Cancel once a part is actually in flight - before that there is no
	// multipart upload to leak.
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

// zeros is an endless reader, so the upload has something to stream.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
