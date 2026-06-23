// Package s3client is the agent's S3 client — a thin wrapper over minio-go that
// the backup/restore + S3Check/S3Delete RPCs use to move dump bytes to and from
// an S3-compatible bucket WITHOUT a control-plane round-trip (ADR-0007). The
// agent runs on the owning host, has the dump's bytes locally, and uploads them
// itself; the control plane only ever decrypts the creds and builds the object
// key, then hands them over mTLS.
//
// minio-go talks to AWS S3 and every S3-compatible store (MinIO, R2, B2, Wasabi,
// DigitalOcean Spaces, …) the same way. The control plane decides path-style vs
// virtual-host addressing from the destination's provider and passes it in.
package s3client

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config is the decrypted S3 destination the control plane sends over mTLS.
type Config struct {
	Endpoint  string // host[:port], no scheme (minio-go adds it from Secure)
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	// PathStyle forces bucket-in-path addressing (MinIO + many S3-compatibles).
	// AWS uses virtual-host style (PathStyle=false).
	PathStyle bool
}

// New builds a minio client for a destination. The endpoint may arrive with a
// scheme (https://… or http://…); we strip it and derive Secure from it,
// defaulting to TLS when no scheme is given (the safe default for a public S3).
func New(cfg Config) (*minio.Client, error) {
	endpoint := cfg.Endpoint
	secure := true
	if rest, ok := strings.CutPrefix(endpoint, "https://"); ok {
		endpoint = rest
	} else if rest, ok := strings.CutPrefix(endpoint, "http://"); ok {
		endpoint, secure = rest, false
	}
	endpoint = strings.TrimSuffix(endpoint, "/")
	if endpoint == "" {
		return nil, fmt.Errorf("s3: empty endpoint")
	}
	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       secure,
		Region:       cfg.Region,
		BucketLookup: bucketLookup(cfg.PathStyle),
	})
}

func bucketLookup(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupDNS
}

// Upload streams `r` to bucket/key via a multipart PUT, no temp file. Size may
// be -1 (unknown — a piped dump), in which case minio-go buffers part-sized
// chunks. Returns the bytes written so the control plane can record the size.
func Upload(ctx context.Context, cfg Config, key string, r io.Reader) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	info, err := cl.PutObject(ctx, cfg.Bucket, key, r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

// Download opens an object for streaming read. The caller closes the returned
// ReadCloser. A missing object surfaces when the first Read happens (minio-go
// is lazy), so backup/restore wrap it with a clear message.
func Download(ctx context.Context, cfg Config, key string) (io.ReadCloser, error) {
	cl, err := New(cfg)
	if err != nil {
		return nil, err
	}
	obj, err := cl.GetObject(ctx, cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// Check verifies the bucket is reachable AND writable with these creds: it
// confirms the bucket exists, then round-trips a tiny probe object (put +
// remove) so a read-only key is reported as not-writable rather than passing a
// HEAD-only probe. Returns a human message on failure.
func Check(ctx context.Context, cfg Config) error {
	cl, err := New(cfg)
	if err != nil {
		return err
	}
	ok, err := cl.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return fmt.Errorf("reach bucket %q: %w", cfg.Bucket, err)
	}
	if !ok {
		return fmt.Errorf("bucket %q does not exist (or the credentials cannot see it)", cfg.Bucket)
	}
	// Writability probe: a 0-byte object under a reserved key, then delete it.
	probe := ".deplo-s3check"
	if _, err := cl.PutObject(ctx, cfg.Bucket, probe, strings.NewReader(""), 0, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("write probe to bucket %q: %w", cfg.Bucket, err)
	}
	_ = cl.RemoveObject(ctx, cfg.Bucket, probe, minio.RemoveObjectOptions{})
	return nil
}

// DeleteOne removes a single object by exact key. Idempotent: removing a
// missing object is not an error (S3 DELETE is idempotent). Returns 1 when the
// object existed, 0 when it was already absent.
func DeleteOne(ctx context.Context, cfg Config, key string) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	// Stat first so the count reflects reality (DELETE itself can't tell us).
	existed := int64(0)
	if _, serr := cl.StatObject(ctx, cfg.Bucket, key, minio.StatObjectOptions{}); serr == nil {
		existed = 1
	}
	if err := cl.RemoveObject(ctx, cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return 0, err
	}
	return existed, nil
}

// DeletePrefix removes every object whose key starts with `prefix` (a target's
// whole folder, for retention + delete-with-artifacts). Returns the count
// removed. Idempotent: an empty prefix listing deletes nothing and is not an
// error.
func DeletePrefix(ctx context.Context, cfg Config, prefix string) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	objCh := cl.ListObjects(ctx, cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	// Collect the keys first so a list error is surfaced before we report a
	// count, and so RemoveObjects gets a clean channel.
	keys := make([]minio.ObjectInfo, 0, 64)
	for o := range objCh {
		if o.Err != nil {
			return 0, fmt.Errorf("list %q: %w", prefix, o.Err)
		}
		keys = append(keys, o)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	send := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		send <- k
	}
	close(send)
	var firstErr error
	for rerr := range cl.RemoveObjects(ctx, cfg.Bucket, send, minio.RemoveObjectsOptions{}) {
		if rerr.Err != nil && firstErr == nil {
			firstErr = fmt.Errorf("delete %q: %w", rerr.ObjectName, rerr.Err)
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	return int64(len(keys)), nil
}
