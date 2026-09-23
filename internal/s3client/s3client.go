package s3client

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config is the decrypted S3 destination the control plane sends over mTLS.
type Config struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	// PathStyle forces bucket-in-path addressing (MinIO + many S3-compatibles).
	PathStyle bool
	// AllowPrivateEndpoint opts OUT of the SSRF guard that rejects an endpoint resolving to a loopback / link-local / private (RFC1918 / ULA) address.
	AllowPrivateEndpoint bool
	// ExtraArgs are the destination's advanced quirk flags (`--flag=value`), as the operator typed them.
	ExtraArgs []string
}

type extraOptions struct {
	forcePathStyle       *bool
	noCompression        bool
	insecureSkipVerify   bool
	disableContentSha256 bool
}

func parseExtraArgs(args []string) (extraOptions, []string) {
	var opts extraOptions
	var unknown []string
	for _, raw := range args {
		name, value, ok := strings.Cut(strings.TrimSpace(raw), "=")
		if !ok {
			unknown = append(unknown, raw)
			continue
		}
		on, err := strconv.ParseBool(value)
		if err != nil {
			unknown = append(unknown, raw)
			continue
		}
		switch name {
		case "--s3-force-path-style":
			opts.forcePathStyle = &on
		case "--s3-sign-accept-encoding":
			opts.noCompression = !on
		case "--s3-insecure-skip-verify":
			opts.insecureSkipVerify = on
		case "--s3-disable-content-sha256":
			opts.disableContentSha256 = on
		default:
			unknown = append(unknown, raw)
		}
	}
	return opts, unknown
}

// New builds a minio client for a destination.
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
	vetted, err := validateEndpointHost(endpoint, cfg.AllowPrivateEndpoint)
	if err != nil {
		return nil, err
	}
	extra, unknown := parseExtraArgs(cfg.ExtraArgs)
	if len(unknown) > 0 {
		log.Printf("s3: ignoring %d flag(s) this agent does not understand: %s",
			len(unknown), strings.Join(unknown, " "))
	}
	pathStyle := cfg.PathStyle
	if extra.forcePathStyle != nil {
		pathStyle = *extra.forcePathStyle
	}
	opts := &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       secure,
		Region:       cfg.Region,
		BucketLookup: bucketLookup(pathStyle),
	}
	key := fmt.Sprintf("%t|%s|%t|%t", secure, endpoint, extra.noCompression, extra.insecureSkipVerify)
	if vetted != nil {
		key += "|" + vetted.host + "|" + vetted.ip.String()
	}
	tr, err := sharedTransport(key, func() (*http.Transport, error) {
		tr, err := minio.DefaultTransport(secure)
		if err != nil {
			return nil, fmt.Errorf("s3: build transport: %w", err)
		}
		tr.DisableCompression = extra.noCompression
		if extra.insecureSkipVerify {
			if tr.TLSClientConfig == nil {
				tr.TLSClientConfig = &tls.Config{}
			}
			tr.TLSClientConfig.InsecureSkipVerify = true
		}
		if vetted != nil {
			base := tr.DialContext
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, port, splitErr := net.SplitHostPort(addr)
				if splitErr == nil && strings.EqualFold(host, vetted.host) {
					addr = net.JoinHostPort(vetted.ip.String(), port)
				}
				return base(ctx, network, addr)
			}
		}
		return tr, nil
	})
	if err != nil {
		return nil, err
	}
	opts.Transport = tr
	return minio.New(endpoint, opts)
}

const maxTransports = 16

var (
	transportsMu sync.Mutex
	transports   = map[string]*http.Transport{}
)

// sharedTransport reuses one transport per destination shape, so a sweep of many calls shares its
// connections instead of leaving a pool of idle ones behind every call.
func sharedTransport(key string, build func() (*http.Transport, error)) (*http.Transport, error) {
	transportsMu.Lock()
	defer transportsMu.Unlock()
	if tr := transports[key]; tr != nil {
		return tr, nil
	}
	tr, err := build()
	if err != nil {
		return nil, err
	}
	if len(transports) >= maxTransports {
		for k, old := range transports {
			old.CloseIdleConnections()
			delete(transports, k)
		}
	}
	transports[key] = tr
	return tr, nil
}

type vettedEndpoint struct {
	host string
	ip   net.IP
}

func bucketLookup(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupDNS
}

func validateEndpointHost(endpoint string, allowPrivate bool) (*vettedEndpoint, error) {
	if allowPrivate {
		return nil, nil
	}
	host := endpoint
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, fmt.Errorf("s3: empty endpoint host")
	}

	var ips []net.IP
	literal := net.ParseIP(host) != nil
	if literal {
		ips = []net.IP{net.ParseIP(host)}
	} else {
		resolved, err := net.LookupIP(host)
		if err != nil {
			return nil, fmt.Errorf("s3: cannot resolve endpoint host %q: %w", host, err)
		}
		ips = resolved
	}
	for _, ip := range ips {
		if reason := blockedIPReason(ip); reason != "" {
			return nil, fmt.Errorf("s3: endpoint host %q resolves to a disallowed %s address %s; refusing to connect (SSRF guard)", host, reason, ip)
		}
	}
	if literal || len(ips) == 0 {
		return nil, nil
	}
	return &vettedEndpoint{host: host, ip: ips[0]}, nil
}

func blockedIPReason(ip net.IP) string {
	switch {
	case ip.IsLoopback():
		return "loopback"
	case ip.IsUnspecified():
		return "unspecified"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return "link-local"
	case ip.IsPrivate():
		return "private"
	}
	return ""
}

// Upload streams `r` to bucket/key via a multipart PUT, no temp file.
func Upload(ctx context.Context, cfg Config, key string, r io.Reader) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	extra, _ := parseExtraArgs(cfg.ExtraArgs)
	info, err := cl.PutObject(ctx, cfg.Bucket, key, r, -1, minio.PutObjectOptions{
		ContentType:          "application/octet-stream",
		DisableContentSha256: extra.disableContentSha256,
	})
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			if rerr := cl.RemoveIncompleteUpload(context.Background(), cfg.Bucket, key); rerr != nil {
				log.Printf("s3: could not clear the incomplete upload at %q: %v", key, rerr)
			}
		}
		return 0, err
	}
	return info.Size, nil
}

// Download opens an object for streaming read.
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

// Check verifies the bucket is reachable AND writable with these creds: it confirms the bucket exists, then round-trips a tiny probe object (put + remove) so a read-only key is reported as not-writable rather than passing a HEAD-only probe.
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
	probe := ".deplo-s3check"
	if _, err := cl.PutObject(ctx, cfg.Bucket, probe, strings.NewReader(""), 0, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("write probe to bucket %q: %w", cfg.Bucket, err)
	}
	_ = cl.RemoveObject(ctx, cfg.Bucket, probe, minio.RemoveObjectOptions{})
	return nil
}

// DeleteOne removes a single object by exact key.
func DeleteOne(ctx context.Context, cfg Config, key string) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	existed := int64(0)
	if _, serr := cl.StatObject(ctx, cfg.Bucket, key, minio.StatObjectOptions{}); serr == nil {
		existed = 1
	}
	if err := cl.RemoveObject(ctx, cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return 0, err
	}
	return existed, nil
}

// DeletePrefix removes every object whose key starts with `prefix` (a target's whole folder, for retention + delete-with-artifacts).
func DeletePrefix(ctx context.Context, cfg Config, prefix string) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	objCh := cl.ListObjects(ctx, cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
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
