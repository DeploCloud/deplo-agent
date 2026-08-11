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
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
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
	// AllowPrivateEndpoint opts OUT of the SSRF guard that rejects an endpoint
	// resolving to a loopback / link-local / private (RFC1918 / ULA) address.
	// Defaults to false (reject): the endpoint arrives off the wire and the agent
	// dials it as root, so a value like 169.254.169.254 (cloud metadata) or
	// 127.0.0.1 must not be reachable unless the destination explicitly opted in.
	AllowPrivateEndpoint bool
	// ExtraArgs are the destination's advanced quirk flags (`--flag=value`), as
	// the operator typed them. See parseExtraArgs: anything not on the allowlist
	// is DROPPED, never an error.
	ExtraArgs []string
}

// extraOptions is what a Config's ExtraArgs actually amount to, once the tokens
// the agent understands have been picked out of them.
type extraOptions struct {
	// forcePathStyle overrides the control plane's provider-derived choice.
	forcePathStyle *bool
	// noCompression stops Go's transport adding `Accept-Encoding: gzip` AFTER
	// the request was signed — the header the signature does not cover, which is
	// what some gateways reject.
	noCompression bool
	// insecureSkipVerify accepts any TLS certificate. For a self-hosted store on
	// a self-signed cert, which is an ordinary thing on a private network.
	insecureSkipVerify bool
	// disableContentSha256 uploads without the streaming content hash, for
	// gateways that reject the trailer it produces.
	disableContentSha256 bool
}

/*
parseExtraArgs reads the flags this agent knows out of a destination's tokens.

It is an ALLOWLIST, and unknown tokens are dropped rather than refused. Both
sides validate — the control plane refuses an unknown flag at the form, so one
arriving here means the two versions disagree, which happens on every fleet
mid-rollout. Failing the backup would make the same destination work on one host
and refuse on the next, for a flag whose entire purpose is a workaround; the
honest behaviour is to apply what this version understands and log the rest.

Accepted shape is `--flag=value` with a boolean value, matching the vocabulary of
the tools operators already reach for when a gateway misbehaves.
*/
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
			// Inverted on purpose: the flag says whether Accept-Encoding takes
			// part in the signature, and turning it OFF is what needs doing.
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
	if err := validateEndpointHost(endpoint, cfg.AllowPrivateEndpoint); err != nil {
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
	// A custom transport only when something asked for one: minio-go's default is
	// tuned for S3 (connection pooling, keep-alives), and replacing it wholesale
	// to flip one bool would cost more than the flag is worth.
	if extra.noCompression || extra.insecureSkipVerify {
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
		opts.Transport = tr
	}
	return minio.New(endpoint, opts)
}

func bucketLookup(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupDNS
}

// validateEndpointHost is the SSRF guard for the S3 endpoint, which arrives off
// the wire (user-controlled) and is dialed by the root agent. It resolves the
// endpoint host and rejects any address in a loopback, unspecified, link-local
// (incl. the cloud metadata address 169.254.169.254), or private (RFC1918 / ULA)
// range — unless the destination explicitly opted in. A public IP, or a hostname
// that resolves only to public addresses, passes (legitimate AWS S3 / R2 / public
// MinIO endpoints keep working). `endpoint` is scheme-stripped host[:port].
func validateEndpointHost(endpoint string, allowPrivate bool) error {
	if allowPrivate {
		return nil
	}
	// Isolate host[:port] from any stray path, then drop the port.
	host := endpoint
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("s3: empty endpoint host")
	}

	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		resolved, err := net.LookupIP(host)
		if err != nil {
			return fmt.Errorf("s3: cannot resolve endpoint host %q: %w", host, err)
		}
		ips = resolved
	}
	// Reject if ANY resolved address is disallowed (conservative — thwarts a
	// hostname that mixes a public A record with an internal one).
	for _, ip := range ips {
		if reason := blockedIPReason(ip); reason != "" {
			return fmt.Errorf("s3: endpoint host %q resolves to a disallowed %s address %s; refusing to connect (SSRF guard)", host, reason, ip)
		}
	}
	return nil
}

// blockedIPReason names the SSRF category an IP falls into, or "" if it is a
// routable public address that is safe to dial.
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

// Upload streams `r` to bucket/key via a multipart PUT, no temp file. Size may
// be -1 (unknown — a piped dump), in which case minio-go buffers part-sized
// chunks. Returns the bytes written so the control plane can record the size.
func Upload(ctx context.Context, cfg Config, key string, r io.Reader) (int64, error) {
	cl, err := New(cfg)
	if err != nil {
		return 0, err
	}
	extra, _ := parseExtraArgs(cfg.ExtraArgs)
	info, err := cl.PutObject(ctx, cfg.Bucket, key, r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		// Some gateways reject the streaming-signature trailer this produces.
		DisableContentSha256: extra.disableContentSha256,
	})
	if err != nil {
		// A multipart upload that dies mid-flight leaves its uploaded parts in the
		// bucket. minio-go tries to abort them, but it does so on the SAME context
		// that just failed, so the abort itself fails whenever the cause was a
		// cancellation - which is exactly the case a canceled backup produces. The
		// parts then sit there: invisible to an object listing, billed all the
		// same, and for a backup that is gigabytes of an artifact nobody wanted.
		//
		// So sweep them on a context of our own. Best effort by construction: the
		// upload's error is the one worth reporting, and a bucket that will not
		// answer this cannot be made to.
		if cerr := ctx.Err(); cerr != nil {
			// context.Background(), not `ctx`: `ctx` is the cancellation we are
			// cleaning up after, and reusing it is precisely the bug this exists to
			// work around.
			if rerr := cl.RemoveIncompleteUpload(context.Background(), cfg.Bucket, key); rerr != nil {
				log.Printf("s3: could not clear the incomplete upload at %q: %v", key, rerr)
			}
		}
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
