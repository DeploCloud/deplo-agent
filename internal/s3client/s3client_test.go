package s3client

import (
	"strings"
	"testing"
)

// New derives the endpoint host + TLS flag from the scheme the control plane sends:
// https:// (or no scheme) => TLS; http:// => plaintext (a local MinIO).
func TestNew_endpointSchemeParsing(t *testing.T) {
	cases := []struct {
		endpoint string
		wantErr  bool
	}{
		{"s3.amazonaws.com", false},
		{"https://s3.amazonaws.com", false},
		{"http://127.0.0.1:9000", false},
		{"https://minio.example.com:9000/", false},
		{"", true},
	}
	for _, tc := range cases {
		_, err := New(Config{
			Endpoint:             tc.endpoint,
			Bucket:               "b",
			AccessKey:            "ak",
			SecretKey:            "sk",
			AllowPrivateEndpoint: true,
		})
		if tc.wantErr && err == nil {
			t.Errorf("endpoint %q: expected an error", tc.endpoint)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("endpoint %q: unexpected error %v", tc.endpoint, err)
		}
	}
}

// PathStyle selects path-style bucket addressing (MinIO/compatibles); the
// default is DNS/virtual-host (AWS). A built client just needs to accept both.
func TestNew_pathStyle(t *testing.T) {
	for _, ps := range []bool{true, false} {
		if _, err := New(Config{Endpoint: "s3.example.com", Bucket: "b", AccessKey: "a", SecretKey: "s", PathStyle: ps, AllowPrivateEndpoint: true}); err != nil {
			t.Errorf("pathStyle=%v: %v", ps, err)
		}
	}
}

// TestNew_ssrfGuard proves the default (AllowPrivateEndpoint=false) rejects endpoints
// that resolve to loopback / link-local (incl. the cloud metadata address) / private /
// unspecified addresses, while a public IP passes.
func TestNew_ssrfGuard(t *testing.T) {
	blocked := []string{
		"http://127.0.0.1:9000",    // loopback v4
		"https://[::1]:9000",       // loopback v6
		"http://169.254.169.254",   // link-local / cloud metadata
		"http://10.0.0.5:9000",     // RFC1918
		"https://172.16.3.4",       // RFC1918
		"http://192.168.1.20:9000", // RFC1918
		"http://0.0.0.0:9000",      // unspecified
		"https://[fd00::1]:9000",   // ULA
	}
	for _, ep := range blocked {
		_, err := New(Config{Endpoint: ep, Bucket: "b", AccessKey: "a", SecretKey: "s"})
		if err == nil {
			t.Errorf("endpoint %q: SSRF guard should have rejected it", ep)
			continue
		}
		if !strings.Contains(err.Error(), "SSRF guard") {
			t.Errorf("endpoint %q: expected an SSRF-guard error, got %v", ep, err)
		}
	}

	// A public IP passes with the guard active (no opt-in needed).
	public := []string{"1.1.1.1", "https://8.8.8.8:9000", "203.0.113.10"}
	for _, ep := range public {
		if _, err := New(Config{Endpoint: ep, Bucket: "b", AccessKey: "a", SecretKey: "s"}); err != nil {
			t.Errorf("public endpoint %q: unexpected error %v", ep, err)
		}
	}

	// The opt-in flag bypasses the guard for a private endpoint (a local MinIO).
	if _, err := New(Config{Endpoint: "http://127.0.0.1:9000", Bucket: "b", AccessKey: "a", SecretKey: "s", AllowPrivateEndpoint: true}); err != nil {
		t.Errorf("opt-in private endpoint: unexpected error %v", err)
	}
}

// The quirk flags are an allowlist, and everything else is DROPPED rather than
// refused — an agent mid-rollout will see flags a newer control plane accepted,
// and failing there would make one destination work per host.
func TestParseExtraArgs(t *testing.T) {
	opts, unknown := parseExtraArgs([]string{
		"--s3-force-path-style=true",
		"--s3-sign-accept-encoding=false",
		"--s3-insecure-skip-verify=true",
		"--s3-disable-content-sha256=true",
	})
	if opts.forcePathStyle == nil || !*opts.forcePathStyle {
		t.Errorf("force-path-style: want true, got %v", opts.forcePathStyle)
	}
	// Inverted: signing the header OFF is what stops Go adding it after signing.
	if !opts.noCompression {
		t.Error("sign-accept-encoding=false should disable compression")
	}
	if !opts.insecureSkipVerify || !opts.disableContentSha256 {
		t.Errorf("skip-verify/content-sha256 not applied: %+v", opts)
	}
	if len(unknown) != 0 {
		t.Errorf("allowed flags reported as unknown: %v", unknown)
	}
}

func TestParseExtraArgs_dropsWhatItDoesNotKnow(t *testing.T) {
	opts, unknown := parseExtraArgs([]string{
		"--s3-upload-cutoff=200M", // real rclone flag, not one we map
		"--s3-force-path-style",   // no value
		"--s3-force-path-style=maybe",
		"",
	})
	if opts.forcePathStyle != nil || opts.noCompression || opts.insecureSkipVerify {
		t.Errorf("nothing should have been applied, got %+v", opts)
	}
	if len(unknown) != 4 {
		t.Errorf("want 4 unknown tokens, got %d (%v)", len(unknown), unknown)
	}
}

// A flag that changes how the client is built must not stop it being built.
func TestNew_withExtraArgs(t *testing.T) {
	cfg := Config{
		Endpoint:             "http://127.0.0.1:9000",
		Bucket:               "b",
		AccessKey:            "a",
		SecretKey:            "s",
		AllowPrivateEndpoint: true,
		ExtraArgs: []string{
			"--s3-sign-accept-encoding=false",
			"--s3-insecure-skip-verify=true",
			"--not-a-flag-we-know=true",
		},
	}
	if _, err := New(cfg); err != nil {
		t.Fatalf("New with extra args: %v", err)
	}
}
