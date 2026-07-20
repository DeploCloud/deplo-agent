package server

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestAuthenticatedURL_injectsToken(t *testing.T) {
	// The token must NOT ride the clone URL (that lands on argv / in
	// /proc/<pid>/cmdline); it is carried as an out-of-band Authorization header.
	clone, display, authHeader := authenticatedURL("https://github.com/acme/app.git", "tok-secret")
	if strings.Contains(clone, "tok-secret") || strings.Contains(clone, "@") {
		t.Fatalf("credentials must not be on the clone URL (argv leak): %s", clone)
	}
	if strings.Contains(display, "tok-secret") {
		t.Fatalf("display URL leaks the token: %s", display)
	}
	want := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:tok-secret"))
	if authHeader != want {
		t.Fatalf("auth header mismatch: got %q want %q", authHeader, want)
	}
}

func TestAuthenticatedURL_liftsPreAuthenticatedCreds(t *testing.T) {
	// A control-plane GitHub-App URL arrives already authenticated; the creds must
	// be lifted OFF the URL (argv leak) into the header, not left on the URL.
	pre := "https://x-access-token:ghs_existing@github.com/acme/app.git"
	clone, display, authHeader := authenticatedURL(pre, "ignored")
	if strings.Contains(clone, "ghs_existing") || strings.Contains(clone, "@") {
		t.Fatalf("pre-authenticated creds must be stripped from the clone URL: %s", clone)
	}
	if clone != "https://github.com/acme/app.git" {
		t.Fatalf("unexpected clone URL: %s", clone)
	}
	if strings.Contains(display, "ghs_existing") {
		t.Fatalf("display URL leaks the existing token: %s", display)
	}
	want := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ghs_existing"))
	if authHeader != want {
		t.Fatalf("auth header mismatch: got %q want %q", authHeader, want)
	}
}

func TestAuthenticatedURL_publicNoToken(t *testing.T) {
	clone, display, authHeader := authenticatedURL("https://github.com/acme/public.git", "")
	if strings.Contains(clone, "@") {
		t.Fatalf("public URL gained credentials: %s", clone)
	}
	if display != "https://github.com/acme/public.git" {
		t.Fatalf("display mismatch: %s", display)
	}
	if authHeader != "" {
		t.Fatalf("public clone must have no auth header: %q", authHeader)
	}
}

func TestSanitizeGitLine_scrubsToken(t *testing.T) {
	line := "Cloning into https://x-access-token:supersecret@github.com/acme/app.git ..."
	got := sanitizeGitLine(line)
	if strings.Contains(got, "supersecret") {
		t.Fatalf("token survived sanitisation: %s", got)
	}
	if !strings.Contains(got, "x-access-token:***@") {
		t.Fatalf("expected masked token marker, got: %s", got)
	}
}
