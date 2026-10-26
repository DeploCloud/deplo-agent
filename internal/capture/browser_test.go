package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

func TestMain_refusesAnOversizedSpec(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader(`{"url":"https://app.example.com/","password":"` + strings.Repeat("x", maxSpec) + `"}`)
	if code := Main(in, &out); code == 0 {
		t.Fatal("an oversized spec must not run")
	}
	var res Result
	if err := json.Unmarshal(out.Bytes(), &res); err != nil || res.Outcome != OutcomeUnreachable {
		t.Fatalf("want an unreachable result, got %q (%v)", out.String(), err)
	}
}

func TestListenSplice_carriesBothWays(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "egress.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		_, _ = io.Copy(c, c)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := ListenSplice(ctx, sock)
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "ping" {
		t.Fatalf("want ping back, got %q %v", got, err)
	}
}

func TestConsentAssetsDecode(t *testing.T) {
	bundle, err := gunzip(consentBundleGz)
	if err != nil || !bytes.Contains(bundle, []byte("autoconsentReceiveMessage")) {
		t.Fatalf("bundle: %v", err)
	}
	rules, err := gunzip(consentRulesGz)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Autoconsent []json.RawMessage `json:"autoconsent"`
	}
	if err := json.Unmarshal(rules, &parsed); err != nil || len(parsed.Autoconsent) == 0 {
		t.Fatalf("rules: %v (%d)", err, len(parsed.Autoconsent))
	}
	if !json.Valid([]byte(consentConfig)) {
		t.Fatal("consentConfig is not JSON")
	}
}
