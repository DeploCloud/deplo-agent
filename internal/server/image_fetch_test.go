package server

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/room"
)

// imageMembers is two members of one cluster that know each other, a holding web:2 and b holding nothing.
func imageMembers(t *testing.T, aHas ...string) (a, b *Service, src, dst *imageHost, downloads *atomic.Int32) {
	t.Helper()
	a, b, _, _, fpA, fpB := memberPair(t)
	src, dst = newImageHost(aHas...), newImageHost()
	a.imageOps, b.imageOps = src.ops(), dst.ops()
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	for _, s := range []*Service{a, b} {
		apply(t, s, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members})
		s.failoverRootOnce.Do(func() { s.failoverRoot = "/docker" })
	}
	downloads = &atomic.Int32{}
	pull, size, stat := failoverPull, imageBytes, room.Stat
	failoverPull = func(context.Context, string) error { downloads.Add(1); return fmt.Errorf("no registry in tests") }
	imageBytes = func(context.Context, string) int64 { return 7 }
	room.Stat = func(string) (int64, int64, bool) { return 80, 100, true }
	t.Cleanup(func() { failoverPull, imageBytes, room.Stat = pull, size, stat })
	// Cleanups run last first: the pulls end before the stubs go.
	t.Cleanup(func() { pullsSettled(t, a); pullsSettled(t, b) })
	return a, b, src, dst, downloads
}

// c42: every member downloaded fise's 3.3 GB from its registry over the lab's one link. A member that lacks an
// image its cluster holds takes it from a member that has it.
func TestHoldFailoverImages_takesAnImageFromAnotherMemberNotTheRegistry(t *testing.T) {
	a, b, _, dst, downloads := imageMembers(t, "ghcr.io/acme/web:2")
	im := &pb.FailoverImage{ImageRef: "ghcr.io/acme/web:2"}
	holdAll(t, a, im)
	pullsSettled(t, a)
	holdAll(t, b, im)
	pullsSettled(t, b)
	if got := holdAll(t, b, im)["ghcr.io/acme/web:2"]; !got.GetHeld() {
		t.Fatalf("b does not hold it: %v", got)
	}
	dst.mu.Lock()
	body := dst.archives["ghcr.io/acme/web:2"]
	dst.mu.Unlock()
	if body != "ARCHIVE of ghcr.io/acme/web:2" || downloads.Load() != 0 {
		t.Fatalf("b loaded %q and downloaded %d times", body, downloads.Load())
	}
}

// A member sends only what its cluster holds for failovers, never any other image it has.
func TestImageGet_anImageNoFailoverHoldsIsNotSent(t *testing.T) {
	a, b, _, _, _ := imageMembers(t, "acme/private:1")
	holdAll(t, a)
	if from, err := b.fetchFromMembers(context.Background(), "acme/private:1"); err == nil {
		t.Fatalf("an image no failover holds came from %s", from)
	}
}

// What no member holds is downloaded by the cluster's first member alone; another waits for it, up to pullerGrace.
func TestHoldFailoverImages_onlyTheFirstMemberDownloadsWhatNoMemberHolds(t *testing.T) {
	a, b, _, _, downloads := imageMembers(t)
	first, other := a, b
	if _, isFirst := a.clusterPeers(); !isFirst {
		first, other = b, a
	}
	im := &pb.FailoverImage{ImageRef: "ghcr.io/acme/web:3"}
	holdAll(t, other, im)
	pullsSettled(t, other)
	if got := holdAll(t, other, im)["ghcr.io/acme/web:3"]; downloads.Load() != 0 || !strings.Contains(got.GetError(), "waiting") {
		t.Fatalf("the second member downloaded (%d) or said %q", downloads.Load(), got.GetError())
	}
	pullsSettled(t, other)
	holdAll(t, first, im)
	pullsSettled(t, first)
	if downloads.Load() != 1 {
		t.Fatalf("the first member downloaded %d times, want once", downloads.Load())
	}
	old := pullerGrace
	pullerGrace = 0
	t.Cleanup(func() { pullerGrace = old })
	other.failover.mu.Lock()
	other.failover.state["ghcr.io/acme/web:3"].lookAt = time.Time{}
	other.failover.mu.Unlock()
	holdAll(t, other, im)
	pullsSettled(t, other)
	if downloads.Load() != 2 {
		t.Fatalf("past the grace the second member still waits: %d downloads", downloads.Load())
	}
}

func digestOf(b []byte) string { s := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(s[:]) }

// fakeRegistry serves web:1 as an index, behind an anonymous token, with one layer.
func fakeRegistry(t *testing.T, layer []byte, corrupt bool) {
	t.Helper()
	config := []byte(`{"architecture":"` + runtime.GOARCH + `"}`)
	manifest, _ := json.Marshal(map[string]any{"mediaType": mtOCIManifest,
		"config": map[string]any{"digest": digestOf(config), "size": len(config)},
		"layers": []any{map[string]any{"digest": digestOf(layer), "size": len(layer)}}})
	index, _ := json.Marshal(map[string]any{"mediaType": mtOCIIndex, "manifests": []any{
		map[string]any{"digest": digestOf(manifest), "platform": map[string]any{"os": "linux", "architecture": runtime.GOARCH}}}})
	blobs := map[string][]byte{digestOf(config): config, digestOf(layer): layer}
	if corrupt {
		blobs[digestOf(layer)] = bytes.ToUpper(layer)
	}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_, _ = io.WriteString(w, `{"token":"t1"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer t1" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="reg",scope="repository:library/web:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch p := strings.TrimPrefix(r.URL.Path, "/v2/library/web/"); {
		case p == "manifests/1":
			_, _ = w.Write(index)
		case p == "manifests/"+digestOf(manifest):
			_, _ = w.Write(manifest)
		case strings.HasPrefix(p, "blobs/") && blobs[strings.TrimPrefix(p, "blobs/")] != nil:
			_, _ = w.Write(blobs[strings.TrimPrefix(p, "blobs/")])
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	base := registryBase
	registryBase = func(string) string { return srv.URL }
	t.Cleanup(func() { registryBase = base })
}

// The one download from a registry writes what `docker load` takes, each blob checked against its digest.
func TestRegistryArchive_readsAnIndexBehindATokenAndChecksEveryBlob(t *testing.T) {
	layer := []byte("a layer's compressed bytes")
	fakeRegistry(t, layer, false)
	var buf bytes.Buffer
	if err := registryArchive(context.Background(), "web:1", &buf); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	tr := tar.NewReader(&buf)
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		files[h.Name], _ = io.ReadAll(tr)
	}
	lname := strings.TrimPrefix(digestOf(layer), "sha256:") + "/layer.tar"
	if !bytes.Equal(files[lname], layer) || !strings.Contains(string(files["manifest.json"]), `"RepoTags":["web:1"]`) {
		t.Fatalf("archive holds %v; manifest %s", len(files), files["manifest.json"])
	}
	fakeRegistry(t, layer, true)
	if err := registryArchive(context.Background(), "web:1", io.Discard); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("a blob that does not match its digest was taken: %v", err)
	}
}

func TestRegistryArchive_readsAReferenceAsDockerDoes(t *testing.T) {
	for ref, want := range map[string]string{
		"postgres":                    "registry-1.docker.io library/postgres latest",
		"docker.io/acme/web:2":        "registry-1.docker.io acme/web 2",
		"ghcr.io/acme/web:2":          "ghcr.io acme/web 2",
		"localhost:5000/web":          "localhost:5000 web latest",
		"ghcr.io/acme/web@sha256:abc": "ghcr.io acme/web sha256:abc",
	} {
		h, r, tag := splitRef(ref)
		if got := h + " " + r + " " + tag; got != want {
			t.Fatalf("%s: %s, want %s", ref, got, want)
		}
	}
}

// c42: the first downloads crossed the lab's one link at its full speed. One is read at registryRate.
func TestRated_readsAtRegistryRate(t *testing.T) {
	old := registryRate
	registryRate = 1 << 20
	t.Cleanup(func() { registryRate = old })
	start := time.Now()
	n, err := io.Copy(io.Discard, rated(context.Background(), bytes.NewReader(make([]byte, 512<<10))))
	if err != nil || n != 512<<10 {
		t.Fatal(n, err)
	}
	if took := time.Since(start); took < 400*time.Millisecond {
		t.Fatalf("512 KB at 1 MB/s took %s", took)
	}
}

// c43 live: eu-1 asked eu-3 for node:22 while eu-3 sent it to eu-2, heard "busy", found no other member with it and,
// first of its cluster, downloaded 410 MB from the registry. A member that holds it but is busy is waited for.
func TestHoldFailoverImages_aBusyMemberIsWaitedForNotTheRegistry(t *testing.T) {
	a, b, src, dst, downloads := imageMembers(t)
	// The asker is the cluster's first member, the one that would download: the holder is the other.
	if _, first := a.clusterPeers(); first {
		a, b, src, dst = b, a, dst, src
	}
	src.mu.Lock()
	src.tags = append(src.tags, "ghcr.io/acme/web:2")
	src.mu.Unlock()
	retry := memberBusyRetry
	memberBusyRetry = 50 * time.Millisecond
	t.Cleanup(func() { memberBusyRetry = retry })
	im := &pb.FailoverImage{ImageRef: "ghcr.io/acme/web:2"}
	holdAll(t, a, im)
	pullsSettled(t, a)
	imageServeSlot <- struct{}{} // a is sending another image
	freed := false
	free := func() {
		if !freed {
			freed = true
			<-imageServeSlot
		}
	}
	t.Cleanup(free)
	holdAll(t, b, im)
	time.Sleep(300 * time.Millisecond)
	if downloads.Load() != 0 {
		t.Fatalf("b downloaded from the registry while a member that holds the image was busy")
	}
	free()
	pullsSettled(t, b)
	dst.mu.Lock()
	body := dst.archives["ghcr.io/acme/web:2"]
	dst.mu.Unlock()
	if body != "ARCHIVE of ghcr.io/acme/web:2" || downloads.Load() != 0 {
		t.Fatalf("b loaded %q and downloaded %d times", body, downloads.Load())
	}
}
