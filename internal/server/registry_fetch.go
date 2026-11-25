package server

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/pace"
)

// The one member that downloads a registry image for its cluster does it at registryRate, never at the link's
// full speed, so the Apps and the name servers sharing the link keep answering (c43).

var (
	registryRate   = int64(4 << 20)
	registryClient = &http.Client{Timeout: 0}
	// registryBase turns a host into its API's address; tests point it at their own server.
	registryBase = func(host string) string { return "https://" + host }
)

const (
	mtOCIIndex      = "application/vnd.oci.image.index.v1+json"
	mtDockerList    = "application/vnd.docker.distribution.manifest.list.v2+json"
	mtOCIManifest   = "application/vnd.oci.image.manifest.v1+json"
	mtDockerManifst = "application/vnd.docker.distribution.manifest.v2+json"
)

// splitRef reads a reference the way docker does: Docker Hub and library/ by default, latest when no tag says.
func splitRef(ref string) (host, repo, tag string) {
	name, digest, hasDigest := strings.Cut(ref, "@")
	host = "registry-1.docker.io"
	if i := strings.IndexByte(name, '/'); i > 0 && (strings.ContainsAny(name[:i], ".:") || name[:i] == "localhost") {
		host, name = name[:i], name[i+1:]
	}
	if host == "docker.io" || host == "index.docker.io" {
		host = "registry-1.docker.io"
	}
	tag = "latest"
	if i := strings.LastIndexByte(name, ':'); i > strings.LastIndexByte(name, '/') {
		name, tag = name[:i], name[i+1:]
	}
	if hasDigest {
		tag = digest
	}
	if host == "registry-1.docker.io" && !strings.Contains(name, "/") {
		name = "library/" + name
	}
	return host, name, tag
}

type descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	Platform  *struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
	} `json:"platform,omitempty"`
}

type registrySession struct {
	host, repo, token string
}

// get asks the registry, once more with an anonymous token when it asks for one.
func (r *registrySession) get(ctx context.Context, path, accept string) (*http.Response, error) {
	for try := 0; ; try++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, registryBase(r.host)+"/v2/"+r.repo+path, nil)
		if err != nil {
			return nil, err
		}
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		if r.token != "" {
			req.Header.Set("Authorization", "Bearer "+r.token)
		}
		res, err := registryClient.Do(req)
		if err != nil {
			return nil, err
		}
		if res.StatusCode == http.StatusUnauthorized && try == 0 {
			challenge := res.Header.Get("WWW-Authenticate")
			res.Body.Close()
			if r.token, err = anonymousToken(ctx, challenge); err != nil {
				return nil, err
			}
			continue
		}
		if res.StatusCode != http.StatusOK {
			res.Body.Close()
			return nil, fmt.Errorf("the registry answered %s for %s", res.Status, path)
		}
		return res, nil
	}
}

// anonymousToken answers a Bearer challenge without credentials, as docker does for a public image.
func anonymousToken(ctx context.Context, challenge string) (string, error) {
	scheme, params, _ := strings.Cut(challenge, " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("the registry asks for credentials")
	}
	q := url.Values{}
	var realm string
	for _, p := range strings.Split(params, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(p), "=")
		v = strings.Trim(v, `"`)
		if k == "realm" {
			realm = v
		} else if k == "service" || k == "scope" {
			q.Set(k, v)
		}
	}
	u, err := url.Parse(realm)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("the registry names no token service")
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	res, err := registryClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	var t struct{ Token, AccessToken string }
	if res.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&t) != nil {
		return "", fmt.Errorf("the registry gave no token")
	}
	if t.Token == "" {
		t.Token = t.AccessToken
	}
	return t.Token, nil
}

// manifestFor is the image manifest for this server's platform, through an index when the tag names one.
func (r *registrySession) manifestFor(ctx context.Context, tag string) (config descriptor, layers []descriptor, err error) {
	accept := strings.Join([]string{mtOCIIndex, mtDockerList, mtOCIManifest, mtDockerManifst}, ", ")
	for hop := 0; hop < 2; hop++ {
		res, err := r.get(ctx, "/manifests/"+tag, accept)
		if err != nil {
			return config, nil, err
		}
		var m struct {
			MediaType string       `json:"mediaType"`
			Manifests []descriptor `json:"manifests"`
			Config    descriptor   `json:"config"`
			Layers    []descriptor `json:"layers"`
		}
		err = json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&m)
		res.Body.Close()
		if err != nil {
			return config, nil, fmt.Errorf("the registry's manifest is unreadable: %w", err)
		}
		if len(m.Manifests) == 0 {
			if m.Config.Digest == "" || len(m.Layers) == 0 {
				return config, nil, errors.New("the registry's manifest names no image")
			}
			return m.Config, m.Layers, nil
		}
		tag = ""
		for _, d := range m.Manifests {
			if d.Platform != nil && d.Platform.OS == "linux" && d.Platform.Architecture == runtime.GOARCH {
				tag = d.Digest
				break
			}
		}
		if tag == "" {
			return config, nil, fmt.Errorf("the image has no linux/%s variant", runtime.GOARCH)
		}
	}
	return config, nil, errors.New("the registry's index points to another index")
}

// registryArchive writes ref as an archive `docker load` takes, every blob read at registryRate and checked
// against its digest.
func registryArchive(ctx context.Context, ref string, w io.Writer) error {
	host, repo, tag := splitRef(ref)
	r := &registrySession{host: host, repo: repo}
	config, layers, err := r.manifestFor(ctx, tag)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(w)
	blob := func(d descriptor, name string) error {
		if !strings.HasPrefix(d.Digest, "sha256:") || d.Size < 0 {
			return fmt.Errorf("unsupported digest %q", d.Digest)
		}
		res, err := r.get(ctx, "/blobs/"+d.Digest, "")
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: d.Size, ModTime: time.Unix(0, 0)}); err != nil {
			return err
		}
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(tw, h), io.LimitReader(rated(ctx, res.Body), d.Size))
		if err != nil {
			return err
		}
		if n != d.Size || "sha256:"+hex.EncodeToString(h.Sum(nil)) != d.Digest {
			return fmt.Errorf("blob %s did not match its digest", d.Digest)
		}
		return nil
	}
	hexOf := func(d descriptor) string { return strings.TrimPrefix(d.Digest, "sha256:") }
	if err := blob(config, hexOf(config)+".json"); err != nil {
		return err
	}
	names := make([]string, len(layers))
	for i, l := range layers {
		names[i] = hexOf(l) + "/layer.tar"
		if err := blob(l, names[i]); err != nil {
			return err
		}
	}
	manifest, _ := json.Marshal([]map[string]any{{"Config": hexOf(config) + ".json", "RepoTags": []string{imageName(ref)}, "Layers": names}})
	if err := tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o644, Size: int64(len(manifest)), ModTime: time.Unix(0, 0)}); err != nil {
		return err
	}
	if _, err := tw.Write(manifest); err != nil {
		return err
	}
	return tw.Close()
}

// rated reads r at most registryRate a second, and gives way to this server's Apps as every copy does.
func rated(ctx context.Context, r io.Reader) io.Reader {
	return &rateReader{ctx: ctx, r: pace.Reader(r), start: time.Now()}
}

type rateReader struct {
	ctx   context.Context
	r     io.Reader
	start time.Time
	n     int64
}

func (r *rateReader) Read(p []byte) (int, error) {
	if len(p) > 256<<10 {
		p = p[:256<<10]
	}
	if ahead := time.Duration(float64(r.n)/float64(registryRate)*float64(time.Second)) - time.Since(r.start); ahead > 0 {
		select {
		case <-time.After(ahead):
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}
