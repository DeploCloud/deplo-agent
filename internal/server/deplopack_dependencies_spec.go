package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

type dependencySpec struct {
	SchemaVersion int              `json:"schemaVersion"`
	Platform      string           `json:"platform"`
	Dependencies  []packDependency `json:"dependencies"`
	fingerprint   string
}

type packDependency struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	Target       string `json:"target"`
	Image        string `json:"image,omitempty"`
	Version      string `json:"version,omitempty"`
	URL          string `json:"url,omitempty"`
	ChecksumsURL string `json:"checksumsUrl,omitempty"`
	Asset        string `json:"asset,omitempty"`
	Format       string `json:"format,omitempty"`
	Entry        string `json:"entry,omitempty"`
	Executable   string `json:"executable,omitempty"`
}

var dependencyID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var dependencyImage = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,511}$`)

func readDependencySpec(ctx context.Context, binary string) (*dependencySpec, error) {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := dockercli.Command(ctx, binary, "--dependency-spec")
	cmd.Dir = os.TempDir()
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	cmd.WaitDelay = dockercli.WaitDelay
	output := &boundedOutput{limit: 64 << 10, cancel: cancel}
	diagnostic := &boundedOutput{limit: 4 << 10, cancel: cancel}
	cmd.Stdout, cmd.Stderr = output, diagnostic
	err := cmd.Run()
	if parent.Err() != nil {
		return nil, parent.Err()
	}
	if output.exceeded || diagnostic.exceeded {
		return nil, fmt.Errorf("dependency specification exceeds output limit")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, nil
	}
	return parseDependencySpec(output.buffer.Bytes())
}

func parseDependencySpec(data []byte) (*dependencySpec, error) {
	var spec dependencySpec
	if json.Unmarshal(data, &spec) != nil {
		return nil, fmt.Errorf("invalid dependency specification JSON")
	}
	if spec.SchemaVersion != 1 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if !json.Valid(data) || decoder.Decode(&spec) != nil {
		return nil, fmt.Errorf("invalid dependency specification JSON")
	}
	if spec.Platform != "linux/"+runtime.GOARCH || len(spec.Dependencies) == 0 || len(spec.Dependencies) > 32 {
		return nil, fmt.Errorf("invalid dependency platform or resource count")
	}
	seen := map[string]bool{}
	seeds := 0
	for _, dep := range spec.Dependencies {
		if !dependencyID.MatchString(dep.ID) || seen[dep.ID] {
			return nil, fmt.Errorf("invalid or duplicate dependency ID")
		}
		seen[dep.ID] = true
		switch dep.Kind {
		case "docker-image":
			if !dependencyImage.MatchString(dep.Image) || (dep.Target != "host" && dep.Target != "buildkit-cache") || dep.URL != "" || dep.ChecksumsURL != "" || dep.Version != "" || dep.Asset != "" || dep.Format != "" || dep.Entry != "" || dep.Executable != "" {
				return nil, fmt.Errorf("invalid image dependency")
			}
			if dep.Target == "buildkit-cache" {
				seeds++
			}
		case "archive":
			if dep.ID != "mise" || dep.Target != "host" || !detectorTag.MatchString(dep.Version) || strings.HasPrefix(dep.Version, "v") || dep.Image != "" || dep.Format != "tar.gz" || dep.Entry != "mise/bin/mise" || dep.Executable != "mise-"+dep.Version || len(dep.Asset) > 256 || dep.Asset == "" || strings.ContainsAny(dep.Asset, "/\\\r\n\x00") || !dependencyHTTPS(dep.URL) || !dependencyHTTPS(dep.ChecksumsURL) {
				return nil, fmt.Errorf("invalid archive dependency")
			}
		default:
			return nil, fmt.Errorf("unsupported dependency kind")
		}
	}
	for _, id := range []string{"mise", "buildkit", "docker-cli"} {
		dep := spec.resource(id)
		if dep == nil || dep.Target != "host" || (id == "mise" && dep.Kind != "archive") || (id != "mise" && dep.Kind != "docker-image") {
			return nil, fmt.Errorf("missing required dependency %s", id)
		}
	}
	if seeds == 0 {
		return nil, fmt.Errorf("dependency specification has no cache images")
	}
	canonical, _ := json.Marshal(spec)
	spec.fingerprint = fmt.Sprintf("%x", sha256.Sum256(canonical))
	return &spec, nil
}

func dependencyHTTPS(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && len(value) <= 2048 && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.Fragment == "" && parsed.RawQuery == ""
}

func (spec *dependencySpec) resource(id string) *packDependency {
	for i := range spec.Dependencies {
		if spec.Dependencies[i].ID == id {
			return &spec.Dependencies[i]
		}
	}
	return nil
}
