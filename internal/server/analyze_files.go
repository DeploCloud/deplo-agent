package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

type checkoutSpec struct {
	SchemaVersion           int      `json:"schemaVersion"`
	PreserveTree            bool     `json:"preserveTree"`
	ContentBasenames        []string `json:"contentBasenames"`
	ContentPatterns         []string `json:"contentPatterns"`
	ContentPathSuffixes     []string `json:"contentPathSuffixes"`
	FullCheckoutBasenames   []string `json:"fullCheckoutBasenames"`
	FullCheckoutPatterns    []string `json:"fullCheckoutPatterns"`
	FullCheckoutDirectories []string `json:"fullCheckoutDirectories"`
	PathEnvironment         []string `json:"pathEnvironment"`
}

// Older detectors and unknown schemas use a complete checkout.
func readCheckoutSpec(ctx context.Context, binary string) (*checkoutSpec, error) {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := dockercli.Command(ctx, binary, "--checkout-spec")
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
	if err != nil || output.exceeded || diagnostic.exceeded {
		return nil, nil
	}
	var spec checkoutSpec
	decoder := json.NewDecoder(bytes.NewReader(output.buffer.Bytes()))
	decoder.DisallowUnknownFields()
	if !json.Valid(output.buffer.Bytes()) || decoder.Decode(&spec) != nil || spec.SchemaVersion != 1 || !spec.PreserveTree || len(spec.ContentBasenames)+len(spec.ContentPatterns)+len(spec.ContentPathSuffixes) == 0 {
		return nil, nil
	}
	for _, names := range [][]string{spec.ContentBasenames, spec.ContentPatterns, spec.ContentPathSuffixes, spec.FullCheckoutBasenames, spec.FullCheckoutPatterns, spec.FullCheckoutDirectories, spec.PathEnvironment} {
		if names == nil || len(names) > 256 {
			return nil, nil
		}
	}
	for _, names := range [][]string{spec.ContentBasenames, spec.ContentPatterns, spec.FullCheckoutBasenames, spec.FullCheckoutPatterns, spec.FullCheckoutDirectories} {
		for _, name := range names {
			if name == "" || name == "." || len(name) > 256 || strings.Contains(name, "/") || !analysisRelative(name) {
				return nil, nil
			}
			if _, err := path.Match(name, ""); err != nil {
				return nil, nil
			}
		}
	}
	for _, name := range spec.ContentPathSuffixes {
		if name == "" || name == "." || len(name) > 256 || !analysisRelative(name) || path.Clean(name) != name {
			return nil, nil
		}
	}
	for _, name := range spec.PathEnvironment {
		if !analysisEnvName.MatchString(name) || len(name) > 256 || name == "DEPLOPACK_PROVIDER" {
			return nil, nil
		}
	}
	return &spec, nil
}

func (spec *checkoutSpec) needsContent(name string) bool {
	if matchesCheckoutBasename(name, spec.ContentBasenames, spec.ContentPatterns) {
		return true
	}
	for _, suffix := range spec.ContentPathSuffixes {
		if name == suffix || strings.HasSuffix(name, "/"+suffix) {
			return true
		}
	}
	return false
}

func (spec *checkoutSpec) needsFullCheckout(name string) bool {
	if matchesCheckoutBasename(name, spec.FullCheckoutBasenames, spec.FullCheckoutPatterns) {
		return true
	}
	for _, directory := range strings.Split(path.Dir(name), "/") {
		for _, required := range spec.FullCheckoutDirectories {
			if directory == required {
				return true
			}
		}
	}
	return false
}

func matchesCheckoutBasename(name string, names, patterns []string) bool {
	base := path.Base(name)
	for _, name := range names {
		if base == name {
			return true
		}
	}
	for _, pattern := range patterns {
		if matched, _ := path.Match(pattern, base); matched {
			return true
		}
	}
	return false
}
