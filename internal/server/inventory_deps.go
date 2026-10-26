package server

import (
	"archive/tar"
	"context"
	"encoding/json"
	"io"
	"path"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// syftDoc is the part of syft's JSON the package list and the dependency graph come from.
type syftDoc struct {
	Artifacts []struct {
		ID, Name, Version, Type, Purl, FoundBy string
		Locations                              []struct {
			Path string `json:"path"`
		} `json:"locations"`
	} `json:"artifacts"`
	Relationships []struct {
		Parent, Child, Type string
	} `json:"artifactRelationships"`
	Distro struct {
		ID        string `json:"id"`
		VersionID string `json:"versionID"`
	} `json:"distro"`
	Descriptor struct {
		Version string `json:"version"`
	} `json:"descriptor"`
}

const (
	lockCataloger      = "javascript-lock-cataloger"
	installedCataloger = "javascript-package-cataloger"
)

// depMark is what the lockfile says about one installed package.
type depMark struct {
	buildOnly bool
	via       []string
}

// appManifest is the app's own package.json: the roots of what runs and of what only builds.
type appManifest struct {
	Dependencies         map[string]string `json:"dependencies"`
	OptionalDependencies map[string]string `json:"optionalDependencies"`
	PeerDependencies     map[string]string `json:"peerDependencies"`
	DevDependencies      map[string]string `json:"devDependencies"`
	Workspaces           json.RawMessage   `json:"workspaces"`
}

// markDeps walks the lockfile graph from the app's dependencies. A package reached only from its
// devDependencies is build-only; a reached one carries the chain that pulls it in. Nothing is
// marked without a manifest and a lockfile, nor for workspaces, whose roots live elsewhere.
func markDeps(doc *syftDoc, workingDir string, manifest []byte) map[string]depMark {
	wd := strings.TrimSuffix(workingDir, "/")
	if wd == "" || len(manifest) == 0 {
		return nil
	}
	var m appManifest
	if json.Unmarshal(manifest, &m) != nil || len(m.Workspaces) > 0 && string(m.Workspaces) != "null" {
		return nil
	}
	under := func(locs []struct {
		Path string `json:"path"`
	}) bool {
		return len(locs) > 0 && strings.HasPrefix(locs[0].Path, wd+"/")
	}

	name := map[string]string{}
	byKey := map[string][]string{}
	npmLock := false
	for _, a := range doc.Artifacts {
		if a.FoundBy != lockCataloger || !under(a.Locations) {
			continue
		}
		name[a.ID] = a.Name
		byKey[a.Name+"@"+a.Version] = append(byKey[a.Name+"@"+a.Version], a.ID)
		if path.Base(a.Locations[0].Path) == "package-lock.json" {
			npmLock = true
		}
	}
	if len(name) == 0 {
		return nil
	}
	// A dependency-of B: B needs A.
	deps := map[string][]string{}
	for _, r := range doc.Relationships {
		if r.Type == "dependency-of" && name[r.Parent] != "" && name[r.Child] != "" {
			deps[r.Child] = append(deps[r.Child], r.Parent)
		}
	}
	walk := func(roots map[string]string) map[string]string {
		prev := map[string]string{}
		var queue []string
		for id, n := range name {
			if _, ok := roots[n]; ok {
				prev[id] = ""
				queue = append(queue, id)
			}
		}
		for len(queue) > 0 {
			id := queue[0]
			queue = queue[1:]
			for _, d := range deps[id] {
				if _, seen := prev[d]; !seen {
					prev[d] = id
					queue = append(queue, d)
				}
			}
		}
		return prev
	}
	prodRoots := map[string]string{}
	for _, set := range []map[string]string{m.Dependencies, m.OptionalDependencies, m.PeerDependencies} {
		for k, v := range set {
			prodRoots[k] = v
		}
	}
	prod, dev := walk(prodRoots), walk(m.DevDependencies)

	out := map[string]depMark{}
	for _, a := range doc.Artifacts {
		if a.FoundBy != installedCataloger || a.Type != "npm" || !under(a.Locations) {
			continue
		}
		ids := byKey[a.Name+"@"+a.Version]
		reached, inDev := "", false
		for _, id := range ids {
			if _, ok := prod[id]; ok && reached == "" {
				reached = id
			}
			if _, ok := dev[id]; ok {
				inDev = true
			}
		}
		switch {
		case reached != "":
			var via []string
			for p := prod[reached]; p != ""; p = prod[p] {
				via = append(via, name[p])
			}
			out[a.ID] = depMark{via: via}
		case inDev || (npmLock && len(ids) == 0):
			// npm's lockfile reaches syft without its dev packages, so absent there means dev.
			out[a.ID] = depMark{buildOnly: true}
		}
	}
	return out
}

// readAppManifest copies <workingDir>/package.json out of an image without running it.
var readAppManifest = func(ctx context.Context, imageID, workingDir string) []byte {
	wd := strings.TrimSuffix(workingDir, "/")
	if wd == "" {
		return nil
	}
	res, err := dockercli.Run(ctx, time.Minute, "create", "--pull=never", imageID, "/deplo-never-runs")
	if err != nil || res.Code != 0 {
		return nil
	}
	ctr := strings.TrimSpace(res.Stdout)
	defer dockercli.Run(context.Background(), time.Minute, "rm", "-f", ctr)
	res, err = dockercli.Run(ctx, time.Minute, "cp", ctr+":"+wd+"/package.json", "-")
	if err != nil || res.Code != 0 {
		return nil
	}
	tr := tar.NewReader(strings.NewReader(res.Stdout))
	if h, err := tr.Next(); err != nil || h.Size > 1<<20 {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(tr, 1<<20))
	return b
}
