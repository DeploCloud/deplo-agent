package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/capture"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/pkgupdate"
)

// The image the package manager runs in; corepack inside it fetches pnpm and yarn berry.
const pkgUpdateImage = "node:22-alpine"

var (
	npmName    = regexp.MustCompile(`^(@[A-Za-z0-9][A-Za-z0-9._~-]*/)?[A-Za-z0-9][A-Za-z0-9._~-]*$`)
	npmVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$`)
)

// registryHost is the only traffic the package manager's container may send out.
func registryHost(host string) bool {
	switch host {
	case "registry.npmjs.org", "registry.yarnpkg.com", "repo.yarnpkg.com":
		return true
	}
	return false
}

var lockfiles = []struct{ file, manager string }{
	{"pnpm-lock.yaml", "pnpm"},
	{"yarn.lock", "yarn"},
	{"package-lock.json", "npm"},
}

// packageManager reads which one the app uses from its lockfile; yarn 2+ keeps a .yarnrc.yml.
func packageManager(dir, repoRoot string) (manager, lockfile string) {
	for _, l := range lockfiles {
		if _, err := os.Stat(filepath.Join(dir, l.file)); err != nil {
			continue
		}
		if l.manager == "yarn" {
			for _, d := range []string{dir, repoRoot} {
				if _, err := os.Stat(filepath.Join(d, ".yarnrc.yml")); err == nil {
					return "yarn-berry", l.file
				}
			}
		}
		return l.manager, l.file
	}
	return "", ""
}

var lockfileVersion = regexp.MustCompile(`(?m)^lockfileVersion: '?([0-9]+)`)

// managerTool picks the pnpm or yarn 2+ to install: the app's own packageManager, else the major
// that writes its lockfile's format, so the update never rewrites the whole lockfile.
func managerTool(manager string, manifest, lock []byte) string {
	var m struct {
		PackageManager string `json:"packageManager"`
	}
	_ = json.Unmarshal(manifest, &m)
	pinned, _, _ := strings.Cut(m.PackageManager, "+")
	name, ver, _ := strings.Cut(pinned, "@")
	switch manager {
	case "pnpm":
		if name == "pnpm" && npmVersion.MatchString(ver) {
			return "pnpm@" + ver
		}
		major := "9"
		if v := lockfileVersion.FindSubmatch(lock); v != nil {
			major = map[string]string{"5": "7", "6": "8"}[string(v[1])]
			if major == "" {
				major = "9"
			}
		}
		return "pnpm@" + major
	case "yarn-berry":
		if name == "yarn" && npmVersion.MatchString(ver) {
			return "@yarnpkg/cli-dist@" + ver
		}
		return "@yarnpkg/cli-dist@4"
	}
	return ""
}

// listing finds where package.json lists the package and the range prefix it uses ("^", "~", "").
func listing(manifest []byte, pkg string) (section, prefix string) {
	var m map[string]json.RawMessage
	if json.Unmarshal(manifest, &m) != nil {
		return "", ""
	}
	for _, key := range []string{"dependencies", "devDependencies", "optionalDependencies"} {
		var deps map[string]string
		if json.Unmarshal(m[key], &deps) != nil {
			continue
		}
		if spec, ok := deps[pkg]; ok {
			c := strings.TrimSpace(spec)
			if strings.HasPrefix(c, "^") || strings.HasPrefix(c, "~") {
				return key, c[:1]
			}
			return key, ""
		}
	}
	return "", ""
}

// PreparePackageUpdate clones the commit, moves the dependency in a sealed container, and answers
// the changed package.json and lockfile; nothing is pushed from here.
func (s *Service) PreparePackageUpdate(ctx context.Context, req *pb.PreparePackageUpdateRequest) (*pb.PreparePackageUpdateResponse, error) {
	pkg, version := strings.TrimSpace(req.GetPackage()), strings.TrimSpace(req.GetVersion())
	if !npmName.MatchString(pkg) || !npmVersion.MatchString(version) {
		return &pb.PreparePackageUpdateResponse{Error: "That is not a package name and version."}, nil
	}
	if req.GetGit() == nil {
		return &pb.PreparePackageUpdateResponse{Error: "No repository to start from."}, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	quiet := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	dir, sha, cleanup, err := s.materializeGit(cctx, req.GetGit(), "pkgupdate", quiet)
	if err != nil {
		return &pb.PreparePackageUpdateResponse{Error: "Could not check out the repository: " + err.Error()}, nil
	}
	defer cleanup()
	root := dir
	if sub := strings.Trim(req.GetGit().GetSubdir(), "/"); sub != "" {
		root = strings.TrimSuffix(dir, "/"+sub)
	}

	manager, lockfile := packageManager(dir, root)
	if manager == "" {
		return &pb.PreparePackageUpdateResponse{Error: "The app has no lockfile Deplo can update (package-lock.json, pnpm-lock.yaml or yarn.lock)."}, nil
	}
	before := map[string][]byte{}
	for _, f := range []string{"package.json", lockfile} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			return &pb.PreparePackageUpdateResponse{Error: "Could not read " + f + "."}, nil
		}
		before[f] = b
	}
	spec := pkgupdate.Spec{Manager: manager, Package: pkg, Version: version, Nested: req.GetOverride(),
		Tool: managerTool(manager, before["package.json"], before[lockfile])}
	spec.Section, spec.Prefix = listing(before["package.json"], pkg)

	if detail, err := s.runPackageManager(cctx, dir, spec); err != nil {
		return nil, err
	} else if detail != "" {
		return &pb.PreparePackageUpdateResponse{Manager: manager, Error: "The package manager could not update it: " + detail}, nil
	}

	resp := &pb.PreparePackageUpdateResponse{Manager: manager, BaseCommit: sha}
	rel, _ := filepath.Rel(root, dir)
	for f, old := range before {
		now, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil || bytes.Equal(now, old) {
			continue
		}
		var z bytes.Buffer
		w := gzip.NewWriter(&z)
		_, _ = w.Write(now)
		_ = w.Close()
		resp.Files = append(resp.Files, &pb.ChangedFile{Path: filepath.ToSlash(filepath.Join(rel, f)), Gzip: z.Bytes()})
	}
	if len(resp.Files) == 0 {
		resp.Error = "Nothing changed: the app may already be on that version."
	}
	return resp, nil
}

// runPackageManager runs the steps in node's image with no network but the egress socket, which
// reaches the registries alone. It answers the package manager's own words when a step fails.
func (s *Service) runPackageManager(ctx context.Context, dir string, spec pkgupdate.Spec) (string, error) {
	if err := ensureImage(ctx, pkgUpdateImage); err != nil {
		return "", err
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	sockDir, err := os.MkdirTemp(s.captureDir(), "deplo-pkgupdate-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(sockDir)
	sock := filepath.Join(sockDir, "egress.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		return "", err
	}
	_ = os.Chmod(sock, 0o666)
	ectx, stop := context.WithCancel(ctx)
	defer stop()
	egress := &capture.Egress{Allow: registryHost}
	go egress.Serve(ectx, l)

	// The checkout's owner: with every capability dropped, even root writes only what it owns.
	st, err := os.Stat(dir)
	if err != nil {
		return "", err
	}
	owner := st.Sys().(*syscall.Stat_t)

	in, _ := json.Marshal(spec)
	var stdout bytes.Buffer
	code, err := dockercli.PipeThrough(ctx, 10*time.Minute, bytes.NewReader(in), &stdout, nil,
		"run", "--rm", "-i", "--network", "none", "--user", fmt.Sprintf("%d:%d", owner.Uid, owner.Gid),
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--tmpfs", "/tmp:rw,nosuid,nodev,size=1g",
		"--memory", "2g", "--memory-swap", "2g", "--cpus", "2", "--pids-limit", "1024",
		"--log-driver", "none", "--label", "deplo.helper=pkgupdate",
		"-v", dir+":"+pkgupdate.WorkDir, "-v", exe+":/deplo-agent:ro", "-v", sock+":"+pkgupdate.SocketPath,
		"--entrypoint", "/deplo-agent", pkgUpdateImage, "pkgupdate")
	if err != nil && stdout.Len() == 0 {
		return "", err
	}
	var out pkgupdate.Result
	if json.Unmarshal([]byte(lastLine(stdout.String())), &out) != nil {
		return fmt.Sprintf("the container stopped with code %d", code), nil
	}
	if !out.OK {
		return out.Detail, nil
	}
	return "", nil
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// ensureImage pulls an image the first time it is needed.
func ensureImage(ctx context.Context, ref string) error {
	if res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", ref); err == nil && res.Code == 0 {
		return nil
	}
	res, err := dockercli.Run(ctx, 10*time.Minute, "pull", ref)
	if err == nil && res.Code != 0 {
		err = errors.New(strings.TrimSpace(res.Stderr))
	}
	if err != nil {
		return fmt.Errorf("download %s: %w", ref, err)
	}
	return nil
}
