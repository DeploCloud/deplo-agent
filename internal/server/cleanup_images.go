package server

import (
	"context"
	"errors"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func cleanDanglingImages(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES}

	var estimate int64
	rawBefore := -1
	enumFailure := func() string {
		res, err := dockerQuery(ctx, cleanupQueryTimeout, "image", "ls", "--filter", "dangling=true", "--quiet")
		if err != nil {
			return err.Error()
		}
		if res.Code != 0 {
			return dockerErr("image ls", res)
		}
		before := uniqueLines(res.Stdout)
		rawBefore = len(before)
		images, err := inspectImages(ctx, before)
		if err != nil {
			return err.Error()
		}
		for _, im := range images {
			if !olderThan(im.created, p.cutoff) {
				continue
			}
			estimate += im.size
			addItem(r, im.id)
			r.ItemsRemoved++
		}
		return ""
	}()

	if p.dryRun {
		if enumFailure != "" {
			r.Error = enumFailure
		}
		r.ReclaimedBytes = estimate
		return r
	}
	if enumFailure != "" {
		log.Printf("deplo-agent: dangling-image enumeration failed (%s); pruning anyway", enumFailure)
	}

	args := []string{"image", "prune", "--force"}
	if p.minAgeHours > 0 {
		args = append(args, "--filter", "until="+strconv.Itoa(p.minAgeHours)+"h")
	}
	cctx, cancel := context.WithTimeout(ctx, cleanupImagePruneTimeout)
	defer cancel()
	pres, err := removeObject(cctx, args...)
	if err != nil {
		return failedScope(r, err.Error())
	}
	if pres.Code != 0 {
		return failedScope(r, dockerErr("image prune", pres))
	}
	total, totalKnown := parsePrunedTotal(pres.Stdout)
	if totalKnown && total == 0 {
		r.ReclaimedBytes = 0
		r.ItemsRemoved = 0
		r.Items = nil
		return r
	}
	if totalKnown {
		r.ReclaimedBytes = total
	} else {
		r.ReclaimedBytes = estimate
	}
	if rawBefore >= 0 {
		if res, err := dockerQuery(ctx, cleanupQueryTimeout, "image", "ls", "--filter", "dangling=true", "--quiet"); err == nil && res.Code == 0 {
			removed := rawBefore - len(uniqueLines(res.Stdout))
			if removed < 0 {
				removed = 0
			}
			r.ItemsRemoved = int32(removed)
			if removed == 0 {
				r.Items = nil
			}
		}
	}
	return r
}
func cleanUnusedAppImages(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES}

	var ids strings.Builder
	for _, filter := range []string{"label=deplo.managed=true", "reference=deplo/*"} {
		res, err := dockerQuery(ctx, cleanupQueryTimeout, "image", "ls", "--filter", filter, "--quiet")
		if err != nil {
			r.Error = err.Error()
			return r
		}
		if res.Code != 0 {
			r.Error = dockerErr("image ls", res)
			return r
		}
		ids.WriteString(res.Stdout + "\n")
	}
	images, err := inspectImages(ctx, uniqueLines(ids.String()))
	if err != nil {
		r.Error = err.Error()
		return r
	}

	byGroup := map[string][]imageInfo{}
	for _, im := range images {
		if im.slug == "" {
			continue
		}
		key := im.repo + "\x00" + im.slug + "\x00" + im.service
		byGroup[key] = append(byGroup[key], im)
	}

	var failures scopeFailures
	for _, key := range sortedKeys(byGroup) {
		group := byGroup[key]
		sort.SliceStable(group, func(i, j int) bool {
			ti, oki := parseDockerTime(group[i].created)
			tj, okj := parseDockerTime(group[j].created)
			if oki && okj && !ti.Equal(tj) {
				return ti.After(tj)
			}
			return group[i].id < group[j].id
		})

		keep := p.keepImagesFor(group[0].slug)
		if p.liveSlugs != nil && !p.liveSlugs[group[0].slug] {
			keep = 0
		}

		for rank, im := range group {
			if rank < keep {
				continue
			}
			if idx.images[im.id] {
				continue
			}
			if !olderThan(im.created, p.appImageCutoff) {
				continue
			}

			if !p.dryRun {
				cctx, cancel := context.WithTimeout(ctx, cleanupRemoveTimeout)
				rres, rerr := removeObject(cctx, "rmi", im.id)
				cancel()
				if rerr != nil {
					failures.add(im.id, rerr.Error())
					continue
				}
				if rres.Code != 0 {
					failures.add(im.id, dockerErr("rmi", rres))
					continue
				}
			}
			r.ReclaimedBytes += im.size
			addItem(r, im.id)
			r.ItemsRemoved++
		}
	}

	r.Error = failures.summary()
	return r
}

var buildToolingImages = []string{"moby/buildkit", "ghcr.io/railwayapp/railpack", "docker/dockerfile"}

func isBuildTooling(repo string) bool {
	for _, prefix := range buildToolingImages {
		if strings.HasPrefix(repo, prefix) {
			return true
		}
	}
	return false
}
func cleanUnusedPulledImages(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES}

	res, err := dockerQuery(ctx, cleanupQueryTimeout,
		"image", "ls", "--filter", "dangling=false", "--quiet")
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if res.Code != 0 {
		r.Error = dockerErr("image ls", res)
		return r
	}
	images, err := inspectImages(ctx, uniqueLines(res.Stdout))
	if err != nil {
		r.Error = err.Error()
		return r
	}
	sort.Slice(images, func(i, j int) bool { return images[i].id < images[j].id })

	var failures scopeFailures
	for _, im := range images {
		if idx.images[im.id] || im.managed {
			continue
		}
		if t, ok := parseDockerTime(im.lastTag); !ok || t.IsZero() {
			continue
		}
		if !olderThan(im.lastTag, p.cutoff) || !olderThan(im.lastTag, p.appImageCutoff) {
			continue
		}
		if !im.recoverableFromRegistry(ctx) {
			continue
		}
		if !p.dryRun {
			failed := false
			for _, tag := range im.tags {
				cctx, cancel := context.WithTimeout(ctx, cleanupRemoveTimeout)
				rres, rerr := removeObject(cctx, "rmi", tag)
				cancel()
				if rerr != nil {
					failures.add(tag, rerr.Error())
					failed = true
					break
				}
				if rres.Code != 0 {
					failures.add(tag, dockerErr("rmi", rres))
					failed = true
					break
				}
			}
			if failed {
				continue
			}
		}
		r.ReclaimedBytes += im.size
		addItem(r, im.id)
		r.ItemsRemoved++
	}

	r.Error = failures.summary()
	return r
}

type imageInfo struct {
	id      string
	slug    string
	service string
	repo    string
	created string
	size    int64
	managed bool
	lastTag string
	tags    []string
	digests []string
}

func (im imageInfo) recoverableFromRegistry(ctx context.Context) bool {
	if im.managed || len(im.tags) == 0 {
		return false
	}
	repositories := make(map[string]string)
	refs := make(map[string]bool)
	for _, digest := range im.digests {
		repo, hash, ok := strings.Cut(digest, "@sha256:")
		if ok && len(hash) == 64 {
			repositories[repo] = digest
		}
	}
	for _, tag := range im.tags {
		repo := repoOf(tag)
		if repositories[repo] == "" || isBuildTooling(repo) {
			return false
		}
		refs[tag] = true
		refs[repositories[repo]] = true
	}
	// Docker's containerd store also assigns RepoDigests to local-only builds.
	for ref := range refs {
		res, err := dockerQuery(ctx, cleanupQueryTimeout, "manifest", "inspect", ref)
		if err != nil || res.Code != 0 {
			return false
		}
	}
	return true
}

func inspectImages(ctx context.Context, ids []string) ([]imageInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := append([]string{"image", "inspect", "--format",
		`{{.Id}}|{{with (index .Config "Labels")}}{{index . "deplo.slug"}}|{{index . "deplo.service"}}{{else}}|{{end}}|{{.Created}}|{{.Size}}|` +
			`{{if .RepoTags}}{{index .RepoTags 0}}{{else if .RepoDigests}}{{index .RepoDigests 0}}{{end}}|` +
			`{{with (index .Config "Labels")}}{{index . "deplo.managed"}}{{end}}|` +
			`{{with (index . "Metadata")}}{{with (index . "LastTagTime")}}{{json .}}{{end}}{{end}}|{{join .RepoTags ","}}|{{join .RepoDigests ","}}`}, ids...)
	res, err := dockerQuery(ctx, cleanupQueryTimeout, args...)
	if err != nil {
		return nil, err
	}

	label := func(s string) string {
		s = strings.TrimSpace(s)
		if s == "<no value>" {
			return ""
		}
		return s
	}
	var out []imageInfo
	for _, line := range splitLines(res.Stdout) {
		parts := strings.Split(line, "|")
		if len(parts) < 5 {
			continue
		}
		size, err := strconv.ParseInt(strings.TrimSpace(parts[4]), 10, 64)
		if err != nil {
			continue
		}
		repo := ""
		if len(parts) > 5 {
			repo = repoOf(strings.TrimSpace(parts[5]))
		}
		im := imageInfo{
			id:      strings.TrimSpace(parts[0]),
			slug:    label(parts[1]),
			service: label(parts[2]),
			repo:    repo,
			created: strings.TrimSpace(parts[3]),
			size:    size,
		}
		if len(parts) > 6 {
			im.managed = label(parts[6]) == "true"
		}
		if len(parts) > 7 {
			im.lastTag = strings.Trim(strings.TrimSpace(parts[7]), `"`)
		}
		if len(parts) > 8 {
			for _, t := range strings.Split(parts[8], ",") {
				if t = strings.TrimSpace(t); t != "" {
					im.tags = append(im.tags, t)
				}
			}
		}
		if len(parts) > 9 {
			im.digests = strings.FieldsFunc(parts[9], func(r rune) bool { return r == ',' })
		}
		if !im.managed {
			im.adoptBuiltName()
		}
		out = append(out, im)
	}
	if res.Code != 0 && len(out) == 0 {
		return nil, errors.New(dockerErr("image inspect", res))
	}
	return out, nil
}

// builtImageName is the name every Deplo build tags its image with (deployImageRef): deplo/<slug>:dpl_<id>.
var builtImageName = regexp.MustCompile(`^(?:docker\.io/)?deplo/([a-z0-9][a-z0-9_.-]*):dpl_[0-9a-f]+$`)

// adoptBuiltName marks an image Deplo built without labels as the app's own: a Railpack build has none,
// since its frontend drops --label.
func (im *imageInfo) adoptBuiltName() {
	for _, tag := range im.tags {
		if m := builtImageName.FindStringSubmatch(tag); m != nil && (im.slug == "" || im.slug == m[1]) {
			im.managed, im.slug = true, m[1]
			return
		}
	}
}

func repoOf(ref string) string {
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		ref = ref[:i]
	}
	return strings.TrimSpace(ref)
}
func sortedKeys(m map[string][]imageInfo) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
