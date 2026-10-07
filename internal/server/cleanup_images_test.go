package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"strings"
	"testing"
	"time"
)

// The dangling-images scope prunes untagged layers.
func TestDockerCleanup_danglingImagesArgv_neverAll(t *testing.T) {
	for _, tc := range []struct {
		name        string
		minAgeHours int32
		want        string
	}{
		{"with an age filter", 168, "image prune --force --filter until=168h"},
		{"without one", 0, "image prune --force"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFixture(t)
			h.install(t)
			if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
				Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES},
				MinAgeHours: tc.minAgeHours,
			}); err != nil {
				t.Fatalf("DockerCleanup: %v", err)
			}
			got := h.argv()
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("argv = %q, want [%q]", got, tc.want)
			}
			for _, a := range got {
				if strings.Contains(a, " -a") || strings.Contains(a, "--all") {
					t.Fatalf("image prune must never be -a/--all, got %q", a)
				}
			}
		})
	}
}

// The unused-app-images allow-list: delete BY ID, one rmi each, keeping the newest keep_images_per_app of the slug and anything a container (running or exited) still references.
func TestDockerCleanup_unusedAppImages_allowList(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
		MinAgeHours:      168,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	want := []string{"rmi sha256:ccc", "rmi sha256:eee"}
	got := h.argv()
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
	for _, a := range got {
		if strings.Contains(a, "sha256:aaa") {
			t.Fatal("removed the image a running container is using")
		}
		if strings.Contains(a, "-f") || strings.Contains(a, "--force") {
			t.Fatalf("rmi must not force: %q", a)
		}
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES)
	if r.GetItemsRemoved() != 2 {
		t.Errorf("items_removed = %d, want 2", r.GetItemsRemoved())
	}
	if r.GetReclaimedBytes() != 900000000+800000000 {
		t.Errorf("reclaimed_bytes = %d, want the two removed images' sizes", r.GetReclaimedBytes())
	}
}

// keep_images_per_app ranks within the slug's WHOLE image set, in-use images included, so keeping 2 keeps the running one plus the next newest.
func TestDockerCleanup_unusedAppImages_keepsN(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 2,
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 1 || got[0] != "rmi sha256:eee" {
		t.Fatalf("argv = %q, want [rmi sha256:eee]", got)
	}
}

// An image with no deplo.slug cannot be reasoned about (which app is it? which of its generations is current?), so the allow-list leaves it alone entirely.
func TestDockerCleanup_unusedAppImages_skipsUnslugged(t *testing.T) {
	h := newFixture(t)
	h.managedImages = []string{"ddd1111"}
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("argv = %q, want none - an image with no deplo.slug is not a candidate", got)
	}
}

// An image's deplo labels are not proof of whose image it is: any tenant can pull one carrying `deplo.slug=<someone else>` and a Created of their choosing.
func TestDockerCleanup_unusedAppImages_forgedSlugCannotEvict(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	gen := func(hoursAgo int) string {
		return now.Add(-time.Duration(hoursAgo) * time.Hour).Format(time.RFC3339Nano)
	}
	h.managedImages = []string{"real1", "real2", "fake1"}
	h.imageRows = map[string]string{
		"real1": "sha256:real1|shop||" + gen(2) + "|100000000|deplo/shop:dpl_two",
		"real2": "sha256:real2|shop||" + gen(4) + "|100000000|deplo/shop:dpl_one",
		"fake1": "sha256:fake1|shop||" + gen(1) + "|100000000|evil/foo:latest",
	}
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 2,
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("argv = %q, want none - a foreign image evicted the app's own generation", got)
	}
}
func TestRepoOf(t *testing.T) {
	for ref, want := range map[string]string{
		"deplo/shop:dpl_abc123":            "deplo/shop",
		"deplo-shop-web:latest":            "deplo-shop-web",
		"registry.acme.com:5000/team/x:v2": "registry.acme.com:5000/team/x",
		"evil/foo@sha256:abc":              "evil/foo",
		"":                                 "",
	} {
		if got := repoOf(ref); got != want {
			t.Errorf("repoOf(%q) = %q, want %q", ref, got, want)
		}
	}
}

// THE REGRESSION that saturated real hosts: an app redeployed many times a day piles up superseded-but-tagged images, all younger than min_age_hours, and the old age gate meant none was EVER a candidate, so every sweep "succeeded" with 0 bytes while the disk filled.
func TestDockerCleanup_unusedAppImages_minAgeDoesNotShield(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	h.imageRows["aaa1111"] = "sha256:aaa|web|<no value>|" + now.Format(time.RFC3339Nano) + "|1000000000"
	h.imageRows["eee1111"] = "sha256:eee|web|<no value>|" + now.Add(-30*time.Minute).Format(time.RFC3339Nano) + "|800000000"
	h.imageRows["ccc1111"] = "sha256:ccc|web|<no value>|" + now.Add(-5*time.Hour).Format(time.RFC3339Nano) + "|900000000"
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
		MinAgeHours:      168,
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 1 || got[0] != "rmi sha256:ccc" {
		t.Fatalf("argv = %q, want [rmi sha256:ccc] - min_age shielded a superseded image (or the grace didn't)", got)
	}
}

// Compose stacks build one image per service under the SAME deplo.slug; the deplo.service image label splits them so "keep the newest N" holds per service.
func TestDockerCleanup_unusedAppImages_composeServicesRankApart(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	newer := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	older := now.Add(-8 * time.Hour).Format(time.RFC3339Nano)
	h.managedImages = []string{"web1111", "web2222", "api1111", "api2222"}
	h.imageRows = map[string]string{
		"web1111": "sha256:web1|shop|web|" + newer + "|100000000",
		"web2222": "sha256:web2|shop|web|" + older + "|100000000",
		"api1111": "sha256:api1|shop|api|" + newer + "|100000000",
		"api2222": "sha256:api2|shop|api|" + older + "|100000000",
	}
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	want := []string{"rmi sha256:web2", "rmi sha256:api2"}
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q - services of one slug must rank separately", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
}

// keep_per_slug is what carries an app's ROLLBACK DEPTH: two apps on one host keep different numbers of generations, and an app the map does not name falls back to the host-wide scalar.
func TestDockerCleanup_unusedAppImages_keepPerSlug(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	gen := func(hoursAgo int) string {
		return now.Add(-time.Duration(hoursAgo) * time.Hour).Format(time.RFC3339Nano)
	}
	h.managedImages = []string{"deep1", "deep2", "deep3", "deep4", "flat1", "flat2"}
	h.imageRows = map[string]string{
		"deep1": "sha256:deep1|deep|<no value>|" + gen(2) + "|100000000",
		"deep2": "sha256:deep2|deep|<no value>|" + gen(4) + "|100000000",
		"deep3": "sha256:deep3|deep|<no value>|" + gen(6) + "|100000000",
		"deep4": "sha256:deep4|deep|<no value>|" + gen(8) + "|100000000",
		"flat1": "sha256:flat1|flat|<no value>|" + gen(2) + "|100000000",
		"flat2": "sha256:flat2|flat|<no value>|" + gen(4) + "|100000000",
	}
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
		KeepPerSlug:      map[string]int32{"deep": 3},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	want := []string{"rmi sha256:deep4", "rmi sha256:flat2"}
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q - per-slug keep did not override, or leaked to the other app", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
}

// A zero (or negative) per-slug value must floor at 1 exactly like the scalar does: an app that keeps no images at all is an app that cannot be started again without a rebuild, which is the one outcome this scope refuses to produce.
func TestDockerCleanup_unusedAppImages_keepPerSlugFloorsAtOne(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 2,
		KeepPerSlug:      map[string]int32{"web": 0},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	want := []string{"rmi sha256:ccc", "rmi sha256:eee"}
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
}

// items_removed for dangling images is a post-prune OBSERVATION, not our pre-flight guess: an image whose timestamp we cannot parse is never a CANDIDATE of ours (olderThan refuses it), but once docker's own filter removes it the diff counts it anyway.
func TestDockerCleanup_danglingCount_isPostPruneDiff(t *testing.T) {
	h := newFixture(t)
	h.danglingImages = []string{"ddd1111", "xxx2222"}
	h.imageRows["xxx2222"] = "sha256:xxx|<no value>|<no value>|not-a-timestamp|700000000"
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES},
		MinAgeHours: 168,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES)
	if r.GetItemsRemoved() != 2 {
		t.Errorf("items_removed = %d, want 2 (the post-prune diff, not the 1-candidate guess)",
			r.GetItemsRemoved())
	}
	if r.GetReclaimedBytes() != 1500000000 {
		t.Errorf("reclaimed_bytes = %d, want docker's own total", r.GetReclaimedBytes())
	}
}

// A compose stack builds one image per SERVICE under one deplo.slug, and the map is keyed by slug alone - so an app's number has to apply to each of its services independently, exactly as the scalar always did.
func TestDockerCleanup_unusedAppImages_keepPerSlugAppliesPerService(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	gen := func(hoursAgo int) string {
		return now.Add(-time.Duration(hoursAgo) * time.Hour).Format(time.RFC3339Nano)
	}
	h.managedImages = []string{"w1", "w2", "w3", "a1", "a2", "a3"}
	h.imageRows = map[string]string{
		"w1": "sha256:w1|shop|web|" + gen(2) + "|100000000",
		"w2": "sha256:w2|shop|web|" + gen(4) + "|100000000",
		"w3": "sha256:w3|shop|web|" + gen(6) + "|100000000",
		"a1": "sha256:a1|shop|api|" + gen(2) + "|100000000",
		"a2": "sha256:a2|shop|api|" + gen(4) + "|100000000",
		"a3": "sha256:a3|shop|api|" + gen(6) + "|100000000",
	}
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
		KeepPerSlug:      map[string]int32{"shop": 2},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	want := []string{"rmi sha256:w3", "rmi sha256:a3"}
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q - the per-slug keep did not apply per service", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
}

// An app the control plane no longer knows keeps NOTHING: before, its newest image was pinned forever by keep-N.
func TestDockerCleanup_unusedAppImages_deadSlugKeepsNothing(t *testing.T) {
	for _, live := range [][]string{nil, {"other"}, {"web"}} {
		h := newFixture(t)
		h.install(t)
		resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
			Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
			KeepImagesPerApp: 2,
			LiveSlugs:        live,
		})
		if err != nil {
			t.Fatal(err)
		}
		r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES)
		removed := h.argv()
		dead := len(live) > 0 && live[0] != "web"
		wantRemoved := 1
		if dead {
			wantRemoved = 2
		}
		if int(r.GetItemsRemoved()) != wantRemoved || len(removed) != wantRemoved {
			t.Fatalf("live=%v: removed %d (%v), want %d", live, r.GetItemsRemoved(), removed, wantRemoved)
		}
		for _, a := range removed {
			if strings.Contains(a, "sha256:aaa") {
				t.Fatalf("live=%v: the image a container runs was removed: %v", live, removed)
			}
		}
	}
}

// Pulled images: unmanaged, unreferenced, old on THIS host, not build tooling - and removed tag by tag.
func TestDockerCleanup_unusedPulledImages_allowList(t *testing.T) {
	h := newFixture(t)
	old := time.Now().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	fresh := time.Now().Format(time.RFC3339Nano)
	row := func(id, managed, lastTag, tags string) string {
		return "sha256:" + id + "|<no value>|<no value>|" + old + "|300000000|" +
			strings.Split(tags, ",")[0] + "|" + managed + "|\"" + lastTag + "\"|" + tags
	}
	h.taggedImages = []string{"p1", "p2", "p3", "p4", "p5", "p6"}
	h.imageRows["p1"] = row("p1", "", old, "kanboard/kanboard:latest")
	h.imageRows["p2"] = row("p2", "", old, "louislam/uptime-kuma:2,louislam/uptime-kuma:2.1.0")
	h.imageRows["p3"] = row("p3", "true", old, "deplo/web:dpl_1")
	h.imageRows["p4"] = row("p4", "", fresh, "nginx:alpine")
	h.imageRows["p5"] = row("p5", "", old, "heroku/builder:24")
	h.imageRows["p6"] = row("aaa", "", old, "postgres:16-alpine")
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES)
	want := []string{
		"rmi kanboard/kanboard:latest",
		"rmi louislam/uptime-kuma:2",
		"rmi louislam/uptime-kuma:2.1.0",
	}
	if got := h.argv(); strings.Join(got, ";") != strings.Join(want, ";") {
		t.Fatalf("removals:\n got %v\nwant %v", got, want)
	}
	if r.GetItemsRemoved() != 2 || r.GetReclaimedBytes() != 600000000 {
		t.Fatalf("result: %+v", r)
	}
}

// An unparseable LastTagTime fails closed, and a policy with no age filter still keeps the deploy grace.
func TestDockerCleanup_unusedPulledImages_unknownAgeNeverQualifies(t *testing.T) {
	h := newFixture(t)
	old := time.Now().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	h.taggedImages = []string{"p1", "p2"}
	h.imageRows["p1"] = "sha256:p1|<no value>|<no value>|" + old + "|1|x:1||\"0001-01-01T00:00:00Z\"|x:1"
	h.imageRows["p2"] = "sha256:p2|<no value>|<no value>|" + old + "|1|y:1||\"" +
		time.Now().Add(-30*time.Minute).Format(time.RFC3339Nano) + "\"|y:1"
	h.install(t)
	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES)
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("removals: %v", got)
	}
	if r.GetItemsRemoved() != 0 {
		t.Fatalf("result: %+v", r)
	}
}

// The inspect template must reach labels with `index`: an image whose Config has no Labels key made a dotted `.Config.Labels` fail the whole batch on Docker 29.
func TestInspectImages_labelsThroughWith(t *testing.T) {
	var got []string
	orig := dockerQuery
	dockerQuery = func(_ context.Context, _ time.Duration, args ...string) (dockercli.Result, error) {
		got = args
		return okResult(""), nil
	}
	t.Cleanup(func() { dockerQuery = orig })
	if _, err := inspectImages(context.Background(), []string{"x"}); err != nil {
		t.Fatal(err)
	}
	format := got[3]
	if strings.Contains(format, ".Config.Labels") || !strings.Contains(format, `(index .Config "Labels")`) {
		t.Fatalf("labels must be reached with index, never dotted, got %q", format)
	}
}
