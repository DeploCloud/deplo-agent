package server

import "testing"

// Every ref the image relay handles is the control plane's own `deplo/<deploy key>:<deployment id[:12]>`, and a deploy key is a slug or a preview's `<slug>__pr-<n>` - so the underscore and the double underscore have to pass, or previews could never build on a build server.
func TestValidateImageRefAcceptsWhatTheControlPlaneMints(t *testing.T) {
	for _, ref := range []string{
		"deplo/hub:6f2c91ab4d3e",
		"deplo/my-app__pr-3:0a1b2c3d4e5f",
		"deplo/a.b_c-d:v1.2.3",
	} {
		if err := validateImageRef(ref); err != nil {
			t.Fatalf("validateImageRef(%q) = %v; want nil", ref, err)
		}
	}
}

// The ref reaches `docker save <ref>` / `docker image inspect <ref>` as an argv element.
func TestValidateImageRefRefusesFlagsAndTraversal(t *testing.T) {
	for _, ref := range []string{
		"",
		"-f",
		"--force",
		"deplo/x",
		":latest",
		"deplo/../etc:tag",
		"deplo/x:..",
		"deplo/x:tag extra",
		"deplo/x:tag\nrm",
		"deplo/x:tag;rm -rf /",
		"node:20",
		"ubuntu:latest",
		"registry.example.com/private/app:v1",
		"deplox/app:tag",
		"xdeplo/app:tag",
		"deplo:tag",
	} {
		if err := validateImageRef(ref); err == nil {
			t.Fatalf("validateImageRef(%q) = nil; want an error", ref)
		}
	}
}

// The security decision of ImportImage: `docker load` restores whatever RepoTags the ARCHIVE declares, not the tag the caller announced.
func TestUnexpectedTagsCatchesWhatTheArchiveSmuggled(t *testing.T) {
	set := func(tags ...string) map[string]bool {
		m := make(map[string]bool)
		for _, t := range tags {
			m[t] = true
		}
		return m
	}

	got := unexpectedTags(
		set("deplo/other:aaa", "traefik:v3"),
		set("deplo/other:aaa", "traefik:v3", "deplo/app:bbb"),
		"deplo/app:bbb",
	)
	if len(got) != 0 {
		t.Fatalf("clean import flagged %v; want nothing", got)
	}

	got = unexpectedTags(
		set("deplo/other:aaa", "node:20"),
		set("deplo/other:aaa", "node:20", "deplo/app:bbb", "deplo/other:evil", "deplo/third:evil"),
		"deplo/app:bbb",
	)
	want := []string{"deplo/other:evil", "deplo/third:evil"}
	if len(got) != len(want) {
		t.Fatalf("unexpectedTags = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpectedTags = %v; want %v (sorted, so the message is stable)", got, want)
		}
	}

	got = unexpectedTags(
		set("deplo/other:aaa"),
		set("deplo/other:aaa", "deplo/app:bbb"),
		"deplo/app:bbb",
	)
	if len(got) != 0 {
		t.Fatalf("pre-existing tag flagged %v; want nothing", got)
	}

	got = unexpectedTags(set(), set("deplo/app:bbb"), "deplo/app:bbb")
	if len(got) != 0 {
		t.Fatalf("the declared tag was flagged: %v", got)
	}
}

// Outside `deplo/` the agent stays out of it, and that is a deliberate trade, not an oversight: `docker image ls` sees the whole host, so a `docker-image` source pulling a base image on this box WHILE an import runs would otherwise be read as smuggled and deleted - breaking a deploy that did nothing wrong.
func TestUnexpectedTagsLeavesForeignNamespacesAlone(t *testing.T) {
	set := func(tags ...string) map[string]bool {
		m := make(map[string]bool)
		for _, t := range tags {
			m[t] = true
		}
		return m
	}
	got := unexpectedTags(
		set("deplo/other:aaa"),
		set("deplo/other:aaa", "deplo/app:bbb", "nginx:latest", "python:3.12",
			"ghcr.io/acme/api:v2", "registry.example.com/x/y:1"),
		"deplo/app:bbb",
	)
	if len(got) != 0 {
		t.Fatalf("unexpectedTags touched foreign namespaces: %v", got)
	}
}
