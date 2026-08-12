package server

import "testing"

// Every ref the image relay handles is the control plane's own
// `deplo/<deploy key>:<deployment id[:12]>`, and a deploy key is a slug or a
// preview's `<slug>__pr-<n>` - so the underscore and the double underscore have
// to pass, or previews could never build on a build server.
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

// The ref reaches `docker save <ref>` / `docker image inspect <ref>` as an argv
// element. There is no shell, so the danger is not injection but a ref that reads
// as a FLAG, or one carrying path traversal on its way into a name. Both are
// rejected here rather than relied on being impossible upstream.
func TestValidateImageRefRefusesFlagsAndTraversal(t *testing.T) {
	for _, ref := range []string{
		"",
		"-f",
		"--force",
		"deplo/x",              // no tag: `docker save deplo/x` would take every tag
		":latest",              // no name
		"deplo/../etc:tag",     // traversal in the name
		"deplo/x:..",           // traversal in the tag
		"deplo/x:tag extra",    // a second argv token smuggled in
		"deplo/x:tag\nrm",      // newline
		"deplo/x:tag;rm -rf /", // shell metacharacters, harmless here but not a ref
		// Outside our namespace. This RPC DELETES images and streams them out, so
		// the prefix is the blast radius: without it a malformed request could
		// `docker rmi` a base image out from under every app on the host, or
		// export an image that was never ours to send.
		"node:20",
		"ubuntu:latest",
		"registry.example.com/private/app:v1",
		"deplox/app:tag", // near miss on the namespace
		"xdeplo/app:tag", // ditto
		"deplo:tag",      // the namespace is not itself an image
	} {
		if err := validateImageRef(ref); err == nil {
			t.Fatalf("validateImageRef(%q) = nil; want an error", ref)
		}
	}
}

// The security decision of ImportImage: `docker load` restores whatever RepoTags
// the ARCHIVE declares, not the tag the caller announced. Confirming the declared
// tag exists afterwards proves nothing about what else arrived, so the tag list is
// diffed either side of the load.
func TestUnexpectedTagsCatchesWhatTheArchiveSmuggled(t *testing.T) {
	set := func(tags ...string) map[string]bool {
		m := make(map[string]bool)
		for _, t := range tags {
			m[t] = true
		}
		return m
	}

	// The ordinary delivery: one new tag, the one that was asked for.
	got := unexpectedTags(
		set("deplo/other:aaa", "traefik:v3"),
		set("deplo/other:aaa", "traefik:v3", "deplo/app:bbb"),
		"deplo/app:bbb",
	)
	if len(got) != 0 {
		t.Fatalf("clean import flagged %v; want nothing", got)
	}

	// A compromised builder shipping a second image alongside the real one. The
	// overwrite of a NEIGHBOUR's app is caught - that is the one its next rollback
	// would run.
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

	// A tag that ALREADY existed is not the archive's doing. `docker load` replacing
	// an image in place keeps the same name, and flagging that would fail every
	// redeploy of a neighbouring app that happens to share this host.
	got = unexpectedTags(
		set("deplo/other:aaa"),
		set("deplo/other:aaa", "deplo/app:bbb"),
		"deplo/app:bbb",
	)
	if len(got) != 0 {
		t.Fatalf("pre-existing tag flagged %v; want nothing", got)
	}

	// The declared tag is never unexpected, whether or not it existed before.
	got = unexpectedTags(set(), set("deplo/app:bbb"), "deplo/app:bbb")
	if len(got) != 0 {
		t.Fatalf("the declared tag was flagged: %v", got)
	}
}

// Outside `deplo/` the agent stays out of it, and that is a deliberate trade, not
// an oversight: `docker image ls` sees the whole host, so a `docker-image` source
// pulling a base image on this box WHILE an import runs would otherwise be read as
// smuggled and deleted - breaking a deploy that did nothing wrong. Base images are
// re-pulled by any build that needs them; a neighbour's `deplo/` image is not.
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
		// Everything a concurrent pull on this host could plausibly add.
		set("deplo/other:aaa", "deplo/app:bbb", "nginx:latest", "python:3.12",
			"ghcr.io/acme/api:v2", "registry.example.com/x/y:1"),
		"deplo/app:bbb",
	)
	if len(got) != 0 {
		t.Fatalf("unexpectedTags touched foreign namespaces: %v", got)
	}
}
