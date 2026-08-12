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
	} {
		if err := validateImageRef(ref); err == nil {
			t.Fatalf("validateImageRef(%q) = nil; want an error", ref)
		}
	}
}
