package server

import "testing"

func TestRelativeDirKeepsADotDir(t *testing.T) {
	for in, want := range map[string]string{
		"":        "",
		"dist":    "dist",
		"./dist":  "dist",
		"/dist":   "dist",
		"  _site": "_site",
		".next":   ".next", // a dot-dir, not a leading "./"
		"./.next": ".next",
	} {
		if got := relativeDir(in); got != want {
			t.Errorf("relativeDir(%q) = %q, want %q", in, got, want)
		}
	}
}
