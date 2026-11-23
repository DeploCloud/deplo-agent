package server

import (
	"strings"
	"testing"
)

func TestImageVolumes_aBuiltImagesVolumesAreItsAbsolutePaths(t *testing.T) {
	cases := map[string]string{
		`{"/var/lib/postgresql/data":{},"/data":{}}` + "\n": "/data,/var/lib/postgresql/data",
		"null\n":          "",
		`{"relative":{}}`: "",
		"not json at all": "",
	}
	for raw, want := range cases {
		if got := strings.Join(imageVolumePaths(raw), ","); got != want {
			t.Errorf("%q: got %q, want %q", raw, got, want)
		}
	}
}
