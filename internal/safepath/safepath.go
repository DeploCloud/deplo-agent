package safepath

import (
	"os"
	"path/filepath"
	"strings"
)

// Inside canonicalises `candidate` (a path formed by joining an off-the-wire, user-controlled relative segment onto a trusted `base`) and returns it only if it is `base` itself or a real descendant.
func Inside(base, candidate string) (string, error) {
	realBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		realBase = filepath.Clean(base)
	}
	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return realBase, nil
	}
	if realCandidate == realBase || strings.HasPrefix(realCandidate, realBase+string(os.PathSeparator)) {
		return realCandidate, nil
	}
	return realBase, nil
}

// Join cleans a user-supplied relative path and joins it under base WITHOUT touching the filesystem, rejecting absolute paths and any ".." segment.
func Join(base, rel string) (string, bool) {
	rel = strings.ReplaceAll(rel, "\\", "/")
	rel = strings.TrimPrefix(rel, "./")
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" || rel == "." {
		return base, true
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == ".." {
			return base, false
		}
	}
	return filepath.Join(base, rel), true
}
