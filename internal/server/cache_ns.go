package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

// cacheNamespace is the BUILDKIT_CACHE_MOUNT_NS every build of an app gets: a
// BuildKit cache mount is host-wide and keyed by its id alone, so without a
// namespace one tenant's `RUN --mount=type=cache,id=<other slug>-…` reads and
// poisons another's. The salt is this host's own, so the namespace cannot be
// guessed from the (public) slug.
func (s *Service) cacheNamespace(slug string) string {
	s.cacheSaltOnce.Do(func() { s.cacheSalt = loadOrMintSalt(filepath.Join(s.agentDir, "build-cache.salt")) })
	mac := hmac.New(sha256.New, s.cacheSalt)
	mac.Write([]byte(slug))
	return hex.EncodeToString(mac.Sum(nil))[:24]
}

// loadOrMintSalt reads the salt file, minting it (0600) on first use. With no
// agent dir (tests) the salt lives for the process only.
func loadOrMintSalt(path string) []byte {
	if filepath.Dir(path) != "." {
		if b, err := os.ReadFile(path); err == nil && len(b) >= 32 {
			return b
		}
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("build cache salt: " + err.Error())
	}
	if filepath.Dir(path) != "." {
		_ = os.WriteFile(path, b, 0o600)
	}
	return b
}
