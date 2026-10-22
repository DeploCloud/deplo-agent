package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
)

func (s *Service) cacheNamespace(slug string) string {
	mac := hmac.New(sha256.New, s.buildCacheSalt())
	mac.Write([]byte(slug))
	return hex.EncodeToString(mac.Sum(nil))[:24]
}

// secretsHash changes whenever a build secret's value does; keyed so the logged argv leaks nothing.
func (s *Service) secretsHash(env []string) string {
	mac := hmac.New(sha256.New, s.buildCacheSalt())
	for _, kv := range slices.Sorted(slices.Values(env)) {
		mac.Write([]byte(kv))
		mac.Write([]byte{0})
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Service) buildCacheSalt() []byte {
	s.cacheSaltOnce.Do(func() { s.cacheSalt = loadOrMintSalt(filepath.Join(s.agentDir, "build-cache.salt")) })
	return s.cacheSalt
}

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
