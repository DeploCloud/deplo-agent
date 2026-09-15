package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

func (s *Service) cacheNamespace(slug string) string {
	s.cacheSaltOnce.Do(func() { s.cacheSalt = loadOrMintSalt(filepath.Join(s.agentDir, "build-cache.salt")) })
	mac := hmac.New(sha256.New, s.cacheSalt)
	mac.Write([]byte(slug))
	return hex.EncodeToString(mac.Sum(nil))[:24]
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
