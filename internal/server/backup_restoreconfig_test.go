package server

import "testing"

// The archive's compose names the network of the day the backup was taken.
func TestRetargetStackNetwork(t *testing.T) {
	stale := "services:\n  web:\n    image: nginx\n    networks:\n      - deplo\n" +
		"networks:\n  deplo:\n    name: deplo-env-environ_gone\n    external: true\n"
	got := retargetStackNetwork(stale, "deplo-env-environ_now")
	if !contains(got, "name: deplo-env-environ_now") || contains(got, "environ_gone") {
		t.Fatalf("network not retargeted:\n%s", got)
	}
	if same := retargetStackNetwork(stale, "deplo-env-environ_gone"); same != stale {
		t.Errorf("an up-to-date file must be untouched:\n%s", same)
	}
	vols := "volumes:\n  data:\n    name: deplo-shop-data\n"
	if got := retargetStackNetwork(vols, "deplo-env-x"); got != vols {
		t.Errorf("a volume name must not be touched:\n%s", got)
	}
	ext := "networks:\n  other:\n    name: someones-net\n    external: true\n"
	if got := retargetStackNetwork(ext, "deplo-env-x"); got != ext {
		t.Errorf("a foreign network must not be touched:\n%s", got)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
