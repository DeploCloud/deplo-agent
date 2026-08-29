package server

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// writeProcNetNs fakes /proc/<pid>/ns/net. The kernel makes it a symlink whose
// INODE is the namespace identity, so sharing a namespace is a hard link here.
func writeProcNetNs(t *testing.T, procRoot string, pid int, shareWith string) string {
	t.Helper()
	dir := filepath.Join(procRoot, strconv.Itoa(pid), "ns")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "net")
	if shareWith != "" {
		if err := os.Link(shareWith, path); err != nil {
			t.Fatalf("link %s -> %s: %v", path, shareWith, err)
		}
		return path
	}
	if err := os.WriteFile(path, []byte("net"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func runningEntry(id, name, project string, pid int, cg string) rosterEntry {
	return rosterEntry{ID: id, Name: name, ProjectID: project, State: "running", PID: pid, CgroupPath: cg}
}

// A compose sidecar on `network_mode: service:x` reads the SAME counters as the
// container it joins. Both must report the same namespace id so the control plane
// can sum that traffic once instead of doubling the stack's network.
func TestCgroupSampler_SidecarsShareANamespaceId(t *testing.T) {
	tmp := t.TempDir()
	proc := filepath.Join(tmp, "proc")
	cgA := filepath.Join(tmp, "cg", "a")
	cgB := filepath.Join(tmp, "cg", "b")
	writeCgroupFiles(t, cgA, fullCgroup())
	writeCgroupFiles(t, cgB, fullCgroup())
	// One namespace, two containers in it, and the same counters visible in both.
	ns := writeProcNetNs(t, proc, 100, "")
	writeProcNetNs(t, proc, 101, ns)
	writeProcNetDev(t, proc, 100, goldenNetDev)
	writeProcNetDev(t, proc, 101, goldenNetDev)

	c := newTestSampler(t, proc)
	out := c.Sample([]rosterEntry{
		runningEntry("a", "app", "prj_1", 100, cgA),
		runningEntry("b", "vpn-sidecar", "prj_1", 101, cgB),
	}, time.Unix(1000, 0))

	if len(out) != 2 {
		t.Fatalf("got %d stats, want 2", len(out))
	}
	if out[0].NetNsId == 0 || out[1].NetNsId == 0 {
		t.Fatalf("namespace id not reported: %d / %d", out[0].NetNsId, out[1].NetNsId)
	}
	if out[0].NetNsId != out[1].NetNsId {
		t.Errorf("sidecar namespace %d != app namespace %d - the aggregate would double the stack's traffic",
			out[1].NetNsId, out[0].NetNsId)
	}
	if out[0].NetRx != out[1].NetRx {
		t.Errorf("same namespace read two different counters: %d vs %d", out[0].NetRx, out[1].NetRx)
	}
	if out[0].NetNsHost || out[1].NetNsHost {
		t.Error("neither container is on the host namespace")
	}
}

// `network_mode: host` puts the container in the agent's own namespace, where
// /proc/<pid>/net/dev is the WHOLE MACHINE's counters. An idle container reported
// 51 GB of traffic that way, so the agent must report nothing and say why.
func TestCgroupSampler_HostNetworkingReportsNoTrafficOfItsOwn(t *testing.T) {
	tmp := t.TempDir()
	proc := filepath.Join(tmp, "proc")
	cg := filepath.Join(tmp, "cg", "host-net")
	writeCgroupFiles(t, cg, fullCgroup())
	// The agent's namespace IS the host's; the container joins it.
	hostNs := writeProcNetNs(t, proc, os.Getpid(), "")
	writeProcNetNs(t, proc, 200, hostNs)
	writeProcNetDev(t, proc, 200, goldenNetDev)

	c := newTestSampler(t, proc)
	out := c.Sample([]rosterEntry{runningEntry("h", "vpn", "prj_2", 200, cg)}, time.Unix(1000, 0))

	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	st := out[0]
	if !st.NetNsHost {
		t.Error("NetNsHost = false, want true - the container shares the agent's namespace")
	}
	if st.NetRx != 0 || st.NetTx != 0 {
		t.Errorf("net = %d/%d, want 0/0 - those bytes belong to the host chart", st.NetRx, st.NetTx)
	}
	// Everything else still measures: only the network is somebody else's.
	if st.MemUsed == 0 || st.Pids == 0 {
		t.Errorf("memory/pids were dropped too: mem=%d pids=%d", st.MemUsed, st.Pids)
	}
}
