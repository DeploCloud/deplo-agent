package server

import "testing"

// parseStatsLine is the pure heart of ContainerStats — it turns one
// `docker stats --format {{json .}}` line into a ContainerStat with no docker
// involved, so the byte/percent/PID parsing is asserted directly here.

func TestParseStatsLine_TypicalRow(t *testing.T) {
	// A real running container: SI units on net/block, binary units on memory.
	line := `{"BlockIO":"1.2MB / 8.19kB","CPUPerc":"12.34%","Container":"abc","ID":"abc123","MemPerc":"5.50%","MemUsage":"10.5MiB / 1.944GiB","Name":"deplo-app","NetIO":"3.4kB / 5.6kB","PIDs":"7"}`
	st, ok := parseStatsLine(line)
	if !ok {
		t.Fatal("expected the line to parse")
	}
	if st.Name != "deplo-app" {
		t.Errorf("name = %q, want deplo-app", st.Name)
	}
	if st.CpuPct != 12.34 {
		t.Errorf("cpu = %v, want 12.34", st.CpuPct)
	}
	if st.MemPct != 5.50 {
		t.Errorf("memPct = %v, want 5.5", st.MemPct)
	}
	// 10.5 MiB = 10.5 * 1024^2 = 11010048; 1.944 GiB = 1.944 * 1024^3 = 2087354106 (rounded).
	if st.MemUsed != 11010048 {
		t.Errorf("memUsed = %d, want 11010048", st.MemUsed)
	}
	if st.MemLimit != 2087354106 {
		t.Errorf("memLimit = %d, want 2087354106", st.MemLimit)
	}
	// Net: 3.4 kB / 5.6 kB (decimal).
	if st.NetRx != 3400 || st.NetTx != 5600 {
		t.Errorf("net = %d/%d, want 3400/5600", st.NetRx, st.NetTx)
	}
	// Block: 1.2 MB / 8.19 kB (decimal).
	if st.BlockRead != 1200000 || st.BlockWrite != 8190 {
		t.Errorf("block = %d/%d, want 1200000/8190", st.BlockRead, st.BlockWrite)
	}
	if st.Pids != 7 {
		t.Errorf("pids = %d, want 7", st.Pids)
	}
}

func TestParseStatsLine_ZeroAndDashes(t *testing.T) {
	// A just-stopped container docker still lists renders "--" and "0B" fields;
	// every numeric must degrade to 0, never a garbage parse.
	line := `{"BlockIO":"0B / 0B","CPUPerc":"--","Container":"x","ID":"x","MemPerc":"--","MemUsage":"0B / 0B","Name":"deplo-x","NetIO":"0B / 0B","PIDs":"0"}`
	st, ok := parseStatsLine(line)
	if !ok {
		t.Fatal("expected the line to parse")
	}
	if st.CpuPct != 0 || st.MemPct != 0 || st.MemUsed != 0 || st.NetRx != 0 || st.BlockWrite != 0 || st.Pids != 0 {
		t.Errorf("expected all-zero stat, got %+v", st)
	}
}

func TestParseStatsLine_RejectsGarbage(t *testing.T) {
	for _, line := range []string{"", "   ", "not json", "{"} {
		if _, ok := parseStatsLine(line); ok {
			t.Errorf("parseStatsLine(%q) = ok, want not-ok", line)
		}
	}
}

func TestSplitSizes(t *testing.T) {
	rx, tx := splitSizes("1.2kB / 3.4kB")
	if rx != 1200 || tx != 3400 {
		t.Errorf("splitSizes = %d/%d, want 1200/3400", rx, tx)
	}
	// A malformed pair (no slash) puts everything on the first side.
	a, b := splitSizes("512B")
	if a != 512 || b != 0 {
		t.Errorf("splitSizes(single) = %d/%d, want 512/0", a, b)
	}
}

func TestParsePercentAndPids(t *testing.T) {
	if v := parsePercent("42.0%"); v != 42.0 {
		t.Errorf("parsePercent = %v, want 42", v)
	}
	if v := parsePercent("--"); v != 0 {
		t.Errorf("parsePercent(--) = %v, want 0", v)
	}
	if v := parsePids("15"); v != 15 {
		t.Errorf("parsePids = %d, want 15", v)
	}
	if v := parsePids(""); v != 0 {
		t.Errorf("parsePids(empty) = %d, want 0", v)
	}
}
