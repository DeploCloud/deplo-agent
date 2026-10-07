package server

import (
	"fmt"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"io/fs"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func addScopeError(r *pb.CleanupScopeResult, msg string) {
	if r.Error == "" {
		r.Error = msg
		return
	}
	r.Error += "; " + msg
}

type scopeFailures struct {
	msgs []string
	n    int
}

func (f *scopeFailures) add(object, msg string) {
	f.n++
	if len(f.msgs) < 3 {
		f.msgs = append(f.msgs, object+": "+msg)
	}
}
func (f *scopeFailures) summary() string {
	if f.n == 0 {
		return ""
	}
	s := strings.Join(f.msgs, "; ")
	if extra := f.n - len(f.msgs); extra > 0 {
		s += fmt.Sprintf(" (and %d more)", extra)
	}
	return s
}
func failedScope(r *pb.CleanupScopeResult, msg string) *pb.CleanupScopeResult {
	r.ReclaimedBytes = 0
	r.ItemsRemoved = 0
	r.Items = nil
	r.Error = msg
	return r
}
func addItem(r *pb.CleanupScopeResult, id string) {
	if len(r.Items) < cleanupMaxItems {
		r.Items = append(r.Items, id)
	}
}
func olderThan(ts string, cutoff time.Time) bool {
	if cutoff.IsZero() {
		return true
	}
	t, ok := parseDockerTime(ts)
	if !ok {
		return false
	}
	return t.Before(cutoff)
}

var dockerTimeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999 -0700 MST",
	"2006-01-02 15:04:05 -0700 MST",
}

func parseDockerTime(ts string) (time.Time, bool) {
	ts = strings.TrimSpace(ts)
	if ts == "" {
		return time.Time{}, false
	}
	for _, layout := range dockerTimeLayouts {
		if t, err := time.Parse(layout, ts); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
func parseHumanSize(s string) int64 {
	s = strings.TrimSpace(s)
	end := 0
	for end < len(s) && (s[end] == '.' || (s[end] >= '0' && s[end] <= '9')) {
		end++
	}
	if end == 0 {
		return 0
	}
	n, err := strconv.ParseFloat(s[:end], 64)
	if err != nil {
		return 0
	}
	mult, ok := sizeUnits[strings.TrimSpace(s[end:])]
	if !ok {
		return 0
	}
	return int64(math.Round(n * mult))
}

var sizeUnits = map[string]float64{
	"":    1,
	"B":   1,
	"kB":  1e3,
	"KB":  1e3,
	"MB":  1e6,
	"GB":  1e9,
	"TB":  1e12,
	"PB":  1e15,
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
	"TiB": 1 << 40,
}

func parsePrunedTotal(out string) (int64, bool) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		for _, prefix := range []string{"Total reclaimed space:", "Total:"} {
			v, ok := strings.CutPrefix(line, prefix)
			if !ok {
				continue
			}
			v = strings.TrimSpace(v)
			if v != "" && v[0] >= '0' && v[0] <= '9' {
				return parseHumanSize(v), true
			}
		}
	}
	return 0, false
}

var cacheRecordID = regexp.MustCompile(`^[a-z0-9]{12,}$`)

func prunedCacheRecordIDs(out string) []string {
	var ids []string
	for _, line := range splitLines(out) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if cacheRecordID.MatchString(fields[0]) {
			ids = append(ids, fields[0])
		}
	}
	return ids
}
func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512
			return nil
		}
		if !d.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}
func splitLines(out string) []string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}
func uniqueLines(out string) []string {
	seen := map[string]bool{}
	var lines []string
	for _, l := range splitLines(out) {
		if !seen[l] {
			seen[l] = true
			lines = append(lines, l)
		}
	}
	return lines
}
func dockerErr(what string, res dockercli.Result) string {
	msg := strings.TrimSpace(res.Stderr)
	if msg == "" {
		msg = strings.TrimSpace(res.Stdout)
	}
	if msg == "" {
		return fmt.Sprintf("docker %s exited %d", what, res.Code)
	}
	return fmt.Sprintf("docker %s: %s", what, msg)
}
