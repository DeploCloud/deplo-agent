package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// wireAside sets every moved stack's volume aside as one dated copy, and records the removals after.
func (f *keepFixture) wireAside(fail error) (aside *[]string, dropped *[]string) {
	aside, dropped = &[]string{}, &[]string{}
	f.g.aside = func(_ context.Context, e lease.Entry, at time.Time) ([]lease.Aside, error) {
		if fail != nil {
			return nil, fail
		}
		*aside = append(*aside, e.Slug)
		vol := "deplo-" + e.Slug + "-data"
		return []lease.Aside{{Slug: e.Slug, Volume: asideName(vol, at), Of: vol, At: at.UTC().Format(time.RFC3339), SizeBytes: 7}}, nil
	}
	f.g.dropAside = func(_ context.Context, e lease.Entry, _ []lease.Aside, _ func() bool) error {
		*dropped = append(*dropped, e.Slug)
		f.gone[e.Slug] = true
		return nil
	}
	return aside, dropped
}

func TestAside_withNowhereToSaveItTheDataIsSetAsideAndTheStackGoes(t *testing.T) {
	f := back(t)
	aside, dropped := f.wireAside(nil)
	f.renew(askedBack())
	r := f.renew(askedBack())
	if strings.Join(*aside, ",") != "files" || strings.Join(*dropped, ",") != "files" {
		t.Fatalf("set aside %v, dropped %v", *aside, *dropped)
	}
	if r.Returned == nil || len(r.Returned.Kept) != 0 || len(r.Returned.Aside) != 1 || !strings.Contains(strings.Join(r.Returned.Removed, ","), "files") {
		t.Fatalf("report %+v", r.Returned)
	}
	if a := r.Returned.Aside[0]; a.Of != "deplo-files-data" || !asideSuffix.MatchString(a.Volume) || a.SizeBytes != 7 {
		t.Fatalf("aside %+v", a)
	}
}

func TestAside_savedDataIsSetAsideTooBeforeItsVolumeGoes(t *testing.T) {
	f := back(t)
	aside, _ := f.wireAside(nil)
	k := filesKeep()
	k.FailedOverAt = "2026-03-01T12:01:02Z"
	f.renew(askedBack(k))
	r := f.renew(askedBack(k))
	if strings.Join(*aside, ",") != "files" || strings.Join(f.purged, ",") != "files" || len(r.Returned.Saved) != 1 {
		t.Fatalf("aside %v, purged %v, saved %+v", *aside, f.purged, r.Returned.Saved)
	}
	if a := r.Returned.Aside; len(a) != 1 || a[0].Volume != "deplo-files-data-aside-20260301T120102Z" {
		t.Fatalf("the copy is not dated by the failover: %+v", a)
	}
}

func TestAside_aCopyThatCannotBeMadeLeavesTheVolume(t *testing.T) {
	f := back(t)
	f.wireAside(errors.New("no space left on device"))
	f.renew(askedBack(filesKeep()))
	if len(f.purged) != 0 {
		t.Fatalf("a volume went without its copy: purged %v", f.purged)
	}
}

// asideDocker answers the volume helper's commands from a table of volumes, and records every command.
type asideDocker struct {
	vols map[string]bool
	ran  []string
}

func (d *asideDocker) run(_ context.Context, _ time.Duration, args ...string) (dockercli.Result, error) {
	cmd := strings.Join(args, " ")
	d.ran = append(d.ran, cmd)
	switch {
	case strings.HasPrefix(cmd, "volume inspect --format"):
		return dockercli.Result{Stdout: "/nonexistent\n"}, nil
	case strings.HasPrefix(cmd, "volume inspect "):
		if d.vols[args[len(args)-1]] {
			return dockercli.Result{}, nil
		}
		return dockercli.Result{Code: 1, Stderr: "no such volume"}, nil
	case strings.HasPrefix(cmd, "volume create"):
		d.vols[args[len(args)-1]] = true
	case strings.HasPrefix(cmd, "ps -a"):
		return dockercli.Result{Stdout: "c1\n"}, nil
	case strings.HasPrefix(cmd, "inspect --format"):
		return dockercli.Result{Stdout: "deplo-fise-fise-db-data " + strings.Repeat("a", 64) + " \n"}, nil
	case strings.HasPrefix(cmd, "volume ls -q"):
		return dockercli.Result{Stdout: "deplo-fise_cache\n"}, nil
	}
	return dockercli.Result{}, nil
}

func TestAside_aVolumeIsCopiedToALabelledDatedCopyOnce(t *testing.T) {
	d := &asideDocker{vols: map[string]bool{"deplo-fise-fise-db-data": true}}
	at := time.Date(2026, 11, 23, 4, 5, 6, 0, time.UTC)
	got, err := setAside(context.Background(), d.run, "fise", []string{"deplo-fise-fise-db-data"}, "", at)
	if err != nil || len(got) != 1 || got[0].Volume != "deplo-fise-fise-db-data-aside-20261123T040506Z" || got[0].At != "2026-11-23T04:05:06Z" {
		t.Fatalf("set aside %+v, %v", got, err)
	}
	all := strings.Join(d.ran, "\n")
	if !strings.Contains(all, "volume create --label deplo.aside.of=deplo-fise-fise-db-data --label deplo.aside.at=2026-11-23T04:05:06Z deplo-fise-fise-db-data-aside-20261123T040506Z") ||
		!strings.Contains(all, "-v deplo-fise-fise-db-data:/from:ro -v deplo-fise-fise-db-data-aside-20261123T040506Z:/to") {
		t.Fatalf("commands:\n%s", all)
	}
	d.ran = nil
	if _, err := setAside(context.Background(), d.run, "fise", []string{"deplo-fise-fise-db-data"}, "", at); err != nil || strings.Contains(strings.Join(d.ran, "\n"), "cp -a") {
		t.Fatalf("a copy made before was made again: %v\n%s", err, strings.Join(d.ran, "\n"))
	}
}

func TestAside_theFilesFolderIsSetAsideWithTheVolumes(t *testing.T) {
	d := &asideDocker{vols: map[string]bool{"deplo-fise-fise-db-data": true}}
	at := time.Date(2026, 11, 23, 4, 5, 6, 0, time.UTC)
	files := t.TempDir()
	got, err := setAside(context.Background(), d.run, "fise", []string{"deplo-fise-fise-db-data"}, files, at)
	if err != nil || len(got) != 1 {
		t.Fatalf("set aside %+v, %v: an empty Files folder has nothing to keep", got, err)
	}
	if err := os.WriteFile(filepath.Join(files, "upload.png"), []byte("png"), 0o644); err != nil {
		t.Fatal(err)
	}
	d.ran = nil
	got, err = setAside(context.Background(), d.run, "fise", []string{"deplo-fise-fise-db-data"}, files, at)
	if err != nil || len(got) != 2 || got[1].Volume != "deplo-fise.files-aside-20261123T040506Z" || got[1].Of != "deplo-fise.files" {
		t.Fatalf("set aside %+v, %v", got, err)
	}
	all := strings.Join(d.ran, "\n")
	if !strings.Contains(all, "volume create --label deplo.aside.of=deplo-fise.files --label deplo.aside.at=2026-11-23T04:05:06Z deplo-fise.files-aside-20261123T040506Z") ||
		!strings.Contains(all, "-v "+files+":/from:ro -v deplo-fise.files-aside-20261123T040506Z:/to") {
		t.Fatalf("commands:\n%s", all)
	}
}

func TestSetAside_aStacksFilesFolderComesWithItsVolumes(t *testing.T) {
	d := &asideDocker{vols: map[string]bool{}}
	was := setAsideRun
	setAsideRun = d.run
	t.Cleanup(func() { setAsideRun = was })
	s := &Service{stackDir: t.TempDir()}
	if err := os.MkdirAll(s.filesRoot("fise"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.filesRoot("fise"), "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := s.SetAside(context.Background(), &pb.SetAsideRequest{Slug: "fise"})
	if err != nil || len(res.GetCopies()) != 1 || res.GetCopies()[0].GetSetAsideOf() != "deplo-fise.files" {
		t.Fatalf("copies %v, %v: the Files folder is set aside even with no volume named", res.GetCopies(), err)
	}
}

func TestAside_aStacksVolumesAreWhatItsContainersMountAndComposeMade(t *testing.T) {
	d := &asideDocker{vols: map[string]bool{}}
	got, err := stackVolumes(context.Background(), d.run, "fise")
	if err != nil || strings.Join(got, ",") != "deplo-fise-fise-db-data,deplo-fise_cache" {
		t.Fatalf("volumes %v, %v: an unnamed one is no Volume, one made outside compose still is", got, err)
	}
}

func TestAside_onlyASetAsideCopyIsRemoved(t *testing.T) {
	s := &Service{}
	for _, name := range []string{"deplo-fise-fise-db-data", "deplo-fise-fise-db-data-aside-2026", "../x-aside-20261123T040506Z"} {
		if _, err := s.RemoveSetAside(context.Background(), &pb.RemoveSetAsideRequest{VolumeName: name}); err == nil {
			t.Fatalf("%q was removed: it is no set-aside copy", name)
		}
	}
}

func TestSetAside_copiesEachVolumeThatExistsAndSkipsAnAbsentOne(t *testing.T) {
	d := &asideDocker{vols: map[string]bool{"deplo-fise-fise-db-data": true}}
	was := setAsideRun
	setAsideRun = d.run
	t.Cleanup(func() { setAsideRun = was })
	s := &Service{}
	res, err := s.SetAside(context.Background(), &pb.SetAsideRequest{Slug: "fise", VolumeNames: []string{"deplo-fise-fise-db-data", "deplo-fise_cache"}})
	if err != nil || len(res.GetCopies()) != 1 {
		t.Fatalf("copies %v, %v: only the volume that exists has a copy", res.GetCopies(), err)
	}
	c := res.GetCopies()[0]
	if c.GetSetAsideOf() != "deplo-fise-fise-db-data" || !asideSuffix.MatchString(c.GetVolumeName()) || c.GetSetAsideAtMs() == 0 {
		t.Fatalf("copy %+v", c)
	}
	if strings.Contains(strings.Join(d.ran, "\n"), "-v deplo-fise_cache:/from") {
		t.Fatalf("an absent volume was copied, which would create it empty:\n%s", strings.Join(d.ran, "\n"))
	}
}

func TestSetAside_refusesACopyOfACopyAndABadName(t *testing.T) {
	s := &Service{}
	for _, v := range []string{"deplo-fise-fise-db-data-aside-20261123T040506Z", "../etc"} {
		if _, err := s.SetAside(context.Background(), &pb.SetAsideRequest{Slug: "fise", VolumeNames: []string{v}}); err == nil {
			t.Fatalf("%q was set aside", v)
		}
	}
}

func TestReadStack_saysWhenTheStackLastRan(t *testing.T) {
	now := time.Date(2026, 11, 23, 7, 49, 13, 0, time.UTC)
	exited := writerContainer{State: "exited", FinishedAt: now.Add(-7 * time.Minute)}
	if at, running := lastRan([]writerContainer{exited}, now); running || at != now.Add(-7*time.Minute).UnixMilli() {
		t.Fatalf("stopped stack: %d %v", at, running)
	}
	if at, running := lastRan([]writerContainer{exited, {State: "running"}}, now); !running || at != now.UnixMilli() {
		t.Fatalf("a running container is running now: %d %v", at, running)
	}
	if at, running := lastRan(nil, now); running || at != 0 {
		t.Fatalf("no container: %d %v", at, running)
	}
	was := listStackContainers
	listStackContainers = func(context.Context, string) ([]writerContainer, error) {
		return []writerContainer{{State: "running"}}, nil
	}
	t.Cleanup(func() { listStackContainers = was })
	res, err := newTestService(t).ReadStack(context.Background(), &pb.StackRef{Slug: "fise"})
	if err != nil || res.GetExists() || !res.GetRunning() || res.GetLastRanMs() == 0 {
		t.Fatalf("read %+v, %v: containers run with no stack file left", res, err)
	}
}

func TestAsideTarget_anotherServersCopyArrivesAsANewLabelledCopy(t *testing.T) {
	d := &asideDocker{vols: map[string]bool{}}
	name := "deplo-fise-fise-db-data-aside-20261123T074913Z"
	if err := createAsideTarget(context.Background(), d.run, name, "deplo-fise-fise-db-data"); err != nil {
		t.Fatal(err)
	}
	if all := strings.Join(d.ran, "\n"); !strings.Contains(all, "volume create --label deplo.aside.of=deplo-fise-fise-db-data --label deplo.aside.at=2026-11-23T07:49:13Z "+name) {
		t.Fatalf("commands:\n%s", all)
	}
	if err := createAsideTarget(context.Background(), d.run, name, "deplo-fise-fise-db-data"); err == nil {
		t.Fatal("an existing copy was written into")
	}
	for _, bad := range [][2]string{{"deplo-fise-fise-db-data", "deplo-fise-fise-db-data"}, {"deplo-other-aside-20261123T074913Z", "deplo-fise-fise-db-data"}} {
		if err := createAsideTarget(context.Background(), d.run, bad[0], bad[1]); err == nil {
			t.Fatalf("%q was taken as a copy of %q", bad[0], bad[1])
		}
	}
}

// pathDocker answers each volume's mountpoint with a folder of the test's own, and lists the set-aside copies it made.
type pathDocker struct {
	asideDocker
	paths map[string]string
}

func (d *pathDocker) run(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error) {
	cmd := strings.Join(args, " ")
	switch {
	case strings.HasPrefix(cmd, "volume inspect --format {{.Mountpoint}} "):
		d.ran = append(d.ran, cmd)
		return dockercli.Result{Stdout: d.paths[args[len(args)-1]] + "\n"}, nil
	case strings.HasPrefix(cmd, "volume ls --filter label="+asideOfLabel):
		d.ran = append(d.ran, cmd)
		var out []string
		for name := range d.vols {
			if of, _, ok := strings.Cut(name, "-aside-"); ok {
				out = append(out, name+"\t"+of+"\t2026-11-24T07:31:41Z")
			}
		}
		return dockercli.Result{Stdout: strings.Join(out, "\n")}, nil
	case strings.HasPrefix(cmd, "volume rm "):
		d.ran = append(d.ran, cmd)
		delete(d.vols, args[len(args)-1])
		return dockercli.Result{}, nil
	}
	return d.asideDocker.run(ctx, timeout, args...)
}

// eu, 24 Nov 2026: a returning member's dated copy of 2 GB was a full copy, on a disk its cluster shares.
func TestAside_aReturningMembersCopyIsMovedAsideNotCopied(t *testing.T) {
	live, aside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(live, "fill"), []byte("2 GB of an App"), 0o600); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 24, 7, 31, 41, 0, time.UTC)
	d := &pathDocker{asideDocker{vols: map[string]bool{}}, map[string]string{"deplo-lab_data": live, asideName("deplo-lab_data", at): aside}}
	got, err := setAside(context.Background(), d.run, "lab", []string{"deplo-lab_data"}, "", at, true)
	if err != nil || len(got) != 1 {
		t.Fatalf("set aside %+v, %v", got, err)
	}
	if b, err := os.ReadFile(filepath.Join(aside, "fill")); err != nil || string(b) != "2 GB of an App" {
		t.Fatalf("the copy holds %q, %v", b, err)
	}
	if left, _ := os.ReadDir(live); len(left) != 0 {
		t.Fatalf("the live volume still holds %d entries", len(left))
	}
	if strings.Contains(strings.Join(d.ran, "\n"), "cp -a") {
		t.Fatalf("the data was copied:\n%s", strings.Join(d.ran, "\n"))
	}
}

func TestAside_whatCannotBeMovedIsCopied(t *testing.T) {
	live := t.TempDir()
	at := time.Date(2026, 11, 24, 7, 31, 41, 0, time.UTC)
	d := &pathDocker{asideDocker{vols: map[string]bool{}}, map[string]string{"deplo-lab_data": live}}
	if _, err := setAside(context.Background(), d.run, "lab", []string{"deplo-lab_data"}, "", at, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(d.ran, "\n"), "cp -a") {
		t.Fatalf("with no path for the copy, nothing copied it:\n%s", strings.Join(d.ran, "\n"))
	}
}

// eu, 24 Nov 2026: a deleted App's dated copy stayed on the member that had just come back.
func TestDropSetAside_aDeletedAppsCopiesGoAndNoOtherAppsDo(t *testing.T) {
	d := &pathDocker{asideDocker{vols: map[string]bool{
		"deplo-lab_data-aside-20261124T073141Z":   true,
		"deplo-lab.files-aside-20261124T073141Z":  true,
		"deplo-memos_data-aside-20261124T073141Z": true,
		"deplo-lab_data":                          true,
	}}, nil}
	if left := dropSetAside(context.Background(), d.run, "lab", []string{"deplo-lab_data"}); left != "" {
		t.Fatal(left)
	}
	if d.vols["deplo-lab_data-aside-20261124T073141Z"] || d.vols["deplo-lab.files-aside-20261124T073141Z"] {
		t.Fatalf("a copy of the deleted App stayed: %v", d.vols)
	}
	if !d.vols["deplo-memos_data-aside-20261124T073141Z"] {
		t.Fatal("another App's copy went")
	}
}
