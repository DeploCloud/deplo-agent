package server

import (
	"context"
	"errors"
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
	got, err := setAside(context.Background(), d.run, "fise", []string{"deplo-fise-fise-db-data"}, at)
	if err != nil || len(got) != 1 || got[0].Volume != "deplo-fise-fise-db-data-aside-20261123T040506Z" || got[0].At != "2026-11-23T04:05:06Z" {
		t.Fatalf("set aside %+v, %v", got, err)
	}
	all := strings.Join(d.ran, "\n")
	if !strings.Contains(all, "volume create --label deplo.aside.of=deplo-fise-fise-db-data --label deplo.aside.at=2026-11-23T04:05:06Z deplo-fise-fise-db-data-aside-20261123T040506Z") ||
		!strings.Contains(all, "-v deplo-fise-fise-db-data:/from:ro -v deplo-fise-fise-db-data-aside-20261123T040506Z:/to") {
		t.Fatalf("commands:\n%s", all)
	}
	d.ran = nil
	if _, err := setAside(context.Background(), d.run, "fise", []string{"deplo-fise-fise-db-data"}, at); err != nil || strings.Contains(strings.Join(d.ran, "\n"), "cp -a") {
		t.Fatalf("a copy made before was made again: %v\n%s", err, strings.Join(d.ran, "\n"))
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
