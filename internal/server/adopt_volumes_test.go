package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const anonVol = "35ca4ea542cb2dbf1d35f1fbb950d6c394f01218577ad9a983fcb46bcead617d"

// fakeAdoptDocker answers the few docker calls adoptVolume makes and records them.
type fakeAdoptDocker struct {
	exists  bool
	mounts  string
	copyErr bool
	calls   []string
}

func (f *fakeAdoptDocker) run(_ context.Context, _ time.Duration, args ...string) (dockercli.Result, error) {
	f.calls = append(f.calls, strings.Join(args, " "))
	switch args[0] {
	case "volume":
		if args[1] == "inspect" && !f.exists {
			return dockercli.Result{Code: 1}, nil
		}
	case "ps":
		return dockercli.Result{Stdout: "c1\n"}, nil
	case "inspect":
		return dockercli.Result{Stdout: f.mounts}, nil
	case "run":
		if f.copyErr {
			return dockercli.Result{Code: 1, Stderr: "no space left"}, nil
		}
	}
	return dockercli.Result{}, nil
}

func (f *fakeAdoptDocker) did(prefix string) bool {
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

var pgAdoption = &pb.VolumeAdoption{Service: "fise-db", Target: "/var/lib/postgresql/data", Volume: "deplo-fise-fise-db-data"}

func TestAdoptVolume_carriesTheUnnamedVolumeOnce(t *testing.T) {
	f := &fakeAdoptDocker{mounts: "true " + anonVol + "=/var/lib/postgresql/data"}
	carried, err := adoptVolume(context.Background(), f.run, "deplo-fise", pgAdoption)
	if err != nil || !carried {
		t.Fatalf("carried=%v err=%v", carried, err)
	}
	for _, want := range []string{"stop -t 30 c1", "volume create deplo-fise-fise-db-data", "run --network none -v " + anonVol + ":/from:ro -v deplo-fise-fise-db-data:/to"} {
		if !f.did(want) {
			t.Fatalf("missing %q in %v", want, f.calls)
		}
	}
	if f.did("volume rm") {
		t.Fatal("the old volume or the new one was removed")
	}
}

func TestAdoptVolume_leavesAnExistingNamedVolume(t *testing.T) {
	f := &fakeAdoptDocker{exists: true, mounts: "true " + anonVol + "=/var/lib/postgresql/data"}
	if carried, err := adoptVolume(context.Background(), f.run, "deplo-fise", pgAdoption); err != nil || carried {
		t.Fatalf("carried=%v err=%v", carried, err)
	}
	if f.did("stop") || f.did("run") {
		t.Fatalf("touched a stack whose volume is named already: %v", f.calls)
	}
}

func TestAdoptVolume_nothingUnnamedAtThatPath(t *testing.T) {
	f := &fakeAdoptDocker{mounts: "true deplo-fise_other=/var/lib/postgresql/data " + anonVol + "=/elsewhere"}
	if carried, err := adoptVolume(context.Background(), f.run, "deplo-fise", pgAdoption); err != nil || carried {
		t.Fatalf("carried=%v err=%v", carried, err)
	}
	if f.did("volume create") {
		t.Fatal("created a volume with nothing to carry")
	}
}

func TestAdoptVolume_aFailedCopyChangesNothing(t *testing.T) {
	f := &fakeAdoptDocker{copyErr: true, mounts: "true " + anonVol + "=/var/lib/postgresql/data"}
	_, err := adoptVolume(context.Background(), f.run, "deplo-fise", pgAdoption)
	if err == nil || !strings.Contains(err.Error(), "nothing was changed") {
		t.Fatalf("err=%v", err)
	}
	if !f.did("volume rm deplo-fise-fise-db-data") || !f.did("start c1") {
		t.Fatalf("did not undo: %v", f.calls)
	}
	if f.did("volume rm " + anonVol) {
		t.Fatal("removed the old volume")
	}
}

func TestAdoptVolume_refusesUnsafeInput(t *testing.T) {
	for _, a := range []*pb.VolumeAdoption{
		{Service: "db", Target: "relative", Volume: "deplo-x-data"},
		{Service: "db;rm", Target: "/data", Volume: "deplo-x-data"},
		{Service: "db", Target: "/data/../etc", Volume: "deplo-x-data"},
		{Service: "db", Target: "/data", Volume: "../x"},
	} {
		f := &fakeAdoptDocker{}
		if _, err := adoptVolume(context.Background(), f.run, "deplo-x", a); err == nil || len(f.calls) != 0 {
			t.Fatalf("%v: err=%v calls=%v", a, err, f.calls)
		}
	}
}
