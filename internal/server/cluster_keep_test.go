package server

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

type keepFixture struct {
	*returnFixture
	kept    []lease.Keep
	purged  []string
	stopped []string
	keepErr error
	// purgeErrs fail that many removals of a saved stack before one goes through.
	purgeErrs int
}

func newKeep(t *testing.T) *keepFixture {
	t.Helper()
	f := &keepFixture{returnFixture: newReturn(t)}
	f.wireKeep()
	return f
}

func (f *keepFixture) wireKeep() {
	f.wire()
	f.g.stop = func(_ context.Context, slug string) error { f.stopped = append(f.stopped, slug); return nil }
	f.g.keep = func(_ context.Context, e lease.Entry, k lease.Keep, member string, _ func() bool) (*lease.Saved, error) {
		f.kept = append(f.kept, k)
		if f.keepErr != nil {
			return nil, f.keepErr
		}
		return &lease.Saved{Slug: e.Slug, Ref: k.Ref, ObjectKey: "deplo/t/app/prj_2/left-x-" + member + ".tar.gz", SizeBytes: 42, Sha256: "abc", At: "2026-03-01T12:05:00Z"}, nil
	}
	f.g.purge = func(_ context.Context, e lease.Entry, _ lease.Keep, _ func() bool) error {
		if f.purgeErrs > 0 {
			f.purgeErrs--
			return errors.New("docker did not answer")
		}
		f.purged = append(f.purged, e.Slug)
		f.gone[e.Slug] = true
		return nil
	}
}

func (f *keepFixture) restart() {
	f.reopen()
	f.wireKeep()
	f.g.Check(context.Background())
}

const keepRequest = `{"kind":"BACKUP_KIND_PROJECT","s3":{"secretKey":"secret_test","objectKey":"deplo/t/app/prj_2/left"},"project":{"slug":"files","volumeNames":["deplo-files-data"]}}`

// asked is the leader's order to a member whose work it moved: what moved, and where to save what holds data.
func askedBack(keeps ...lease.Keep) lease.Order {
	o := movedAway(1)
	o.Return, o.Keep = true, keeps
	return o
}

func filesKeep() lease.Keep {
	return lease.Keep{Slug: "files", Ref: "dst_1", Request: json.RawMessage(keepRequest)}
}

// back is a member whose lease ran out while it was away, started again.
func back(t *testing.T) *keepFixture {
	f := newKeep(t)
	f.renew(orderFor(1))
	f.clk.Add(5 * time.Minute)
	f.restart()
	return f
}

func TestKeep_whatHoldsDataIsSavedBeforeItIsRemovedAndTheLeaderIsTold(t *testing.T) {
	f := back(t)
	f.renew(askedBack(filesKeep()))
	if len(f.kept) != 1 || f.kept[0].Slug != "files" || strings.Join(f.purged, ",") != "files" {
		t.Fatalf("saved %+v, purged %v, want the one moved stack that holds data", f.kept, f.purged)
	}
	// web holds nothing: it is only removed. files went with its backup, not through the plain removal.
	if got := f.removedSlugs(); got != "web" {
		t.Fatalf("removed without a backup: %q", got)
	}
	r := f.renew(askedBack(filesKeep()))
	if r.Returned == nil || strings.Join(r.Returned.Removed, ",") != "files,web" {
		t.Fatalf("report %+v", r.Returned)
	}
	if len(r.Returned.Saved) != 1 || r.Returned.Saved[0].Slug != "files" || r.Returned.Saved[0].Ref != "dst_1" || r.Returned.Saved[0].SizeBytes != 42 {
		t.Fatalf("saved %+v", r.Returned.Saved)
	}
	if len(r.Returned.Kept) != 0 || len(f.kept) != 1 {
		t.Fatalf("kept %+v, saved %d times", r.Returned.Kept, len(f.kept))
	}
}

func TestKeep_aBackupThatFailsLeavesTheVolumeAndSaysWhy(t *testing.T) {
	f := back(t)
	f.keepErr = errors.New("no space left on device")
	f.renew(askedBack(filesKeep()))
	// The copy still must not run: the stack goes, by the removal that never touches a volume.
	if got := f.removedSlugs(); got != "files,web" || len(f.purged) != 0 {
		t.Fatalf("removed %q, purged %v", got, f.purged)
	}
	r := f.renew(askedBack(filesKeep()))
	if r.Returned == nil || len(r.Returned.Saved) != 0 || len(r.Returned.Kept) != 1 {
		t.Fatalf("report %+v", r.Returned)
	}
	if k := r.Returned.Kept[0]; k.Slug != "files" || k.Reason != lease.KeptBackupFailed || k.Error != "no space left on device" || k.Ref != "dst_1" {
		t.Fatalf("kept %+v", k)
	}
}

func TestKeep_aDestinationOnAnotherMembersDiskIsNotTried(t *testing.T) {
	f := back(t)
	k := filesKeep()
	k.StoreMember = "srv_b"
	f.renew(askedBack(k))
	if len(f.kept) != 0 {
		t.Fatalf("tried to write to another member's disk: %+v", f.kept)
	}
	r := f.renew(askedBack(k))
	if r.Returned == nil || len(r.Returned.Kept) != 1 || r.Returned.Kept[0].Reason != lease.KeptStoreElsewhere || f.removedSlugs() != "files,web" {
		t.Fatalf("report %+v, removed %q", r.Returned, f.removedSlugs())
	}
	// Its own disk is another matter.
	f = back(t)
	k.StoreMember = "srv_a"
	f.renew(askedBack(k))
	if len(f.kept) != 1 {
		t.Fatal("a destination on this member's own disk was not written to")
	}
}

func TestKeep_aLeaderWithNoDestinationForItLeavesTheVolume(t *testing.T) {
	f := back(t)
	f.renew(askedBack())
	r := f.renew(askedBack())
	if len(f.kept) != 0 || r.Returned == nil || len(r.Returned.Kept) != 1 || r.Returned.Kept[0].Reason != lease.KeptNoDestination {
		t.Fatalf("saved %+v, report %+v", f.kept, r.Returned)
	}
	if got := f.removedSlugs(); got != "files,web" {
		t.Fatalf("removed %q", got)
	}
}

func TestKeep_whatHoldsDataWaitsStoppedForTheLeadersWord(t *testing.T) {
	f := back(t)
	// The first renewal after a dead lease may not say yet that the leader moved anything.
	f.renew(movedAway(1))
	if got := f.removedSlugs(); got != "web" {
		t.Fatalf("removed %q before the leader said where its data goes", got)
	}
	if !slices.Contains(f.stopped, "files") {
		t.Fatalf("files was left as it was while waiting: stopped %v", f.stopped)
	}
	f.renew(askedBack(filesKeep()))
	if len(f.kept) != 1 {
		t.Fatalf("not saved once the leader spoke: %+v", f.kept)
	}

	// A leader that never asks: after the wait the copy goes, and its volume stays.
	f = back(t)
	f.renew(movedAway(1))
	f.clk.Add(keepGrace + time.Second)
	f.renew(movedAway(1))
	if got := f.removedSlugs(); got != "files,web" || len(f.kept) != 0 {
		t.Fatalf("removed %q, saved %+v", got, f.kept)
	}
	if c := f.g.clusters["clu_1"]; c.Returned == nil || len(c.Returned.Kept) != 1 || c.Returned.Kept[0].Reason != lease.KeptNoDestination {
		t.Fatalf("lease %+v", c.Returned)
	}
}

func TestKeep_theCredentialsNeverReachTheLeaseFile(t *testing.T) {
	f := back(t)
	f.keepErr = errors.New("unreachable")
	f.renew(askedBack(filesKeep()))
	raw, err := os.ReadFile(f.file)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret_test") || strings.Contains(string(raw), "secretKey") {
		t.Fatalf("the destination's credentials were written to disk: %s", raw)
	}
	if !strings.Contains(string(raw), lease.KeptBackupFailed) {
		t.Fatalf("what was kept is not remembered for the leader: %s", raw)
	}
}

func TestKeep_anArchiveIsWrittenDownBeforeItsVolumeGoesAndNeverMadeTwice(t *testing.T) {
	f := back(t)
	f.purgeErrs = 1
	if r := f.renew(askedBack(filesKeep())); r.Returned != nil {
		t.Fatalf("reported a return that is not done: %+v", r.Returned)
	}
	// The archive exists and its volume does not go yet: an agent that stopped here would still know where it is.
	raw, _ := os.ReadFile(f.file)
	if !strings.Contains(string(raw), "left-x-srv_a.tar.gz") || len(f.purged) != 0 {
		t.Fatalf("the lease file after the archive: %s (purged %v)", raw, f.purged)
	}
	f.restart()
	f.renew(askedBack(filesKeep()))
	if len(f.kept) != 1 || strings.Join(f.purged, ",") != "files" {
		t.Fatalf("archived %d times, purged %v", len(f.kept), f.purged)
	}
	r := f.renew(askedBack(filesKeep()))
	if r.Returned == nil || len(r.Returned.Saved) != 1 || r.Returned.Saved[0].Slug != "files" || strings.Join(r.Returned.Removed, ",") != "files,web" {
		t.Fatalf("report %+v", r.Returned)
	}
}

func keepService(t *testing.T, slug string) (*Service, func() []string) {
	t.Helper()
	argv := dockerOnPath(t, "exit 0")
	s := &Service{agentDir: t.TempDir(), stackDir: t.TempDir()}
	mustWrite(t, s.stackPath(slug), "services: {db: {image: postgres}}")
	return s, argv
}

func stubLeft(t *testing.T, fn func(*pb.BackupRequest, io.Writer) (*pb.BackupResult, error)) {
	t.Helper()
	old, tries, wait := runLeftBackup, leftTries, leftWait
	runLeftBackup = func(_ *Service, _ context.Context, req *pb.BackupRequest, out io.Writer) (*pb.BackupResult, error) {
		return fn(req, out)
	}
	leftTries, leftWait = 2, time.Millisecond
	t.Cleanup(func() { runLeftBackup, leftTries, leftWait = old, tries, wait })
}

func serviceKeep(slug string) lease.Keep {
	return lease.Keep{Slug: slug, Ref: "dst_1", Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","ageRecipient":"age1example",
		"s3":{"bucket":"b","objectKey":"deplo/team_a/database/db_1/left"},
		"project":{"slug":"` + slug + `","volumeNames":["deplo-` + slug + `_data"]}}`)}
}

func TestKeepLeft_archivesTheVolumeThenTakesTheStackDownWithIt(t *testing.T) {
	const slug = "deplo-agent-test-keep"
	s, argv := keepService(t, slug)
	var got *pb.BackupRequest
	stubLeft(t, func(req *pb.BackupRequest, _ io.Writer) (*pb.BackupResult, error) {
		if all := strings.Join(argv(), "\n"); strings.Contains(all, "down") {
			t.Fatalf("the stack went before its backup: %q", all)
		}
		got = req
		return &pb.BackupResult{Ok: true, ObjectKey: req.GetS3().GetObjectKey(), SizeBytes: 2048, DecryptedSizeBytes: 4096, Sha256: "feed"}, nil
	})
	e, still := lease.Entry{Kind: "database", ID: "db_1", Member: "srv_b", Slug: slug, SingleWriter: true}, func() bool { return true }
	saved, err := s.keepLeft(context.Background(), e, serviceKeep(slug), "srv_a", still)
	if err != nil {
		t.Fatal(err)
	}
	if all := strings.Join(argv(), "\n"); strings.Contains(all, "down") || strings.Contains(all, "volume rm") || !isFile(s.stackPath(slug)) {
		t.Fatalf("the archive step removed something: %q", all)
	}
	if err := s.purgeLeft(context.Background(), e, serviceKeep(slug), still); err != nil {
		t.Fatal(err)
	}
	key := got.GetS3().GetObjectKey()
	if !regexp.MustCompile(`^deplo/team_a/database/db_1/left-\d{8}T\d{6}Z-srv_a\.tar\.gz\.age$`).MatchString(key) {
		t.Fatalf("object key %q", key)
	}
	if got.GetKind() != pb.BackupKind_BACKUP_KIND_PROJECT || got.GetStreamOut() || got.GetProject().GetComposeYaml() != "" {
		t.Fatalf("request %+v", got)
	}
	if saved.Slug != slug || saved.Ref != "dst_1" || saved.ObjectKey != key || saved.SizeBytes != 2048 || saved.DecryptedSizeBytes != 4096 || saved.Sha256 != "feed" || saved.At == "" {
		t.Fatalf("saved %+v", saved)
	}
	all := strings.Join(argv(), "\n")
	if !strings.Contains(all, "compose -p deplo-"+slug) || !strings.Contains(all, "down --remove-orphans -v") {
		t.Fatalf("the stack was not taken down with its volumes: %q", all)
	}
	if !strings.Contains(all, "volume rm") || !strings.Contains(all, "deplo-"+slug+"_data") {
		t.Fatalf("the volume the archive holds was not removed by name: %q", all)
	}
	if isFile(s.stackPath(slug)) {
		t.Fatal("the stack file is still here")
	}
}

func TestKeepLeft_namesTheArchiveAfterTheFailoverSoARepeatWritesOverIt(t *testing.T) {
	const slug = "deplo-agent-test-keep"
	s, _ := keepService(t, slug)
	var keys []string
	stubLeft(t, func(req *pb.BackupRequest, _ io.Writer) (*pb.BackupResult, error) {
		keys = append(keys, req.GetS3().GetObjectKey())
		return &pb.BackupResult{Ok: true, SizeBytes: 1}, nil
	})
	k := serviceKeep(slug)
	k.FailedOverAt = "2026-03-01T14:32:05+01:00"
	e := lease.Entry{Kind: "database", ID: "db_1", Member: "srv_b", Slug: slug, SingleWriter: true}
	for range 2 {
		saved, err := s.keepLeft(context.Background(), e, k, "srv_a", func() bool { return true })
		if err != nil || saved.ObjectKey != "deplo/team_a/database/db_1/left-20260301T133205Z-srv_a.tar.gz.age" {
			t.Fatalf("saved %+v (%v)", saved, err)
		}
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("an agent that stopped mid-archive writes a second file: %v", keys)
	}
}

func TestKeepLeft_aFailedBackupTouchesNothing(t *testing.T) {
	const slug = "deplo-agent-test-keep"
	s, argv := keepService(t, slug)
	tries := 0
	stubLeft(t, func(*pb.BackupRequest, io.Writer) (*pb.BackupResult, error) {
		tries++
		return nil, errors.New("the bucket did not answer")
	})
	saved, err := s.keepLeft(context.Background(), lease.Entry{Slug: slug, SingleWriter: true}, serviceKeep(slug), "srv_a", func() bool { return true })
	if err == nil || saved != nil || !strings.Contains(err.Error(), "the bucket did not answer") {
		t.Fatalf("saved %+v, err %v", saved, err)
	}
	if tries != 2 {
		t.Fatalf("tried %d times, want every try the agent allows itself", tries)
	}
	if all := strings.Join(argv(), "\n"); strings.Contains(all, "down") || strings.Contains(all, "volume rm") {
		t.Fatalf("docker removed something: %q", all)
	}
	if !isFile(s.stackPath(slug)) {
		t.Fatal("the stack file went with a backup that failed")
	}
}

func TestKeepLeft_refusesAKeepThatIsNotThisStacksVolumes(t *testing.T) {
	const slug = "deplo-agent-test-keep"
	s, argv := keepService(t, slug)
	stubLeft(t, func(*pb.BackupRequest, io.Writer) (*pb.BackupResult, error) {
		t.Fatal("a backup ran")
		return nil, nil
	})
	e := lease.Entry{Slug: slug, SingleWriter: true}
	for name, k := range map[string]lease.Keep{
		"another stack":    {Slug: slug, Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","s3":{"objectKey":"k/left"},"project":{"slug":"other","volumeNames":["deplo-other_data"]}}`)},
		"a database dump":  {Slug: slug, Request: json.RawMessage(`{"kind":"BACKUP_KIND_DATABASE","s3":{"objectKey":"k/left"},"database":{"container":"x"}}`)},
		"a foreign volume": {Slug: slug, Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","s3":{"objectKey":"k/left"},"project":{"slug":"` + slug + `","volumeNames":["pgdata"]}}`)},
		"a stream":         {Slug: slug, Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","streamOut":true,"project":{"slug":"` + slug + `","volumeNames":["deplo-x_data"]}}`)},
		"no destination":   {Slug: slug, Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","project":{"slug":"` + slug + `","volumeNames":["deplo-x_data"]}}`)},
		"unreadable":       {Slug: slug, Request: json.RawMessage(`{`)},
	} {
		if _, err := s.keepLeft(context.Background(), e, k, "srv_a", func() bool { return true }); err == nil {
			t.Errorf("%s: archived", name)
		}
		if name != "no destination" && s.purgeLeft(context.Background(), e, k, func() bool { return true }) == nil {
			t.Errorf("%s: purged", name)
		}
	}
	// Placed here again, or being started here: its volume is the live one now.
	if _, err := s.keepLeft(context.Background(), e, serviceKeep(slug), "srv_a", func() bool { return false }); err == nil {
		t.Error("a stack this member runs again was archived")
	}
	if s.purgeLeft(context.Background(), e, serviceKeep(slug), func() bool { return false }) == nil {
		t.Error("a stack this member runs again was removed with its volume")
	}
	if got := strings.Join(argv(), ""); got != "" {
		t.Fatalf("docker ran: %q", got)
	}
}

func TestKeepLeft_writesAnArchiveThatHoldsTheVolume(t *testing.T) {
	const slug = "deplo-agent-test-keep"
	vol := "deplo-" + slug + "_data"
	data := t.TempDir()
	mustWrite(t, filepath.Join(data, "PG_VERSION"), "16\n")
	// docker as a script: the helper's tar reads the fixture, everything else answers nothing.
	argv := dockerOnPath(t, `case "$*" in *"tar -C /v"*) exec tar -C `+data+` -cf - . ;; esac`)
	s, root := newStoreService(t)
	s.agentDir = t.TempDir()
	mustWrite(t, s.stackPath(slug), "services: {db: {image: postgres}}")
	recipient, identity := testKeypair(t)
	k := lease.Keep{Slug: slug, Ref: "dst_1", Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","ageRecipient":"` + recipient + `",
		"store":{"objectKey":"deplo/team_a/database/db_1/left"},"project":{"slug":"` + slug + `","volumeNames":["` + vol + `"]}}`)}

	e, still := lease.Entry{Kind: "database", ID: "db_1", Member: "srv_b", Slug: slug, SingleWriter: true}, func() bool { return true }
	saved, err := s.keepLeft(context.Background(), e, k, "srv_a", still)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.purgeLeft(context.Background(), e, k, still); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, saved.ObjectKey))
	if err != nil {
		t.Fatalf("the archive is not in the store: %v", err)
	}
	if int64(len(raw)) != saved.SizeBytes || saved.Sha256 == "" || bytes.Contains(raw, []byte("PG_VERSION")) {
		t.Fatalf("saved %+v, %d bytes on disk, readable without the key: %v", saved, len(raw), bytes.Contains(raw, []byte("PG_VERSION")))
	}
	rd, err := openArtifactReader(bytes.NewReader(raw), identity)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	tr := tar.NewReader(rd)
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		names = append(names, h.Name)
	}
	if !slices.Contains(names, "volumes/"+vol+"/PG_VERSION") {
		t.Fatalf("the archive holds %v, want the volume as the restore reads it", names)
	}
	all := strings.Join(argv(), "\n")
	if !strings.Contains(all, vol+":/v:ro") || strings.Index(all, "tar -C /v") > strings.Index(all, "down --remove-orphans -v") {
		t.Fatalf("the volume was not read before it was removed: %q", all)
	}
}
