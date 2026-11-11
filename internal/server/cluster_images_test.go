package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// imageHost is one host's images: newest first, like docker image ls.
type imageHost struct {
	mu        sync.Mutex
	tags      []string
	saved     []string
	archives  map[string]string
	loadErr   error
	inUse     map[string]bool
	removed   []string
	loadCalls int
	// arch is what an image says it was built for; one not named says nothing.
	arch map[string]string
}

func newImageHost(tags ...string) *imageHost {
	return &imageHost{tags: tags, archives: map[string]string{}, inUse: map[string]bool{}, arch: map[string]string{}}
}

func (f *imageHost) ops() clusterImageOps {
	return clusterImageOps{
		has: func(_ context.Context, ref string) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return slices.Contains(f.tags, ref), nil
		},
		save: func(_ context.Context, ref string, w io.Writer) error {
			f.mu.Lock()
			f.saved = append(f.saved, ref)
			f.mu.Unlock()
			_, err := io.WriteString(w, "ARCHIVE of "+ref)
			return err
		},
		load: func(_ context.Context, ref string, feed func(io.Writer) error) (int64, string, error) {
			var buf bytes.Buffer
			if err := feed(&buf); err != nil {
				return 0, "", err
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.loadCalls++
			if f.loadErr != nil {
				return 0, "", f.loadErr
			}
			size := int64(buf.Len())
			zr, err := gzip.NewReader(&buf)
			if err != nil {
				return 0, "", err
			}
			body, _ := io.ReadAll(zr)
			f.archives[ref] = string(body)
			f.tags = append([]string{ref}, f.tags...)
			return size, "", nil
		},
		tags: func(_ context.Context, repo string) ([]string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			var out []string
			for _, t := range f.tags {
				if repoOf(t) == repo {
					out = append(out, t)
				}
			}
			return out, nil
		},
		arch: func(_ context.Context, ref string) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.arch[ref], nil
		},
		remove: func(_ context.Context, ref string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.inUse[ref] {
				return errors.New("in use")
			}
			f.removed = append(f.removed, ref)
			f.tags = slices.DeleteFunc(f.tags, func(t string) bool { return t == ref })
			return nil
		},
	}
}

// twoMembers is two agents of one cluster behind one relay, each with its own fake docker.
func twoMembers(t *testing.T, src, dst *imageHost) (*Service, *Service, string) {
	t.Helper()
	stubBridge(t, "")
	ca := testpki.New(t)
	mk := func() (*Service, *wire.Identity) {
		dir, err := os.MkdirTemp("", "ag")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		id := ca.Provision(t, dir, "member")
		s := New(t.TempDir(), t.TempDir(), "/", "")
		s.SetAgentDir(dir)
		t.Cleanup(func() { s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}) })
		return s, id
	}
	a, ida := mk()
	b, idb := mk()
	a.imageOps, b.imageOps = src.ops(), dst.ops()
	relay := testrelay.New(t, ca.Leaf(t, "relay"),
		wire.Party{Fingerprint: ida.Fingerprint, Role: wire.RoleMember},
		wire.Party{Fingerprint: idb.Fingerprint, Role: wire.RoleMember})
	for _, s := range []*Service{a, b} {
		if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{relay.URL}}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "both members to reach the relay", func() bool { return relay.Connected() == 2 })
	sourcePrint[a] = ida.Fingerprint
	t.Cleanup(func() { delete(sourcePrint, a) })
	return a, b, idb.Fingerprint
}

// sourcePrint is each sending agent's certificate, for the entry the target is handed.
var sourcePrint = map[*Service]string{}

// expect is Deplo telling the target which image the source is about to send.
func expect(t *testing.T, target, source *Service, ref string, keep int32) {
	t.Helper()
	if _, err := target.ExpectClusterImage(context.Background(), &pb.ExpectClusterImageRequest{ImageRef: ref, SourceFingerprint: sourcePrint[source], Keep: keep}); err != nil {
		t.Fatal(err)
	}
}

func code(err error) codes.Code { return status.Code(err) }

func TestCluster_advertisesImageCopies(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.images") {
		t.Fatal("Hello does not advertise cluster.images")
	}
}

func TestCopyClusterImage_sendsTheArchiveToTheMemberOverTheRelay(t *testing.T) {
	src, dst := newImageHost("deplo/web:dpl_aa"), newImageHost()
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_aa", 0)
	resp, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: target})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAlreadyPresent() || resp.GetBytes() == 0 {
		t.Fatalf("answer %+v", resp)
	}
	if dst.archives["deplo/web:dpl_aa"] != "ARCHIVE of deplo/web:dpl_aa" {
		t.Fatalf("the member loaded %q", dst.archives)
	}
}

func TestCopyClusterImage_aMemberThatHoldsItGetsNoTransfer(t *testing.T) {
	src, dst := newImageHost("deplo/web:dpl_aa"), newImageHost("deplo/web:dpl_aa")
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_aa", 0)
	resp, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: target})
	if err != nil || !resp.GetAlreadyPresent() {
		t.Fatalf("answer %+v %v", resp, err)
	}
	if len(src.saved) != 0 || dst.loadCalls != 0 {
		t.Fatalf("bytes moved: saved %v, loads %d", src.saved, dst.loadCalls)
	}
}

func TestCopyClusterImage_theMemberKeepsTwoReleasesAndReportsWhatItPruned(t *testing.T) {
	src := newImageHost("deplo/web:dpl_cc")
	dst := newImageHost("deplo/web:dpl_bb", "deplo/web:dpl_aa", "deplo/other:dpl_zz", "postgres:16")
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_cc", 2)
	resp, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_cc", TargetFingerprint: target, Keep: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(resp.GetPruned(), []string{"deplo/web:dpl_aa"}) {
		t.Fatalf("pruned %v", resp.GetPruned())
	}
	want := []string{"deplo/web:dpl_cc", "deplo/web:dpl_bb", "deplo/other:dpl_zz", "postgres:16"}
	if !slices.Equal(dst.tags, want) {
		t.Fatalf("the member holds %v, want %v", dst.tags, want)
	}
}

func TestCopyClusterImage_aReleaseInUseStaysAndIsNotReported(t *testing.T) {
	src := newImageHost("deplo/web:dpl_cc")
	dst := newImageHost("deplo/web:dpl_bb", "deplo/web:dpl_aa")
	dst.inUse["deplo/web:dpl_aa"] = true
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_cc", 1)
	resp, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_cc", TargetFingerprint: target, Keep: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(resp.GetPruned(), []string{"deplo/web:dpl_bb"}) || !slices.Contains(dst.tags, "deplo/web:dpl_aa") {
		t.Fatalf("pruned %v, holds %v", resp.GetPruned(), dst.tags)
	}
}

func TestCopyClusterImage_anOlderReleaseCopiedLateIsNotPruned(t *testing.T) {
	src := newImageHost("deplo/web:dpl_aa")
	dst := newImageHost("deplo/web:dpl_cc", "deplo/web:dpl_bb")
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_aa", 0)
	resp, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: target})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(dst.tags, "deplo/web:dpl_aa") || len(resp.GetPruned()) != 1 {
		t.Fatalf("pruned %v, holds %v", resp.GetPruned(), dst.tags)
	}
}

func TestCopyClusterImage_aFailedLoadIsAnError(t *testing.T) {
	src, dst := newImageHost("deplo/web:dpl_aa"), newImageHost()
	dst.loadErr = errors.New("no space left")
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_aa", 0)
	_, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: target})
	if code(err) != codes.Aborted || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("got %v", err)
	}
}

func TestCopyClusterImage_aMemberRefusesAnImageBuiltForAnotherArchitecture(t *testing.T) {
	other := "arm64"
	if runtime.GOARCH == other {
		other = "amd64"
	}
	src := newImageHost("deplo/web:dpl_cc")
	dst := newImageHost("deplo/web:dpl_bb", "deplo/web:dpl_aa")
	dst.arch["deplo/web:dpl_cc"] = other
	a, b, target := twoMembers(t, src, dst)
	expect(t, b, a, "deplo/web:dpl_cc", 2)
	_, err := a.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_cc", TargetFingerprint: target, Keep: 2})
	if code(err) != codes.Aborted || !strings.Contains(err.Error(), "built for "+other) {
		t.Fatalf("got %v", err)
	}
	// It is gone again, and the releases the member runs were not pruned to make room for it.
	if want := []string{"deplo/web:dpl_bb", "deplo/web:dpl_aa"}; !slices.Equal(dst.tags, want) {
		t.Fatalf("the member holds %v, want %v", dst.tags, want)
	}
}

func TestCopyClusterImage_aMemberTakesNoImageItWasNotToldToExpect(t *testing.T) {
	src := newImageHost("deplo/web:dpl_cc", "deplo/other:dpl_zz")
	dst := newImageHost("deplo/web:dpl_bb", "deplo/web:dpl_aa")
	a, b, target := twoMembers(t, src, dst)
	ctx := context.Background()
	copyOf := func(ref string, keep int32) error {
		_, err := a.CopyClusterImage(ctx, &pb.CopyClusterImageRequest{ImageRef: ref, TargetFingerprint: target, Keep: keep})
		return err
	}
	if err := copyOf("deplo/web:dpl_cc", 1); code(err) != codes.FailedPrecondition {
		t.Fatalf("an image nobody announced: %v", err)
	}
	// Told of another image, and of this one from another certificate: neither opens the door.
	expect(t, b, a, "deplo/other:dpl_zz", 0)
	if _, err := b.ExpectClusterImage(ctx, &pb.ExpectClusterImageRequest{ImageRef: "deplo/web:dpl_cc", SourceFingerprint: strings.Repeat("9", 64)}); err != nil {
		t.Fatal(err)
	}
	if err := copyOf("deplo/web:dpl_cc", 1); code(err) != codes.FailedPrecondition {
		t.Fatalf("an image announced from another member: %v", err)
	}
	if dst.loadCalls != 0 || len(dst.removed) != 0 {
		t.Fatalf("a refused image still changed the member: %d loads, removed %v", dst.loadCalls, dst.removed)
	}
	// The entry says how many releases stay, not the sender; and it opens the door once.
	expect(t, b, a, "deplo/web:dpl_cc", 3)
	if err := copyOf("deplo/web:dpl_cc", 1); err != nil {
		t.Fatal(err)
	}
	if len(dst.removed) != 0 {
		t.Fatalf("the sender's keep of 1 pruned %v", dst.removed)
	}
	dst.mu.Lock()
	dst.tags = slices.DeleteFunc(dst.tags, func(t string) bool { return t == "deplo/web:dpl_cc" })
	dst.mu.Unlock()
	if err := copyOf("deplo/web:dpl_cc", 1); code(err) != codes.FailedPrecondition {
		t.Fatalf("the same entry was used twice: %v", err)
	}
}

func TestExpectClusterImage_refusals(t *testing.T) {
	_, b, _ := twoMembers(t, newImageHost(), newImageHost())
	fp := strings.Repeat("a", 64)
	for name, req := range map[string]*pb.ExpectClusterImageRequest{
		"an image outside deplo/":   {ImageRef: "postgres:16", SourceFingerprint: fp},
		"the forwarder's image":     {ImageRef: fwdImage, SourceFingerprint: fp},
		"a source that is no print": {ImageRef: "deplo/web:dpl_aa", SourceFingerprint: "nope"},
		"a keep beyond the limit":   {ImageRef: "deplo/web:dpl_aa", SourceFingerprint: fp, Keep: 99},
	} {
		if _, err := b.ExpectClusterImage(context.Background(), req); code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v", name, err)
		}
	}
	lone := New(t.TempDir(), t.TempDir(), "/", "")
	if _, err := lone.ExpectClusterImage(context.Background(), &pb.ExpectClusterImageRequest{ImageRef: "deplo/web:dpl_aa", SourceFingerprint: fp}); code(err) != codes.FailedPrecondition {
		t.Fatalf("an agent that is no member: %v", err)
	}
}

func TestCopyClusterImage_refusals(t *testing.T) {
	src, dst := newImageHost("deplo/web:dpl_aa"), newImageHost()
	a, _, target := twoMembers(t, src, dst)
	ctx := context.Background()
	cases := map[string]struct {
		req  *pb.CopyClusterImageRequest
		want codes.Code
	}{
		"an image outside deplo/":   {&pb.CopyClusterImageRequest{ImageRef: "postgres:16", TargetFingerprint: target}, codes.InvalidArgument},
		"a flag as the ref":         {&pb.CopyClusterImageRequest{ImageRef: "--all", TargetFingerprint: target}, codes.InvalidArgument},
		"a target that is no print": {&pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: "nope"}, codes.InvalidArgument},
		"a keep beyond the limit":   {&pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: target, Keep: 99}, codes.InvalidArgument},
		"an image this host lacks":  {&pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_zz", TargetFingerprint: target}, codes.FailedPrecondition},
		"a member the relay lacks":  {&pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: strings.Repeat("a", 64)}, codes.Unavailable},
	}
	for name, c := range cases {
		if _, err := a.CopyClusterImage(ctx, c.req); code(err) != c.want {
			t.Errorf("%s: got %v, want %v", name, err, c.want)
		}
	}
	if len(src.saved) != 0 {
		t.Fatalf("a refused copy still saved %v", src.saved)
	}
}

func TestCopyClusterImage_anAgentThatIsNoMemberRefuses(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.imageOps = newImageHost("deplo/web:dpl_aa").ops()
	_, err := s.CopyClusterImage(context.Background(), &pb.CopyClusterImageRequest{ImageRef: "deplo/web:dpl_aa", TargetFingerprint: strings.Repeat("a", 64)})
	if code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v", err)
	}
}

// asked drives acceptMemberStream by hand and returns the target's first answer line.
func asked(t *testing.T, from wire.Party, line string, dst *imageHost) string {
	t.Helper()
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.imageOps = dst.ops()
	a, b := net.Pipe()
	go s.acceptMemberStream(from, b)
	_ = a.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(a, line+"\n"); err != nil {
		return ""
	}
	verb, arg, err := wire.ReadHeader(a)
	a.Close()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(verb + " " + arg)
}

func TestAcceptMemberStream_servesOnlyMembersAndOnlyAnnouncedImages(t *testing.T) {
	member := wire.Party{Fingerprint: strings.Repeat("a", 64), Role: wire.RoleMember}
	leader := wire.Party{Fingerprint: strings.Repeat("a", 64), Role: wire.RoleLeader}
	if got := asked(t, member, "IMAGE deplo/web:dpl_aa 2", newImageHost()); !strings.HasPrefix(got, "ERR") {
		t.Fatalf("an image nobody announced was answered %q", got)
	}
	if got := asked(t, leader, "IMAGE deplo/web:dpl_aa 2", newImageHost()); got != "" {
		t.Fatalf("a leader was answered %q", got)
	}
	for _, bad := range []string{"IMAGE postgres:16 2", "IMAGE deplo/web:dpl_aa 0", "IMAGE deplo/web:dpl_aa 99", "IMAGE deplo/web:dpl_aa", "IMAGE --all 2"} {
		if got := asked(t, member, bad, newImageHost()); !strings.HasPrefix(got, "ERR") {
			t.Errorf("%q was answered %q", bad, got)
		}
	}
	if got := asked(t, member, "SOMETHING else", newImageHost()); got != "" {
		t.Fatalf("an unknown verb was answered %q", got)
	}
}
