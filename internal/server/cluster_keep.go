package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// What a member that comes back still holds of a database or a Volume App that moved is archived to the
// workload's destination before its volume goes; a backup that fails leaves the volume (spec #176).

// keepGrace is how long a moved stack that holds data waits, stopped, for the leader to say where it goes.
const keepGrace = 30 * time.Second

// leftTimeout bounds one archive and removal: a volume can take far longer than the rest of a return.
const leftTimeout = 2 * time.Hour

// Three tries in a row; what still fails stays on this member and is tried again later (retryKept).
var (
	leftTries = 3
	leftWait  = 10 * time.Second
)

// noteKeeps takes what an order says about a return under way, and about what stayed here past one; the
// caller holds g.mu.
func (g *leaseGuard) noteKeeps(o lease.Order, next *clusterLease) {
	if next.Returning {
		if next.since.IsZero() {
			next.since = g.now()
		}
		next.asked = next.asked || o.Return
	} else {
		next.since, next.asked = time.Time{}, false
	}
	if len(o.Keep) == 0 {
		if !next.Returning && o.Start == nil {
			delete(g.keeps, o.Cluster)
		}
		return
	}
	by := map[string]lease.Keep{}
	for _, k := range o.Keep {
		by[k.Slug] = k
	}
	g.keeps[o.Cluster] = by
}

// savedBefore is the archives an earlier pass of this return wrote, by stack.
func (c *clusterLease) savedBefore() map[string]*lease.Saved {
	out := map[string]*lease.Saved{}
	if c.Returned != nil {
		for i := range c.Returned.Saved {
			out[c.Returned.Saved[i].Slug] = &c.Returned.Saved[i]
		}
	}
	return out
}

// movedAway says a stack is still placed on another member and nothing is starting it here.
func (g *leaseGuard) movedAway(cluster, slug string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if _, starting := g.starting[slug]; c == nil || starting {
		return false
	}
	elsewhere := false
	for _, e := range c.Placement {
		if e.Slug != slug {
			continue
		}
		if e.Member == c.Member {
			return false
		}
		elsewhere = elsewhere || e.Member != ""
	}
	return elsewhere
}

// keepMoved decides what happens to the data of one moved stack. gone: the stack went with its volumes;
// retry: the archive is written and only the removal is left, for the next pass.
func (g *leaseGuard) keepMoved(cluster, me string, e lease.Entry, k lease.Keep, already *lease.Saved) (saved *lease.Saved, kept *lease.Kept, gone, retry bool) {
	switch {
	case len(k.Request) == 0 && already != nil:
		return nil, nil, false, false
	case len(k.Request) == 0:
		return nil, &lease.Kept{Slug: e.Slug, Ref: k.Ref, Reason: lease.KeptNoDestination}, false, false
	case storeOutOfReach(k, me):
		return nil, &lease.Kept{Slug: e.Slug, Ref: k.Ref, Reason: lease.KeptStoreElsewhere}, false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), leftTimeout)
	defer cancel()
	still := func() bool { return g.movedAway(cluster, e.Slug) }
	if saved = already; saved == nil {
		sv, err := g.keep(ctx, e, k, me, still)
		switch {
		case errors.Is(err, errRunsAgain):
			// Placed here again meanwhile: the next pass reads the placement anew and leaves it alone.
			return nil, nil, false, true
		case errors.Is(err, errNothingHeld):
			return nil, nil, false, false
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not save what %s still held here, so its volume stays: %v\n", cluster, e.Slug, err)
			return nil, &lease.Kept{Slug: e.Slug, Ref: k.Ref, Reason: lease.KeptBackupFailed, Error: err.Error()}, false, false
		}
		// Written down before the volume goes: an agent that stops here still tells the leader where the data is.
		g.noteSaved(cluster, *sv)
		saved = sv
	}
	if err := g.purge(ctx, e, k, still); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: %s is saved, but its copy could not be removed: %v\n", cluster, e.Slug, err)
		return saved, nil, false, true
	}
	return saved, nil, true, false
}

func (g *leaseGuard) noteSaved(cluster string, sv lease.Saved) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil {
		return
	}
	if c.Returned == nil {
		c.Returned = &lease.Returned{Removed: []string{}}
	}
	noteReturned(c.Returned, []lease.Saved{sv}, nil)
	g.save()
}

// noteReturned adds what one pass saved and kept: a later word on a stack replaces an earlier one, and a
// stack that was saved is not also kept.
func noteReturned(r *lease.Returned, saved []lease.Saved, kept []lease.Kept) {
	for _, sv := range saved {
		r.Saved = append(slices.DeleteFunc(r.Saved, func(o lease.Saved) bool { return o.Slug == sv.Slug }), sv)
	}
	for _, k := range kept {
		r.Kept = append(slices.DeleteFunc(r.Kept, func(o lease.Kept) bool { return o.Slug == k.Slug }), k)
	}
	r.Kept = slices.DeleteFunc(r.Kept, func(k lease.Kept) bool {
		return slices.ContainsFunc(r.Saved, func(sv lease.Saved) bool { return sv.Slug == k.Slug })
	})
}

// memberName is a member's id as it goes into an archive's name.
var memberName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// storeOutOfReach is a destination on the disk of a server this member was given no certificate for.
func storeOutOfReach(k lease.Keep, me string) bool {
	return k.StoreMember != "" && k.StoreMember != me && k.StoreFingerprint == ""
}

// runLeftBackup writes one archive the way the Backup RPC does, a streamed one into out; a var so a
// test can answer without Docker.
var runLeftBackup func(s *Service, ctx context.Context, req *pb.BackupRequest, out io.Writer) (*pb.BackupResult, error)

// Set here, not in the declaration, like the failover restore: the backup path reaches the lease guard.
func init() { runLeftBackup = backupLikeTheRPC }

func backupLikeTheRPC(s *Service, ctx context.Context, req *pb.BackupRequest, out io.Writer) (*pb.BackupResult, error) {
	var res *pb.BackupResult
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		if d := ev.GetData(); len(d) > 0 && out != nil {
			_, err := out.Write(d)
			return err
		}
		return nil
	}}
	dest, err := destinationFromBackup(s, req, e.data)
	if err != nil {
		return nil, errors.New(statusMessage(err))
	}
	s.backupProject(ctx, req.GetProject(), dest, e)
	switch {
	case res == nil:
		return nil, errors.New("the backup ended without a result")
	case !res.GetOk():
		return nil, errors.New(res.GetError())
	}
	return res, nil
}

// readKeep reads a keep's request: an archive of this stack's own volumes and nothing else.
func readKeep(e lease.Entry, k lease.Keep) (*pb.BackupRequest, error) {
	req := &pb.BackupRequest{}
	if err := bundleJSON.Unmarshal(k.Request, req); err != nil {
		return nil, fmt.Errorf("the keep is unreadable: %w", err)
	}
	p := req.GetProject()
	if validateSlug(e.Slug) != nil || req.GetKind() != pb.BackupKind_BACKUP_KIND_PROJECT || p == nil || p.GetSlug() != e.Slug || req.GetStreamOut() {
		return nil, errors.New("the keep is not an archive of this stack's volumes")
	}
	if len(p.GetVolumeNames()) == 0 {
		return nil, errors.New("the keep names no volume")
	}
	for _, v := range p.GetVolumeNames() {
		if !strings.HasPrefix(v, deploVolumePrefix) || validateVolumeName(v) != nil {
			return nil, fmt.Errorf("%q is not a volume Deplo made", v)
		}
	}
	return req, nil
}

// errRunsAgain is a stack that was placed here again, or is being started here, while its old data waited.
var errRunsAgain = errors.New("this server runs it again")

// errNothingHeld is a keep whose volumes are not on this server any more.
var errNothingHeld = errors.New("this server holds none of its volumes")

// holdsVolume says whether one of the volumes is on this host. Only docker's own "no such volume" counts as
// absent: reading a name it does not know would make an empty volume, and an archive of nothing.
var holdsVolume = func(ctx context.Context, names []string) bool {
	for _, name := range names {
		res, err := dockercli.Run(ctx, 20*time.Second, "volume", "inspect", "--format", "{{.Name}}", name)
		if err != nil || res.Code == 0 || !strings.Contains(strings.ToLower(res.Stderr), "no such volume") {
			return true
		}
	}
	return false
}

// keepLeft archives the volumes of a stack that runs elsewhere now, under the stem the control plane
// gave, then the failover's time and this member: a repeat writes over the first. Nothing is removed.
func (s *Service) keepLeft(ctx context.Context, e lease.Entry, k lease.Keep, member string, still func() bool) (*lease.Saved, error) {
	req, err := readKeep(e, k)
	if err != nil {
		return nil, err
	}
	if !memberName.MatchString(member) {
		return nil, errors.New("the keep names no usable member")
	}
	at, perr := time.Parse(time.RFC3339, k.FailedOverAt)
	if perr != nil {
		// A leader that does not say when: the old name, by the clock.
		at = time.Now()
	}
	name := lease.LeftName(at, member, req.GetAgeRecipient() != "")
	// Elsewhere is the disk of another member of this cluster: the archive goes to it over the relay.
	elsewhere := k.StoreMember != "" && k.StoreMember != member
	switch {
	case req.GetS3() != nil && req.GetS3().GetObjectKey() != "":
		req.S3.ObjectKey += name
		elsewhere = false
	case req.GetStore() != nil && req.GetStore().GetObjectKey() != "" && !storeOutOfReach(k, member):
		req.Store.ObjectKey += name
	default:
		return nil, errors.New("the keep names no destination this server can write to")
	}
	if !still() {
		return nil, errRunsAgain
	}
	if !holdsVolume(ctx, req.GetProject().GetVolumeNames()) {
		return nil, errNothingHeld
	}
	if _, err := s.StopStack(ctx, &pb.StackRef{Slug: e.Slug}); err != nil {
		return nil, err
	}
	defer s.lockStack(e.Slug)()
	// Asked again under the lock: a start that got in first made this volume the live one.
	if !still() {
		return nil, errRunsAgain
	}
	for try := 1; ; try++ {
		var res *pb.BackupResult
		if elsewhere {
			res, err = s.sendLeft(ctx, req, k.StoreFingerprint)
		} else {
			res, err = runLeftBackup(s, ctx, req, nil)
		}
		if err == nil {
			return &lease.Saved{Slug: e.Slug, Ref: k.Ref, ObjectKey: req.GetS3().GetObjectKey() + req.GetStore().GetObjectKey(), SizeBytes: res.GetSizeBytes(),
				DecryptedSizeBytes: res.GetDecryptedSizeBytes(), Sha256: res.GetSha256(), At: time.Now().UTC().Format(time.RFC3339)}, nil
		}
		if try >= leftTries || ctx.Err() != nil {
			return nil, err
		}
		time.Sleep(leftWait)
	}
}

// purgeLeft takes a saved stack down with the volumes its archive holds.
func (s *Service) purgeLeft(ctx context.Context, e lease.Entry, k lease.Keep, still func() bool) error {
	req, err := readKeep(e, k)
	if err != nil {
		return err
	}
	defer s.lockStack(e.Slug)()
	if !still() {
		return errRunsAgain
	}
	res, err := s.destroyStack(ctx, &pb.StackRef{Slug: e.Slug, RemoveVolumes: true, ReclaimVolumes: req.GetProject().GetVolumeNames()})
	if err == nil && !res.GetOk() {
		err = errors.New(res.GetError())
	}
	if err == nil && res.GetError() != "" {
		// ponytail: a volume something else still mounts stays; nothing looks at it again.
		fmt.Fprintf(os.Stderr, "deplo-agent: %s is saved and stopped, but %s\n", e.Slug, res.GetError())
	}
	return err
}

// stopMoved stops a stack that waits for the leader's word on its data.
func (s *Service) stopMoved(ctx context.Context, slug string) error {
	res, err := s.StopStack(ctx, &pb.StackRef{Slug: slug})
	if err == nil && !res.GetOk() {
		err = errors.New(res.GetError())
	}
	return err
}
