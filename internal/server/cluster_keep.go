package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// What a member that comes back still holds of a database or a Volume App that moved is archived to the
// workload's destination before its volume goes; a backup that fails leaves the volume (spec #176).

// keepGrace is how long a moved stack that holds data waits, stopped, for the leader to say where it goes.
const keepGrace = 30 * time.Second

// leftBehindTimeout bounds one archive and removal: a volume can take far longer than the rest of a return.
const leftBehindTimeout = 2 * time.Hour

// ponytail: three tries, then the volume stays for good; a later sweep could try a kept one again.
var (
	leftBehindTries = 3
	leftBehindWait  = 10 * time.Second
)

// noteKeeps takes what an order says about a return under way; the caller holds g.mu.
func (g *leaseGuard) noteKeeps(o lease.Order, next *clusterLease) {
	if !next.Returning {
		next.since, next.asked = time.Time{}, false
		delete(g.keeps, o.Cluster)
		return
	}
	if next.since.IsZero() {
		next.since = g.now()
	}
	next.asked = next.asked || o.Return
	if len(o.Keep) == 0 {
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
	case k.StoreMember != "" && k.StoreMember != me:
		return nil, &lease.Kept{Slug: e.Slug, Ref: k.Ref, Reason: lease.KeptStoreElsewhere}, false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), leftBehindTimeout)
	defer cancel()
	sv, err := g.keep(ctx, e, k, me, func() bool { return g.movedAway(cluster, e.Slug) }, already)
	switch {
	case err == nil:
		return sv, nil, true, false
	case sv != nil:
		fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: %s is saved, but its copy could not be removed: %v\n", cluster, e.Slug, err)
		return sv, nil, false, true
	}
	fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not save what %s still held here, so its volume stays: %v\n", cluster, e.Slug, err)
	return nil, &lease.Kept{Slug: e.Slug, Ref: k.Ref, Reason: lease.KeptBackupFailed, Error: err.Error()}, false, false
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

// runLeftBehindBackup writes one archive the way the Backup RPC does; a var so a test can answer without Docker.
var runLeftBehindBackup func(s *Service, ctx context.Context, req *pb.BackupRequest) (*pb.BackupResult, error)

// Set here, not in the declaration, like the failover restore: the backup path reaches the lease guard.
func init() { runLeftBehindBackup = backupLikeTheRPC }

func backupLikeTheRPC(s *Service, ctx context.Context, req *pb.BackupRequest) (*pb.BackupResult, error) {
	var res *pb.BackupResult
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
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

// leftBehindRequest reads a keep and names the archive: the stem the control plane gave, the time, this member.
func leftBehindRequest(e lease.Entry, k lease.Keep, member string, at time.Time) (*pb.BackupRequest, error) {
	req := &pb.BackupRequest{}
	if err := bundleJSON.Unmarshal(k.Request, req); err != nil {
		return nil, fmt.Errorf("the keep is unreadable: %w", err)
	}
	p := req.GetProject()
	if req.GetKind() != pb.BackupKind_BACKUP_KIND_PROJECT || p == nil || p.GetSlug() != e.Slug || req.GetStreamOut() {
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
	name := "-" + at.UTC().Format("20060102T150405Z") + "-" + member + ".tar.gz"
	if req.GetAgeRecipient() != "" {
		name += ".age"
	}
	switch {
	case req.GetS3() != nil && req.GetS3().GetObjectKey() != "":
		req.S3.ObjectKey += name
	case req.GetStore() != nil && req.GetStore().GetObjectKey() != "":
		req.Store.ObjectKey += name
	default:
		return nil, errors.New("the keep names no destination")
	}
	return req, nil
}

// keepLeftBehind archives the volumes of a stack that runs elsewhere now, then takes the stack down with them.
// The stack lock is held throughout, so a start of it here waits and then begins on an empty volume.
func (s *Service) keepLeftBehind(ctx context.Context, e lease.Entry, k lease.Keep, member string, still func() bool, already *lease.Saved) (*lease.Saved, error) {
	if validateSlug(e.Slug) != nil || !memberName.MatchString(member) {
		return nil, errors.New("the keep names no usable stack or member")
	}
	req, err := leftBehindRequest(e, k, member, time.Now())
	if err != nil {
		return nil, err
	}
	again := errors.New("this server runs it again")
	if !still() {
		return nil, again
	}
	if _, err := s.StopStack(ctx, &pb.StackRef{Slug: e.Slug}); err != nil {
		return nil, err
	}
	defer s.lockStack(e.Slug)()
	// Asked again under the lock: a start that got in first made this volume the live one.
	if !still() {
		return nil, again
	}
	saved := already
	for try := 0; saved == nil; try++ {
		res, berr := runLeftBehindBackup(s, ctx, req)
		if berr == nil {
			key := req.GetS3().GetObjectKey() + req.GetStore().GetObjectKey()
			saved = &lease.Saved{Slug: e.Slug, Ref: k.Ref, ObjectKey: key, SizeBytes: res.GetSizeBytes(),
				DecryptedSizeBytes: res.GetDecryptedSizeBytes(), Sha256: res.GetSha256(), At: time.Now().UTC().Format(time.RFC3339)}
			break
		}
		if try+1 >= leftBehindTries || ctx.Err() != nil {
			return nil, berr
		}
		time.Sleep(leftBehindWait)
	}
	if !still() {
		return saved, again
	}
	res, err := s.destroyStack(ctx, &pb.StackRef{Slug: e.Slug, RemoveVolumes: true, ReclaimVolumes: req.GetProject().GetVolumeNames()})
	if err == nil && !res.GetOk() {
		err = errors.New(res.GetError())
	}
	if err == nil && res.GetError() != "" {
		// ponytail: a volume something else still mounts stays; nothing looks at it again.
		fmt.Fprintf(os.Stderr, "deplo-agent: %s is saved and stopped, but %s\n", e.Slug, res.GetError())
	}
	return saved, err
}

// stopMoved stops a stack that waits for the leader's word on its data.
func (s *Service) stopMoved(ctx context.Context, slug string) error {
	res, err := s.StopStack(ctx, &pb.StackRef{Slug: slug})
	if err == nil && !res.GetOk() {
		err = errors.New(res.GetError())
	}
	return err
}
