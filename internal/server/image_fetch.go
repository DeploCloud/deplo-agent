package server

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/pace"
)

// A member that lacks an image its cluster holds for failovers takes it from a member that has it, over the
// cluster's network; only the first member of the cluster downloads one no member has (c43).
// Stream: "IMAGEGET <ref>", answered HAVE then the gzipped archive, or ERR.

const (
	imageGetVerb = "IMAGEGET"
	imageBusy    = "busy"
)

var (
	errNoMemberHolds = errors.New("no other member holds it")
	// errMemberBusy is a member that holds it sending another image: wait for it, never the registry (c43).
	errMemberBusy = errors.New("a member that holds it is sending another image")
	// errMemberSentBroken is a member's image that would not load here: the registry is next, at once (c44).
	errMemberSentBroken = errors.New("the image a member sent would not load")
	// memberBusyRetry is how often a busy member is asked again, and memberBusyFor for how long in one pass.
	memberBusyRetry = 10 * time.Second
	memberBusyFor   = 15 * time.Minute
	// pullerGrace is how long a member waits for the first member to download an image before it does itself.
	pullerGrace = 10 * time.Minute
	memberAsk   = 10 * time.Second
	// imageServeSlot lets one image leave this member at a time: a second asker tries another member.
	imageServeSlot = make(chan struct{}, 1)
)

// serveImageGet sends a member of this cluster an image this one holds for its failovers, and nothing else.
func (s *Service) serveImageGet(from wire.Party, c net.Conn, ref string) {
	if validateClusterImageRef(ref) != nil || !s.failoverKept()[imageName(ref)] || !s.inMyCluster(from) {
		_ = wire.WriteHeader(c, wire.VerbErr, "not held here")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), imageCopyTimeout)
	defer cancel()
	if held, _ := s.images().has(ctx, ref); !held {
		_ = wire.WriteHeader(c, wire.VerbErr, "not held here")
		return
	}
	select {
	case imageServeSlot <- struct{}{}:
		defer func() { <-imageServeSlot }()
	default:
		_ = wire.WriteHeader(c, wire.VerbErr, imageBusy+" sending another image")
		return
	}
	_ = c.SetDeadline(time.Now().Add(imageCopyTimeout))
	if wire.WriteHeader(c, imageHave, "") != nil {
		return
	}
	gz, _ := gzip.NewWriterLevel(pace.Writer(c), gzip.BestSpeed)
	if err := s.images().save(ctx, ref, gz); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not send %s to another member: %v\n", ref, err)
		return
	}
	_ = gz.Close()
}

func (s *Service) inMyCluster(from wire.Party) bool {
	for _, fp := range s.forwarders().state().Members {
		if from.Is(fp) {
			return true
		}
	}
	return false
}

// clusterPeers is every other member of this server's cluster, in fingerprint order, and whether this one comes first.
func (s *Service) clusterPeers() (peers []string, first bool) {
	self := ""
	if id, err := s.clusterIdentity(); err == nil {
		self = strings.ToLower(id.Fingerprint)
	}
	all := []string{}
	for _, fp := range s.forwarders().state().Members {
		all = append(all, strings.ToLower(fp))
	}
	slices.Sort(all)
	for _, fp := range all {
		if fp != self {
			peers = append(peers, fp)
		}
	}
	return peers, len(peers) == 0 || (self != "" && all[0] == self)
}

// fetchFromMembers takes ref from the first other member that holds it, and says which one sent it.
func (s *Service) fetchFromMembers(ctx context.Context, ref string) (string, error) {
	m := s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	peers, _ := s.clusterPeers()
	if party == nil || len(peers) == 0 {
		return "", errNoMemberHolds
	}
	busy, broken := false, error(nil)
	for _, fp := range peers {
		actx, cancel := context.WithTimeout(ctx, memberAsk)
		conn, err := party.Dial(actx, wire.Party{Fingerprint: fp, Role: wire.RoleMember})
		cancel()
		if err != nil {
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(memberAsk))
		verb, arg := "", ""
		if wire.WriteHeader(conn, imageGetVerb, ref) == nil {
			verb, arg, _ = wire.ReadHeader(conn)
		}
		if verb != imageHave {
			busy = busy || (verb == wire.VerbErr && strings.HasPrefix(arg, imageBusy))
			conn.Close()
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(imageCopyTimeout))
		_, _, err = s.images().load(ctx, imageName(ref), func(w io.Writer) error {
			_, cerr := io.Copy(w, conn)
			return cerr
		})
		conn.Close()
		if err == nil {
			return fp, nil
		}
		fmt.Fprintf(os.Stderr, "deplo-agent: could not take %s from the cluster's member %s: %v\n", ref, fp[:min(12, len(fp))], err)
		broken = fmt.Errorf("%w: %v", errMemberSentBroken, err)
	}
	if broken != nil {
		return "", broken
	}
	if busy {
		return "", errMemberBusy
	}
	return "", errNoMemberHolds
}

// fetchWaiting is fetchFromMembers, asking again while a member that holds the image sends another one.
func (s *Service) fetchWaiting(ctx context.Context, ref string) (string, error) {
	until := time.Now().Add(memberBusyFor)
	for {
		from, err := s.fetchFromMembers(ctx, ref)
		if !errors.Is(err, errMemberBusy) || time.Now().After(until) {
			return from, err
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(memberBusyRetry):
		}
	}
}

// registryPull downloads ref from its registry at registryRate; docker pulls what that cannot read (a digest, an
// index of indexes) at its own speed.
var registryPull = func(ctx context.Context, s *Service, ref string) error {
	if !strings.Contains(ref, "@") && s.images().load != nil {
		_, _, err := s.images().load(ctx, imageName(ref), func(w io.Writer) error { return registryArchive(ctx, ref, w) })
		if err == nil {
			return nil
		}
		fmt.Fprintf(os.Stderr, "deplo-agent: could not download %s at a paced rate, docker pulls it: %v\n", ref, err)
	}
	res, err := dockercli.Run(ctx, failoverPullTimeout, "pull", "--quiet", ref)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("%s", firstLine(res.Stderr))
	}
	return nil
}
