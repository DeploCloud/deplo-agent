package server

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// holdBudget is the longest a backup keeps its App paused (deplo c44).
var holdBudget = time.Second

// The panel matches this text word for word to skip, not fail, a live App with no data yet.
var errNoVolumeFiles = errors.New("the App's data holds no file on this server, so its archive is not a backup")

func (s *Service) backupProject(ctx context.Context, p *pb.ProjectDescriptor, dest *artifactDestination, e *bkEmitter) {
	if p == nil || p.GetSlug() == "" {
		e.result(false, "project backup request missing descriptor / slug", "", 0)
		return
	}
	if err := validateSlug(p.GetSlug()); err != nil {
		e.result(false, err.Error(), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Backing up project %q (%d volume(s), files=%v)", p.GetSlug(), len(p.GetVolumeNames()), p.GetIncludeFiles()))

	produce := func(w io.Writer) error {
		tw := tar.NewWriter(w)
		err := s.writeProjectArchive(ctx, p, tw, e, dest.store != nil)
		// c43: a stopped copy on a stale server archived its empty volume, kept as a 398 B backup.
		if err == nil && e.volumeFiles == 0 && slices.ContainsFunc(p.GetVolumeNames(), func(v string) bool { return v != "" }) {
			err = errNoVolumeFiles
		}
		if cerr := tw.Close(); err == nil {
			err = cerr
		}
		return err
	}

	written, werr := s.writeArtifact(ctx, dest, produce)
	if werr != nil {
		e.result(false, statusMessage(werr), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Wrote %s (%d bytes)", dest.label, written.size))
	e.emitResult(true, "", dest.key, written)
}

// archiveHere says the archive is written on this server's disk too.
func (s *Service) writeProjectArchive(ctx context.Context, p *pb.ProjectDescriptor, tw *tar.Writer, e *bkEmitter, archiveHere bool) error {
	copier := startCopier(ctx, p.GetVolumeNames(), s.newStage())
	defer copier.remove()
	skip := ""
	if copier != nil {
		skip = copier.id
		select {
		case <-ctx.Done():
		case <-time.After(copierSettle):
		}
	}
	// Copied once while the App runs: the pause then lasts only for what changed meanwhile.
	why := copier.roomToStage(ctx, archiveHere)
	first, copied := int64(0), false
	since := time.Now().Add(-2 * time.Second)
	if running, _ := runningOn(ctx, p.GetVolumeNames(), skip); why == "" && len(running) > 0 {
		if n, err := copier.copyToStage(ctx, time.Time{}); err != nil {
			why = "copying the data on the server's own disk failed: " + err.Error()
		} else {
			first, copied = n, true
		}
	}
	if copied {
		copier.readyPass(ctx)
	}
	hold, herr := s.holdVolumes(ctx, p.GetSlug(), p.GetVolumeNames(), func(text string) { e.log("info", text) }, skip)
	if herr != nil {
		return herr
	}
	// c44: fise was paused 4.1 s and lab-kit-a 21.5 s for a whole copy. Past holdBudget the App runs again, and the
	// copy made while it was paused is no backup.
	heldCtx, cancelHeld := context.WithCancel(ctx)
	defer cancelHeld()
	var overran atomic.Bool
	var watchdog *time.Timer
	if hold != nil {
		e.holding = true
		e.log("info", "Paused the app while its data is copied")
		watchdog = time.AfterFunc(holdBudget-time.Since(hold.start), func() {
			overran.Store(true)
			cancelHeld()
			hold.release()
		})
	}
	overBudget := func() error {
		msg := fmt.Sprintf("its data could not be copied within %s of pausing it, so it runs again and this backup was not kept", spoken(holdBudget))
		if why != "" {
			msg += ": " + why
		}
		return errors.New(msg)
	}
	// Also on a panic: an App left paused stays so until the agent restarts.
	ended := false
	end := func() {
		if !ended {
			ended = true
			if watchdog != nil {
				watchdog.Stop()
			}
			s.endHold(hold, e)
			if copier == nil || !copier.staged {
				copier.remove()
			}
		}
	}
	defer end()
	if hold != nil && why == "" && !copied {
		why = "it started while its data was being copied, so nothing was copied on the server's own disk first"
	}
	if hold != nil && why == "" {
		copyPaused := func() (int64, error) { return copier.copyToStage(heldCtx, since) }
		if copier.told {
			copyPaused = func() (int64, error) {
				files, n, took, err := copier.tellPass(heldCtx, since)
				if err == nil {
					e.log("info", fmt.Sprintf("Copied what changed while it was paused: %d files (%s) in %s", files, humanBytes(n), took.Round(time.Millisecond)))
				}
				return n, err
			}
		}
		if n, err := copyPaused(); overran.Load() {
			return overBudget()
		} else if err != nil {
			why = "copying the data on the server's own disk failed: " + err.Error()
		} else {
			copier.staged = true
			end()
			e.staged = true
			if first > 0 && n >= first {
				e.heldWhy = fmt.Sprintf("its data changed as fast as it was copied: the copy made while it was paused (%s) was no smaller than the first", humanBytes(n))
				e.log("warn", "The second copy was not smaller than the first: "+e.heldWhy)
			}
		}
	}
	if hold != nil && why != "" {
		e.heldWhy = why
		e.log("warn", "The app stays paused for the whole copy: "+why)
	}
	for _, vol := range p.GetVolumeNames() {
		if vol == "" {
			continue
		}
		e.log("info", "Archiving volume "+vol)
		err := s.archiveVolume(heldCtx, copier.tarArgs(heldCtx, vol), vol, tw, e)
		if overran.Load() {
			return overBudget()
		}
		if err != nil {
			return fmt.Errorf("archive volume %q: %w", vol, err)
		}
	}
	end()
	if overran.Load() {
		return overBudget()
	}

	if p.GetIncludeFiles() {
		root := s.filesRoot(p.GetSlug())
		if st, err := os.Stat(root); err == nil && st.IsDir() {
			e.log("info", "Archiving project files")
			if err := addDirToTar(tw, root, "files"); err != nil {
				return fmt.Errorf("archive files: %w", err)
			}
		}
	}

	if y := p.GetComposeYaml(); y != "" {
		if err := addBytesToTar(tw, "snapshot/compose.yml", []byte(y)); err != nil {
			return err
		}
	}
	if len(p.GetEnvSnapshot()) > 0 {
		if err := addBytesToTar(tw, "snapshot/env", []byte(renderEnvFile(p.GetEnvSnapshot()))); err != nil {
			return err
		}
	}
	for _, m := range p.GetMounts() {
		rel, err := normalizeRel(m.GetPath())
		if err != nil || rel == "" {
			e.log("warn", "Skipping unsafe mount path in snapshot: "+m.GetPath())
			continue
		}
		if err := addBytesToTar(tw, "snapshot/mounts/"+rel, []byte(m.GetContent())); err != nil {
			return err
		}
	}
	return nil
}

// endHold resumes the app and records how long it was held.
func (s *Service) endHold(hold *volumeHold, e *bkEmitter) {
	if hold == nil {
		return
	}
	held := hold.release()
	e.mu.Lock()
	e.holding = false
	e.held = held
	e.mu.Unlock()
	e.log("info", fmt.Sprintf("The app was paused for %s and is running again", held.Round(time.Millisecond)))
}

func (s *Service) archiveVolume(ctx context.Context, argv []string, vol string, tw *tar.Writer, e *bkEmitter) error {
	if err := validateVolumeName(vol); err != nil {
		return err
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	// A reader that gives up stops the copy: its helper would otherwise run on, and the App stay paused behind it.
	cctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		code, err := dockercli.PipeOut(cctx, backupStepTimeout, pw, nil, argv...)
		if err == nil && code != 0 {
			err = fmt.Errorf("volume tar exited %d", code)
		}
		pw.CloseWithError(err)
		done <- err
	}()
	tr := tar.NewReader(pr)
	prefix := "volumes/" + vol
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			_, _ = io.Copy(io.Discard, pr)
			if perr := <-done; perr != nil && (e.holding || e.staged) {
				return perr
			} else if perr != nil {
				e.log("warn", fmt.Sprintf("volume %q: %v (archive completed; a file likely changed during read)", vol, perr))
			}
			return nil
		}
		if err != nil {
			_ = pr.CloseWithError(err)
			stop()
			<-done
			return err
		}
		name := strings.TrimPrefix(filepath.ToSlash(hdr.Name), "./")
		hdr.Name = prefix + "/" + name
		if hdr.Typeflag == tar.TypeDir {
			hdr.Name = strings.TrimRight(hdr.Name, "/") + "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = pr.CloseWithError(err)
			stop()
			<-done
			return err
		}
		if hdr.Typeflag == tar.TypeReg {
			e.volumeFiles++
			if _, err := io.Copy(tw, tr); err != nil {
				_ = pr.CloseWithError(err)
				stop()
				<-done
				return err
			}
		}
	}
}

func (s *Service) restoreProject(ctx context.Context, p *pb.ProjectDescriptor, src *artifactSource, e *rsEmitter) {
	if p == nil || p.GetSlug() == "" {
		e.result(false, "project restore request missing descriptor / slug")
		return
	}
	slug := p.GetSlug()
	if err := validateSlug(slug); err != nil {
		e.result(false, err.Error())
		return
	}
	e.log("info", fmt.Sprintf("Restoring project %q from %s", slug, src.label))

	e.log("info", "Stopping the stack")
	if _, err := dockercli.Run(ctx, 90*time.Second, s.composeCtl(slug, "stop")...); err != nil {
		e.log("warn", "stack stop: "+err.Error()+" (continuing)")
	}

	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()

	snapshot, err := s.unpackProjectArchive(ctx, slug, p.GetVolumeNames(), rd, e)
	if err != nil {
		e.result(false, err.Error())
		return
	}

	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}

	if src.dataOnly {
		e.log("info", "Restore complete; the stack starts from its failover bundle")
		e.result(true, "")
		return
	}
	rr := restoreConfig(slug, p, snapshot, src.integrityProven, src.configUntrusted)
	if rr.ComposeYaml == "" {
		e.log("warn", "No compose snapshot in the archive; leaving the stack stopped")
		e.result(true, "")
		return
	}
	e.log("info", "Re-applying the backed-up stack configuration")
	res, rerr := s.Reroute(ctx, rr)
	if rerr != nil {
		e.result(false, "restart stack: "+rerr.Error())
		return
	}
	if !res.GetOk() {
		e.result(false, "restart stack: "+res.GetError())
		return
	}
	e.log("info", "Restore complete; stack restarted")
	e.result(true, "")
}

func restoreConfig(
	slug string,
	p *pb.ProjectDescriptor,
	snap projectSnapshot,
	proven bool,
	untrusted bool,
) *pb.RerouteRequest {
	pick := func(fromArchive, fromControlPlane string) string {
		if proven && fromArchive != "" {
			return fromArchive
		}
		if fromControlPlane != "" {
			return fromControlPlane
		}
		if untrusted {
			return ""
		}
		return fromArchive
	}
	compose := retargetStackNetwork(pick(snap.compose, p.GetComposeYaml()), p.GetNetwork())

	env := p.GetEnvSnapshot()
	if proven && len(snap.env) > 0 {
		env = snap.env
	} else if len(env) == 0 && !untrusted {
		env = snap.env
	}

	mounts := p.GetMounts()
	if proven && len(snap.mounts) > 0 {
		mounts = snap.mounts
	} else if len(mounts) == 0 && !untrusted {
		mounts = snap.mounts
	}

	return &pb.RerouteRequest{
		Slug: slug, ComposeYaml: compose, Env: env, Mounts: mounts,
		Network: p.GetNetwork(),
	}
}

type projectSnapshot struct {
	compose string
	env     map[string]string
	mounts  []*pb.MountFile
}

func readSnapshotEntry(tr io.Reader, name string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(tr, maxSnapshotBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxSnapshotBytes {
		return nil, fmt.Errorf("snapshot entry %q exceeds the %d-byte limit", name, int64(maxSnapshotBytes))
	}
	return b, nil
}

type budgetReader struct {
	r      io.Reader
	n      int64
	budget int64
}

func (b *budgetReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if b.n > b.budget {
		return n, fmt.Errorf("project archive exceeds the %d-byte restore limit", b.budget)
	}
	return n, err
}

func (s *Service) unpackProjectArchive(ctx context.Context, slug string, volumeNames []string, r io.Reader, e *rsEmitter) (projectSnapshot, error) {
	snap := projectSnapshot{env: map[string]string{}}

	for _, vol := range volumeNames {
		if vol == "" {
			continue
		}
		if err := validateVolumeName(vol); err != nil {
			return snap, err
		}
	}

	for _, vol := range volumeNames {
		if vol == "" {
			continue
		}
		e.log("info", "Wiping volume "+vol)
		if err := wipeVolume(ctx, vol); err != nil {
			return snap, fmt.Errorf("wipe volume %q: %w", vol, err)
		}
	}
	filesRoot := s.filesRoot(slug)
	if err := os.RemoveAll(filesRoot); err != nil {
		return snap, fmt.Errorf("wipe files dir: %w", err)
	}

	vstreams := newVolumeStreams(ctx, volumeNames)
	defer vstreams.closeAll()

	tr := tar.NewReader(&budgetReader{r: r, budget: maxProjectRestoreBytes})
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return snap, fmt.Errorf("read archive: %w", err)
		}
		name := filepath.ToSlash(hdr.Name)
		switch {
		case strings.HasPrefix(name, "volumes/"):
			rest := strings.TrimPrefix(name, "volumes/")
			vol, inner, ok := strings.Cut(rest, "/")
			if !ok || inner == "" {
				continue
			}
			w, ok := vstreams.writerFor(vol)
			if !ok {
				continue
			}
			if hasDotDot(inner) {
				return snap, fmt.Errorf("archive volume entry %q contains a path traversal", inner)
			}
			if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeDir {
				continue
			}
			hdr.Name = inner
			if err := w.WriteHeader(hdr); err != nil {
				return snap, fmt.Errorf("write volume entry: %w", err)
			}
			if hdr.Typeflag == tar.TypeReg {
				if _, err := io.Copy(w, tr); err != nil {
					return snap, fmt.Errorf("write volume data: %w", err)
				}
			}
		case strings.HasPrefix(name, "files/"):
			rel := strings.TrimPrefix(name, "files/")
			if err := extractToDir(filesRoot, rel, hdr, tr); err != nil {
				return snap, fmt.Errorf("extract files: %w", err)
			}
		case name == "snapshot/compose.yml":
			b, err := readSnapshotEntry(tr, name)
			if err != nil {
				return snap, err
			}
			snap.compose = string(b)
		case name == "snapshot/env":
			b, err := readSnapshotEntry(tr, name)
			if err != nil {
				return snap, err
			}
			snap.env = parseEnvFile(string(b))
		case strings.HasPrefix(name, "snapshot/mounts/"):
			b, err := readSnapshotEntry(tr, name)
			if err != nil {
				return snap, err
			}
			snap.mounts = append(snap.mounts, &pb.MountFile{
				Path:    strings.TrimPrefix(name, "snapshot/mounts/"),
				Content: string(b),
			})
		}
	}

	if err := vstreams.finish(e); err != nil {
		return snap, err
	}
	return snap, nil
}

func retargetStackNetwork(composeYaml, network string) string {
	if composeYaml == "" || network == "" {
		return composeYaml
	}
	lines := strings.Split(composeYaml, "\n")
	inNetworks := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			inNetworks = trimmed == "networks:"
			continue
		}
		if !inNetworks || !strings.HasPrefix(trimmed, "name:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(trimmed, "name:"))
		value = strings.Trim(value, `"'`)
		if !dockercli.IsTenantNetwork(value) || value == network {
			continue
		}
		indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
		lines[i] = indent + "name: " + network
	}
	return strings.Join(lines, "\n")
}
