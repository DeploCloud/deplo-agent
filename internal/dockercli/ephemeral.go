package dockercli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"time"
)

const ephemeralKillGrace = 5 * time.Second

// ephemeral tags a `docker run --rm` with a name and a `docker exec` with a marker, because killing
// the docker client stops neither; the returned cleanup removes what a cancel would leave running.
func ephemeral(args []string) ([]string, func()) {
	if len(args) == 0 {
		return args, func() {}
	}
	id := randomID()
	switch {
	case args[0] == "run" && slices.Contains(args, "--rm") && !slices.Contains(args, "--name"):
		name := "deplo-helper-" + id
		return append([]string{"run", "--name", name}, args[1:]...), func() { ForceRemove(name) }
	case args[0] == "exec":
		return append([]string{"exec", "-e", MarkerEnv + "=" + id}, args[1:]...),
			func() { KillMarked(id, ephemeralKillGrace) }
	}
	return args, func() {}
}

// cleanupIfCancelled runs cleanup once the command's context ended before it did.
func cleanupIfCancelled(cctx context.Context, cleanup func()) {
	if cctx.Err() != nil {
		cleanup()
	}
}

// ForceRemove removes a container on a fresh context, since the caller's is usually the one that ended.
func ForceRemove(name string) {
	_, _ = Run(context.Background(), 30*time.Second, "rm", "-f", name)
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
