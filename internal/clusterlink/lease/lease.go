// Package lease is the order a cluster leader sends each member, a contract with deplo-cluster's
// internal/lease: the names, the JSON and the timings change together (spec #176).
package lease

import (
	"bufio"
	"encoding/json"
	"io"
	"time"
)

// FenceAfter is how long a member lives without a renewal before it stops its single-writer workloads.
const FenceAfter = 60 * time.Second

// MaxLine bounds one order or reply.
const MaxLine = 16 << 20

// StartTimeout is how long a member may take to bring a moved workload up.
const StartTimeout = 5 * time.Minute

// RestoreTimeout is how long a start that restores a backup first may take.
const RestoreTimeout = StartTimeout + 30*time.Minute

// StaleEpoch is the reply Error of a member that has seen a newer leader.
const StaleEpoch = "stale_epoch"

// Entry is one replica of a workload and where the leader placed it.
type Entry struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	Replica      int    `json:"replica"`
	Member       string `json:"member"`
	Slug         string `json:"slug"`
	SingleWriter bool   `json:"singleWriter"`
}

// Order is what each renewal carries. Member names the receiver, so it knows which entries are its own.
type Order struct {
	Cluster   string  `json:"cluster"`
	Member    string  `json:"member"`
	Epoch     uint64  `json:"epoch"`
	Placement []Entry `json:"placement"`
	// ExtendSeconds adds to the time this lease lives without a renewal: a leader about to restart sends it.
	ExtendSeconds int `json:"extendSeconds,omitempty"`
	// Start asks this member to bring one moved workload up from its failover bundle.
	Start *Start `json:"start,omitempty"`
}

// RegistryAuth is a registry login a pulled image needs.
type RegistryAuth struct {
	Host     string `json:"host"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// Start is a failover bundle opened for one restart, a contract with deplo-cluster's internal/lease.
type Start struct {
	DeploymentID  string            `json:"deploymentId"`
	Slug          string            `json:"slug"`
	ProjectID     string            `json:"projectId"`
	ImageRef      string            `json:"imageRef"`
	ComposeYAML   string            `json:"composeYaml"`
	Network       string            `json:"network"`
	Env           map[string]string `json:"env"`
	RegistryAuth  []RegistryAuth    `json:"registryAuth"`
	ComposeUpArgs []string          `json:"composeUpArgs"`
	// Ready and RestartRule are the deploy's own messages as JSON, so the moved App is checked and watched as before.
	Ready       json.RawMessage `json:"ready,omitempty"`
	RestartRule json.RawMessage `json:"restartRule,omitempty"`
	// StartAfterReady names the services this member starts only once the rest is ready.
	StartAfterReady  []string    `json:"startAfterReady,omitempty"`
	Mounts           []MountFile `json:"mounts,omitempty"`
	FilesBindSources []string    `json:"filesBindSources,omitempty"`
	// Restore is the backup this member restores before the workload counts as started.
	Restore *Restore `json:"restore,omitempty"`
}

// Restore names one backup and carries this agent's own RestoreRequest as JSON.
type Restore struct {
	BackupID string          `json:"backupId"`
	TakenAt  string          `json:"takenAt"`
	Request  json.RawMessage `json:"request"`
}

// MountFile is a file the deploy writes beside the stack.
type MountFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// Reply is a member's answer; on StaleEpoch, Epoch is the one it holds.
type Reply struct {
	// Started is true only when a Start order ran and the workload came up.
	Started bool `json:"started,omitempty"`
	// Restored is true only when the Start order's backup was restored.
	Restored bool   `json:"restored,omitempty"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Epoch    uint64 `json:"epoch,omitempty"`
}

// ReadOrder reads one order line.
func ReadOrder(r io.Reader) (Order, error) {
	var o Order
	line, err := bufio.NewReaderSize(io.LimitReader(r, MaxLine), 4096).ReadBytes('\n')
	if err != nil {
		return o, err
	}
	err = json.Unmarshal(line, &o)
	return o, err
}

// WriteReply writes one reply line.
func WriteReply(w io.Writer, r Reply) error {
	raw, _ := json.Marshal(r)
	_, err := w.Write(append(raw, '\n'))
	return err
}
