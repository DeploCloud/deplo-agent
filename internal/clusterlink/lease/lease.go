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
const MaxLine = 1 << 20

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
}

// Reply is a member's answer; on StaleEpoch, Epoch is the one it holds.
type Reply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Epoch uint64 `json:"epoch,omitempty"`
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
