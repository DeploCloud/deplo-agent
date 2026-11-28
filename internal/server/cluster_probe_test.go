package server

import (
	"context"
	"testing"
)

// A probe looks at every stack its lease places here in one docker call: on a one-core member one look per stack
// held the new leader's first renewal at a hand-over (c43: 1.49 s).
func TestProbe_runningIsOneLookForEveryStack(t *testing.T) {
	g := newGuard(t).g
	single := 0
	g.running = func(context.Context, string) bool { single++; return true }
	batches := 0
	g.runningAll = func(_ context.Context, slugs []string) map[string]bool {
		batches++
		return runningIn("deplo-db-main|\nother|deplo-files\nweb|", slugs)
	}
	got := g.runningOf([]string{"db-main", "files", "elsewhere"}, 7)
	if batches != 1 || single != 0 || len(got) != 2 || got[0].Slug != "db-main" || got[1].Slug != "files" || got[0].Epoch != 7 {
		t.Fatalf("%d batched and %d single looks found %+v", batches, single, got)
	}
	g.runningAll = func(context.Context, []string) map[string]bool { return nil }
	if got := g.runningOf([]string{"db-main"}, 7); single != 1 || len(got) != 1 {
		t.Fatalf("docker did not answer the batch: %d single looks found %+v", single, got)
	}
}
