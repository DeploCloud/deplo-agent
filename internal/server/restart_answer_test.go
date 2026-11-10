package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWatcherReportsWhatItLastSaw(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}, alive: true}
	w := newWatcherOn(t, h)
	if got := w.answerFor("web", "deplo-web"); got != "" {
		t.Fatalf("no rule: %q", got)
	}
	w.release("web", httpRule(), true)
	if got := w.answerFor("web", "deplo-web"); got != "" {
		t.Fatalf("before a probe: %q", got)
	}
	runTicks(w, h, 1)
	if got := w.answerFor("web", "deplo-web"); got != "answering" {
		t.Fatalf("answering: %q", got)
	}
	h.alive = false
	runTicks(w, h, 1)
	if got := w.answerFor("web", "deplo-web"); got != "answering" {
		t.Fatalf("one miss is not a verdict: %q", got)
	}
	w.restart = func(context.Context, string) error { return errors.New("nope") }
	runTicks(w, h, 2)
	if got := w.answerFor("web", "deplo-web"); got != "not_answering" {
		t.Fatalf("not answering: %q", got)
	}
	w.holdFor("web")
	if got := w.answerFor("web", "deplo-web"); got != "" {
		t.Fatalf("held by a deploy: %q", got)
	}
}

func TestWatcherCallsAFreshContainerStarting(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Second), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	runTicks(w, h, 1)
	if got := w.answerFor("web", "deplo-web"); got != "starting" {
		t.Fatalf("start period: %q", got)
	}
}
