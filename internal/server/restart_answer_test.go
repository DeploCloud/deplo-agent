package server

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The Overview reads the metrics frame, not ListInstances: the verdict has to ride it too.
func TestAMetricsFrameCarriesTheVerdictOfEachWatchedContainer(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}, alive: false}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	w.restart = func(context.Context, string) error { return errors.New("nope") }
	runTicks(w, h, 3)
	frame := []*pb.ContainerStat{{Name: "deplo-web"}, {Name: "deplo-other"}}
	w.stampAnswers(frame)
	if frame[0].GetAnswer() != "not_answering" || frame[1].GetAnswer() != "" {
		t.Fatalf("answers on the frame: %q, %q", frame[0].GetAnswer(), frame[1].GetAnswer())
	}
	w.holdFor("web")
	frame[0].Answer = ""
	w.stampAnswers(frame)
	if frame[0].GetAnswer() != "" {
		t.Fatalf("held by a deploy: %q", frame[0].GetAnswer())
	}
}

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
