package server

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The restart-loop guard stops ONE looping service; a name docker would read as a flag must never reach the CLI.
func TestStopStackRefusesAnInvalidService(t *testing.T) {
	s := &Service{stackDir: t.TempDir()}
	for _, name := range []string{"--rm", "-f", "", "web;rm -rf /", "../web"} {
		_, err := s.StopStack(context.Background(), &pb.StackRef{
			Slug:     "stopsvc-fixture",
			Services: []string{name},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("service %q must be refused, got err=%v", name, err)
		}
	}
}

func TestComposeCtlPutsServicesAfterTheVerb(t *testing.T) {
	s := &Service{stackDir: t.TempDir()}
	args := append([]string{"stop"}, "web", "worker")
	joined := strings.Join(s.composeCtl("stopsvc-fixture", args...), " ")
	if !strings.HasSuffix(joined, "stop web worker") {
		t.Fatalf("services must follow the verb: %s", joined)
	}
}
