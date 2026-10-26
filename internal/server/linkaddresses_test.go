package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// An agent serving without mTLS never links, so it keeps no pair to report or replace.
func TestLinkAddresses_anAgentThatDoesNotLinkHasNoPair(t *testing.T) {
	dockerOnPath(t, "exit 1")
	c, done := dialLocal(t)
	defer done()
	ctx := context.Background()

	resp, err := c.Hello(ctx, &pb.HelloRequest{ContractVersion: pb.ContractVersion_CONTRACT_VERSION_V1})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetLinkAddresses() != nil {
		t.Fatalf("Hello reports %v, want no pair at all", resp.GetLinkAddresses())
	}
	_, err = c.SetLinkAddresses(ctx, &pb.LinkAddresses{Primary: "https://panel.example.com"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SetLinkAddresses = %v, want FailedPrecondition", err)
	}
}
