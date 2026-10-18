package server

import (
	pb "github.com/DeploCloud/deplo-agent/gen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestDeployRequestPreservesPublishedFilesBindField(t *testing.T) {
	wire := protowire.AppendTag(nil, 23, protowire.BytesType)
	wire = protowire.AppendString(wire, "config/app.conf")
	wire = protowire.AppendTag(wire, 24, protowire.BytesType)
	wire = protowire.AppendString(wire, "deploy/compose.yml")
	var request pb.DeployRequest
	if err := proto.Unmarshal(wire, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.GetFilesBindSources()) != 1 || request.GetFilesBindSources()[0] != "config/app.conf" {
		t.Fatalf("published field 23 changed: %v", request.GetFilesBindSources())
	}
	if request.GetComposePath() != "deploy/compose.yml" {
		t.Fatal("Compose path overlaps the published files field")
	}
}
