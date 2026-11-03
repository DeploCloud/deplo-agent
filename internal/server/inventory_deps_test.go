package server

import (
	"encoding/json"
	"reflect"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// depsDoc builds a syft document: installed copies under /app, lockfile entries and their edges.
func depsDoc(t *testing.T, lockfile string, lock []string, edges [][2]string) *syftDoc {
	t.Helper()
	var arts []map[string]any
	for _, n := range []string{"express", "body-parser", "bytes", "mocha", "debug", "left-pad"} {
		arts = append(arts, map[string]any{
			"id": "i-" + n, "name": n, "version": "1.0.0", "type": "npm", "purl": "pkg:npm/" + n + "@1.0.0",
			"foundBy": installedCataloger, "locations": []map[string]string{{"path": "/app/node_modules/" + n + "/package.json"}},
		})
	}
	for _, n := range lock {
		arts = append(arts, map[string]any{
			"id": "l-" + n, "name": n, "version": "1.0.0", "type": "npm", "purl": "pkg:npm/" + n + "@1.0.0",
			"foundBy": lockCataloger, "locations": []map[string]string{{"path": "/app/" + lockfile}},
		})
	}
	var rels []map[string]string
	for _, e := range edges {
		rels = append(rels, map[string]string{"parent": "l-" + e[0], "child": "l-" + e[1], "type": "dependency-of"})
	}
	b, _ := json.Marshal(map[string]any{"artifacts": arts, "artifactRelationships": rels})
	var doc syftDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return &doc
}

const manifest = `{"dependencies":{"express":"^4"},"devDependencies":{"mocha":"^10","left-pad":"1"}}`

func TestMarkDeps_pnpmGraph(t *testing.T) {
	// pnpm and yarn list dev packages in the lockfile: the graph tells them apart.
	doc := depsDoc(t, "pnpm-lock.yaml",
		[]string{"express", "body-parser", "bytes", "mocha", "debug", "left-pad"},
		[][2]string{{"body-parser", "express"}, {"bytes", "body-parser"}, {"debug", "mocha"}, {"debug", "express"}})
	got := markDeps(doc, "/app", []byte(manifest))
	want := map[string]depMark{
		"i-express":     {direct: true},
		"i-body-parser": {via: []string{"express"}},
		"i-bytes":       {via: []string{"body-parser", "express"}},
		"i-debug":       {via: []string{"express"}},
		"i-mocha":       {buildOnly: true},
		"i-left-pad":    {buildOnly: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("marks:\n got %v\nwant %v", got, want)
	}
}

func TestMarkDeps_devlessLockLeavesDevOut(t *testing.T) {
	// npm's and bun's lockfiles reach syft without dev packages: installed but absent is build-only.
	for _, lock := range []string{"package-lock.json", "bun.lock"} {
		doc := depsDoc(t, lock,
			[]string{"express", "body-parser", "bytes", "debug"},
			[][2]string{{"body-parser", "express"}, {"bytes", "body-parser"}, {"debug", "express"}})
		got := markDeps(doc, "/app/", []byte(manifest))
		if !got["i-mocha"].buildOnly || !got["i-left-pad"].buildOnly || got["i-bytes"].buildOnly {
			t.Fatalf("%s: unexpected marks: %v", lock, got)
		}
	}
}

func TestMarkDeps_marksNothingItCannotTell(t *testing.T) {
	doc := depsDoc(t, "yarn.lock", []string{"express"}, nil)
	for name, m := range map[string][]byte{
		"no manifest": nil,
		"workspaces":  []byte(`{"workspaces":["packages/*"],"dependencies":{"express":"4"}}`),
		"not json":    []byte(`{`),
	} {
		if got := markDeps(doc, "/app", m); len(got) != 0 {
			t.Errorf("%s: want no marks, got %v", name, got)
		}
	}
	// Without a lockfile only the manifest speaks: what it names is direct, the rest unknown.
	got := markDeps(depsDoc(t, "yarn.lock", nil, nil), "/app", []byte(manifest))
	if want := map[string]depMark{"i-express": {direct: true}}; !reflect.DeepEqual(got, want) {
		t.Errorf("no lockfile: got %v, want %v", got, want)
	}
}

func TestMarkDeps_bunPeersAreFollowed(t *testing.T) {
	// syft draws no edge for a peer; bun.lock still names it, and the chain goes through it.
	doc := depsDoc(t, "bun.lock", []string{"express", "body-parser", "bytes"}, [][2]string{{"bytes", "body-parser"}})
	for i := range doc.Artifacts {
		if doc.Artifacts[i].ID == "l-express" {
			doc.Artifacts[i].Metadata.PeerDependencies = map[string]string{"body-parser": "^1"}
		}
	}
	got := markDeps(doc, "/app", []byte(manifest))
	if !reflect.DeepEqual(got["i-bytes"], depMark{via: []string{"body-parser", "express"}}) {
		t.Fatalf("bytes: got %v", got["i-bytes"])
	}
	if got["i-body-parser"].direct || !got["i-express"].direct {
		t.Fatalf("direct marks wrong: %v", got)
	}
}

func TestMarkDeps_nestedCopyIsNotDirect(t *testing.T) {
	doc := depsDoc(t, "bun.lock", []string{"express"}, nil)
	doc.Artifacts[0].Locations[0].Path = "/app/node_modules/foo/node_modules/express/package.json"
	if got := markDeps(doc, "/app", []byte(manifest)); got["i-express"].direct {
		t.Fatalf("a nested copy is another package's: %v", got["i-express"])
	}
}

func TestApplyDepMarks(t *testing.T) {
	doc := depsDoc(t, "package-lock.json", []string{"express"}, nil)
	resp := &pb.InventoryImageResponse{Packages: []*pb.ImagePackage{
		{Purl: "pkg:npm/mocha@1.0.0", Location: "/app/node_modules/mocha/package.json"},
		{Purl: "pkg:npm/express@1.0.0", Location: "/app/node_modules/express/package.json"},
	}}
	applyDepMarks(resp, doc, map[string]depMark{"i-mocha": {buildOnly: true}, "i-express": {direct: true}})
	if !resp.Packages[0].BuildOnly || resp.Packages[1].BuildOnly || !resp.Packages[1].Direct || resp.Packages[0].Direct {
		t.Fatalf("marks not applied: %v", resp.Packages)
	}
}
