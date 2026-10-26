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
		"i-express":     {},
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

func TestMarkDeps_npmLockLeavesDevOut(t *testing.T) {
	// npm's lockfile reaches syft without dev packages: installed but absent is build-only.
	doc := depsDoc(t, "package-lock.json",
		[]string{"express", "body-parser", "bytes", "debug"},
		[][2]string{{"body-parser", "express"}, {"bytes", "body-parser"}, {"debug", "express"}})
	got := markDeps(doc, "/app/", []byte(manifest))
	if !got["i-mocha"].buildOnly || !got["i-left-pad"].buildOnly || got["i-bytes"].buildOnly {
		t.Fatalf("unexpected marks: %v", got)
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
	if got := markDeps(depsDoc(t, "yarn.lock", nil, nil), "/app", []byte(manifest)); len(got) != 0 {
		t.Errorf("no lockfile: want no marks, got %v", got)
	}
}

func TestApplyDepMarks(t *testing.T) {
	doc := depsDoc(t, "package-lock.json", []string{"express"}, nil)
	resp := &pb.InventoryImageResponse{Packages: []*pb.ImagePackage{
		{Purl: "pkg:npm/mocha@1.0.0", Location: "/app/node_modules/mocha/package.json"},
		{Purl: "pkg:npm/express@1.0.0", Location: "/app/node_modules/express/package.json"},
	}}
	applyDepMarks(resp, doc, map[string]depMark{"i-mocha": {buildOnly: true}, "i-express": {via: nil}})
	if !resp.Packages[0].BuildOnly || resp.Packages[1].BuildOnly {
		t.Fatalf("marks not applied: %v", resp.Packages)
	}
}
