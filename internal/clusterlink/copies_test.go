package clusterlink

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// deplo-cluster keeps a copy of party.go, wire/ and lease/ by hand. Nothing else says when one of them drifts:
// this compares them with the checkout beside this repo, and is skipped without one.
func TestHandKeptCopiesAgreeWithTheClusterRepo(t *testing.T) {
	repo := os.Getenv("DEPLO_CLUSTER_REPO")
	if repo == "" {
		repo = filepath.Join("..", "..", "..", "deplo-cluster")
	}
	if _, err := os.Stat(filepath.Join(repo, "go.mod")); err != nil {
		t.Skip("no deplo-cluster checkout beside this repo (set DEPLO_CLUSTER_REPO)")
	}
	// The party is the same code, but for its package and the module it imports wire from.
	mine, theirs := code(t, "party.go"), code(t, filepath.Join(repo, "internal", "party", "party.go"))
	mine = strings.NewReplacer("deplo-agent/internal/clusterlink", "deplo-cluster/internal", "package clusterlink", "package party").Replace(mine)
	if mine != theirs {
		t.Error("party.go and deplo-cluster internal/party/party.go differ in more than their comments")
	}
	// The contracts may each carry helpers of their own: what both declare has to be the same.
	for _, pkg := range []string{"wire", "lease"} {
		a, b := shapes(t, filepath.Join(pkg, pkg+".go")), shapes(t, filepath.Join(repo, "internal", pkg, pkg+".go"))
		for name, shape := range a {
			if other, ok := b[name]; ok && other != shape {
				t.Errorf("%s.%s differs:\n  here:  %s\n  there: %s", pkg, name, shape, other)
			}
		}
	}
}

// code is a Go file without its comments, formatted.
func code(t *testing.T, file string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := printer.Fprint(&out, fset, f); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// shapes is every struct of a file as its fields with their types and tags, and every constant as its value.
func shapes(t *testing.T, file string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	show := func(n any) string {
		var out bytes.Buffer
		_ = printer.Fprint(&out, fset, n)
		return out.String()
	}
	out := map[string]string{}
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range g.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				st, ok := s.Type.(*ast.StructType)
				if !ok {
					out["type "+s.Name.Name] = show(s.Type)
					continue
				}
				var fields []string
				for _, fl := range st.Fields.List {
					tag := ""
					if fl.Tag != nil {
						tag = fl.Tag.Value
					}
					for _, n := range fl.Names {
						fields = append(fields, fmt.Sprintf("%s %s %s", n.Name, show(fl.Type), tag))
					}
				}
				sort.Strings(fields)
				out["type "+s.Name.Name] = strings.Join(fields, "; ")
			case *ast.ValueSpec:
				if g.Tok != token.CONST {
					continue
				}
				for i, n := range s.Names {
					if i < len(s.Values) {
						out["const "+n.Name] = show(s.Values[i])
					}
				}
			}
		}
	}
	return out
}
