package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The routes the panel renders for one App (lib/deploy/answer.ts in DeploCloud/deplo), kept in testdata: a contract
// across the two repos, as cluster-rollout.md says. Change one side and the agent refuses every table.
type answerReference struct {
	AppID          string          `json:"appId"`
	Hosts          []string        `json:"hosts"`
	TLSPort        uint32          `json:"tlsPort"`
	WebPort        uint32          `json:"webPort"`
	Alias          string          `json:"alias"`
	Transport      string          `json:"transport"`
	RoutesFile     string          `json:"routesFile"`
	EntrypointPort uint32          `json:"entrypointPort"`
	Routes         json.RawMessage `json:"routes"`
}

func readAnswerReference(t *testing.T, path string) (answerReference, any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ref answerReference
	var whole any
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(raw, &whole)
	return ref, whole
}

func TestAnswers_thePanelsRoutesPassAsTheyAre(t *testing.T) {
	ref, _ := readAnswerReference(t, filepath.Join("testdata", "answer-routes.golden.json"))
	if ref.Alias != answerAlias || ref.Transport != answerTransport || ref.RoutesFile != answerRoutesFile {
		t.Fatalf("the panel names %q, %q, %q; this agent %q, %q, %q", ref.Alias, ref.Transport, ref.RoutesFile,
			answerAlias, answerTransport, answerRoutesFile)
	}
	r, err := parseAnswerRoutes(answer{AppID: ref.AppID, TLSPort: ref.TLSPort, WebPort: ref.WebPort}, string(ref.Routes))
	if err != nil {
		t.Fatalf("the panel's routes are refused: %v", err)
	}
	hosts := r.hosts()
	slices.Sort(hosts)
	if !slices.Equal(hosts, ref.Hosts) {
		t.Fatalf("they pass on %v, the panel meant %v", hosts, ref.Hosts)
	}
	if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", AnswerPort: ref.EntrypointPort,
		Answers:      []*pb.ClusterAnswer{{AppId: ref.AppID, Routes: string(ref.Routes), TlsPort: ref.TLSPort, WebPort: ref.WebPort}},
		ServeAnswers: []string{ref.AppID}}); err != nil {
		t.Fatalf("a table with them is refused: %v", err)
	}
}

// panelReference is the panel's own copy of the reference, from DEPLO_REPO or a checkout beside this repo.
func panelReference(t *testing.T) string {
	t.Helper()
	repo := os.Getenv("DEPLO_REPO")
	if repo == "" {
		repo = filepath.Join("..", "..", "..", "deplo")
	}
	path := filepath.Join(repo, "lib", "deploy", "answer-routes.golden.json")
	if _, err := os.Stat(path); err != nil {
		t.Skip("no panel reference beside this repo (set DEPLO_REPO)")
	}
	return path
}

// What the panel renders today passes this agent, even while its copy lags a change made here first.
func TestAnswers_thePanelsReferencePasses(t *testing.T) {
	ref, _ := readAnswerReference(t, panelReference(t))
	if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", AnswerPort: ref.EntrypointPort,
		Answers:      []*pb.ClusterAnswer{{AppId: ref.AppID, Routes: string(ref.Routes), TlsPort: ref.TLSPort, WebPort: ref.WebPort}},
		ServeAnswers: []string{ref.AppID}}); err != nil {
		t.Fatalf("this agent refuses what the panel renders today: %v", err)
	}
}

// The panel keeps the same file; beside this repo, the two must say the same.
func TestAnswers_theReferenceAgreesWithThePanelRepo(t *testing.T) {
	theirs := panelReference(t)
	_, mine := readAnswerReference(t, filepath.Join("testdata", "answer-routes.golden.json"))
	_, panel := readAnswerReference(t, theirs)
	if !reflect.DeepEqual(mine, panel) {
		t.Fatal("testdata/answer-routes.golden.json differs from the panel's lib/deploy/answer-routes.golden.json")
	}
}
