package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

const (
	shopTLS uint32 = 10001
	shopWeb uint32 = 10002
)

// routesFor is one App's routes as Deplo renders them: its hostnames by SNI on websecure, their checks on web.
func routesFor(appID string, tls, web uint32, hosts ...string) string {
	name := entranceName(appID)
	sni, hostRules := make([]string, len(hosts)), make([]string, len(hosts))
	for i, h := range hosts {
		sni[i], hostRules[i] = "HostSNI(`"+h+"`)", "Host(`"+h+"`)"
	}
	webRule := hostRules[0]
	if len(hosts) > 1 {
		webRule = "(" + strings.Join(hostRules, " || ") + ")"
	}
	r := map[string]any{
		"tcp": map[string]any{
			"routers": map[string]any{name: map[string]any{"rule": strings.Join(sni, " || "), "entryPoints": []string{"websecure"},
				"service": name, "tls": map[string]any{"passthrough": true}}},
			"services": map[string]any{name: map[string]any{"loadBalancer": map[string]any{"serversTransport": entranceTransport,
				"servers": []any{map[string]any{"address": fmt.Sprintf("%s:%d", entranceAlias, tls)}}}}},
		},
		"http": map[string]any{
			"routers": map[string]any{name + "-acme": map[string]any{"rule": webRule + " && " + entranceChallenge,
				"entryPoints": []string{"web"}, "service": name + "-acme", "priority": 2000000001}},
			"services": map[string]any{name + "-acme": map[string]any{"loadBalancer": map[string]any{
				"servers": []any{map[string]any{"url": fmt.Sprintf("http://%s:%d", entranceAlias, web)}}}}},
		},
	}
	raw, _ := json.Marshal(r)
	return string(raw)
}

func shop() *pb.ClusterEntrance {
	return &pb.ClusterEntrance{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb,
		Routes: routesFor("prj1", shopTLS, shopWeb, "shop.example.com", "www.shop.example.com")}
}

func appEntry(member string) lease.Entry {
	return lease.Entry{Kind: "app", ID: "prj1", Member: member, Slug: "shop"}
}

// quickEntrances shortens how long a passed-on connection waits for a ready server.
func quickEntrances(t *testing.T, wait time.Duration) {
	t.Helper()
	w, r, s := entranceWait, entranceRetry, entranceSettle
	entranceWait, entranceRetry, entranceSettle = wait, 50*time.Millisecond, 0
	t.Cleanup(func() { entranceWait, entranceRetry, entranceSettle = w, r, s })
}

// askPort connects as the entrance's container does for a caller that dialed port: the port line, then its bytes.
func askPort(t *testing.T, s forwardSpec, port uint32) string {
	t.Helper()
	c, err := net.DialTimeout("unix", filepath.Join(s.Dir, fwdSocket), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = io.WriteString(c, "PORT "+strconv.Itoa(int(port))+"\n")
	_, _ = io.WriteString(c, "ping")
	out, _ := io.ReadAll(c)
	return string(out)
}

// entrancePair is two members: A passes the App's domains on, B runs the App. Each one's proxy is a backend
// that names its server, so an answer says which proxy it came from.
func entrancePair(t *testing.T) (a, b *Service, fa, fb *fakeFwdHost, members []*pb.ClusterForwarderMember) {
	t.Helper()
	a, b, fa, fb, fpA, fpB := memberPair(t)
	members = []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	return a, b, fa, fb, members
}

// bothAnswer gives A and B the entrance on proxies of their own, and B the App to answer for.
func bothAnswer(t *testing.T, a, b *Service, members []*pb.ClusterForwarderMember) {
	t.Helper()
	onA, onB := backend(t, "proxy-a"), backend(t, "proxy-b")
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onA,
		Entrances: []*pb.ClusterEntrance{shop()}, ServeEntrances: []string{"prj1"}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onB,
		Entrances: []*pb.ClusterEntrance{shop()}, ServeEntrances: []string{"prj1"}})
}

func TestEntrances_passADomainOnToTheProxyOfTheServerThatRunsTheApp(t *testing.T) {
	a, b, fa, _, members := entrancePair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))

	s := fa.spec(t, entranceAlias)
	if s.Network != entranceNetwork || s.Port != entranceListen {
		t.Fatalf("the entrance's forwarder is %+v", s)
	}
	if got := askPort(t, s, shopTLS); got != "proxy-b:PING" {
		t.Fatalf("its TLS reached %q, want B's internal entrypoint", got)
	}
	if got := askPort(t, s, shopWeb); got != "proxy-b:PING" {
		t.Fatalf("a certificate check reached %q, want B's internal entrypoint", got)
	}
	if got := askPort(t, s, 22); got != "" {
		t.Fatalf("a port that is no App's was passed on: %q", got)
	}
	file, err := os.ReadFile(filepath.Join(a.traefikDir(), entranceRoutesFile))
	if err != nil || !strings.Contains(string(file), "HostSNI(`shop.example.com`)") ||
		!strings.Contains(string(file), `"address": "deplo-entrance:10001"`) {
		t.Fatalf("the proxy's entrances file is %q (%v)", file, err)
	}
}

// After a failover the App may run on the server a visitor reached: its proxy gets the connection back.
func TestEntrances_answerOnTheirOwnProxyWhileTheAppRunsHere(t *testing.T) {
	a, b, fa, _, members := entrancePair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvA"))
	if got := askPort(t, fa.spec(t, entranceAlias), shopTLS); got != "proxy-a:PING" {
		t.Fatalf("it answered %q", got)
	}
}

func TestEntrances_receiverAnswersOnlyAnAppItWasToldToServeAndRuns(t *testing.T) {
	quickEntrances(t, 300*time.Millisecond)
	for name, tc := range map[string]struct {
		serve []string
		runs  string
	}{
		"not told to serve it": {nil, "srvB"},
		"another App":          {[]string{"prj2"}, "srvB"},
		"placed elsewhere":     {[]string{"prj1"}, "srvC"},
	} {
		t.Run(name, func(t *testing.T) {
			a, b, fa, _, members := entrancePair(t)
			onB := backend(t, "proxy-b")
			apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onB,
				Entrances: []*pb.ClusterEntrance{shop()}})
			apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onB,
				ServeEntrances: tc.serve})
			placeOn(t, a, "srvA", appEntry("srvB"))
			placeOn(t, b, "srvB", appEntry(tc.runs))
			if got := askPort(t, fa.spec(t, entranceAlias), shopTLS); got != "" {
				t.Fatalf("the receiver answered %q", got)
			}
		})
	}
}

// Two servers run the App: the one a visitor reached is not ready, so the other answers.
func TestEntrances_fallBackToAnotherServerThatRunsTheApp(t *testing.T) {
	quickEntrances(t, 2*time.Second)
	a, b, fa, _, members := entrancePair(t)
	bothAnswer(t, a, b, members)
	two := []lease.Entry{appEntry("srvA"), {Kind: "app", ID: "prj1", Replica: 1, Member: "srvB", Slug: "shop"}}
	placeOn(t, a, "srvA", two...)
	placeOn(t, b, "srvB", two...)
	fa.mu.Lock()
	fa.notReady["prj1"] = true
	fa.mu.Unlock()
	if got := askPort(t, fa.spec(t, entranceAlias), shopTLS); got != "proxy-b:PING" {
		t.Fatalf("with A not ready it answered %q, want B", got)
	}
}

// A failover placed the App on B, which is still starting it: the visitor's connection waits for it.
func TestEntrances_waitForTheAppToBeReady(t *testing.T) {
	quickEntrances(t, 5*time.Second)
	a, b, fa, fb, members := entrancePair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	fb.mu.Lock()
	fb.notReady["prj1"] = true
	fb.mu.Unlock()
	go func() {
		time.Sleep(400 * time.Millisecond)
		fb.mu.Lock()
		delete(fb.notReady, "prj1")
		fb.mu.Unlock()
	}()
	start := time.Now()
	if got := askPort(t, fa.spec(t, entranceAlias), shopTLS); got != "proxy-b:PING" {
		t.Fatalf("once B was ready it answered %q", got)
	}
	if waited := time.Since(start); waited < 300*time.Millisecond {
		t.Fatalf("it answered after %v, before B was ready", waited)
	}
}

func TestEntrances_closeAConnectionNoServerIsReadyFor(t *testing.T) {
	quickEntrances(t, 400*time.Millisecond)
	a, b, fa, fb, members := entrancePair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	fb.mu.Lock()
	fb.notReady["prj1"] = true
	fb.mu.Unlock()
	start := time.Now()
	if got := askPort(t, fa.spec(t, entranceAlias), shopTLS); got != "" {
		t.Fatalf("a server that is not ready answered %q", got)
	}
	if waited := time.Since(start); waited < 400*time.Millisecond {
		t.Fatalf("it gave up after %v, before its wait", waited)
	}
}

// A name of an App on two servers: when this one's copy does not answer, the other one does.
func TestForwarders_aNameFallsBackToAnotherServerThatRunsTheApp(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	port := backend(t, "on-b")
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	held := &pb.ClusterHeldName{Network: envNet, Kind: "app", WorkloadId: "prj1", Name: "deplo-shop", Port: port}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{held}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "app", WorkloadId: "prj1", Port: port, SourceFingerprints: []string{fpA}}}})
	two := []lease.Entry{appEntry("srvA"), {Kind: "app", ID: "prj1", Replica: 1, Member: "srvB", Slug: "shop"}}
	placeOn(t, a, "srvA", two...)
	placeOn(t, b, "srvB", two...)
	fa.mu.Lock()
	fa.running["prj1"] = "deplo-env-other"
	fa.mu.Unlock()
	if got := ask(t, fa.spec(t, "deplo-shop")); got != "on-b:PING" {
		t.Fatalf("with A's copy not answering it answered %q, want B", got)
	}
}

func TestEntrances_oneForwarderCarriesEveryApp(t *testing.T) {
	a, _, fa, _, members := entrancePair(t)
	blog := &pb.ClusterEntrance{AppId: "prj2", TlsPort: 10003, WebPort: 10004, Routes: routesFor("prj2", 10003, 10004, "blog.example.com")}
	file := func() string {
		raw, _ := os.ReadFile(filepath.Join(a.traefikDir(), entranceRoutesFile))
		return string(raw)
	}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: 9443,
		Entrances: []*pb.ClusterEntrance{shop(), blog}})
	first := fa.spec(t, entranceAlias)
	if !strings.Contains(file(), "blog.example.com") || !strings.Contains(file(), "shop.example.com") {
		t.Fatalf("the file lacks an App: %s", file())
	}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: 9443,
		Entrances: []*pb.ClusterEntrance{shop()}})
	fa.mu.Lock()
	n, removed := 0, slices.Clone(fa.removed)
	for _, s := range fa.specs {
		if s.Alias == entranceAlias {
			n++
		}
	}
	fa.mu.Unlock()
	if n != 1 || len(removed) != 0 || fa.spec(t, entranceAlias).Container != first.Container {
		t.Fatalf("an App leaving replaced the forwarder: %d forwarders, removed %v", n, removed)
	}
	if strings.Contains(file(), "blog.example.com") {
		t.Fatalf("the App that left still has routes: %s", file())
	}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members})
	if strings.TrimSpace(file()) != "{}" {
		t.Fatalf("with no App the file still routes: %s", file())
	}
	fa.mu.Lock()
	gone := slices.Contains(fa.removed, first.Container)
	fa.mu.Unlock()
	if !gone {
		t.Fatal("with no App the forwarder stayed")
	}
}

func TestEntrances_refuseABadTable(t *testing.T) {
	withRoutes := func(edit func(r map[string]any)) *pb.ClusterEntrance {
		var r map[string]any
		_ = json.Unmarshal([]byte(shop().Routes), &r)
		edit(r)
		raw, _ := json.Marshal(r)
		return &pb.ClusterEntrance{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb, Routes: string(raw)}
	}
	tcpRouter := func(r map[string]any) map[string]any {
		return r["tcp"].(map[string]any)["routers"].(map[string]any)["deplo-entrance-prj1"].(map[string]any)
	}
	httpRouter := func(r map[string]any) map[string]any {
		return r["http"].(map[string]any)["routers"].(map[string]any)["deplo-entrance-prj1-acme"].(map[string]any)
	}
	other := &pb.ClusterEntrance{AppId: "prj2", TlsPort: 10003, WebPort: 10004, Routes: routesFor("prj2", 10003, 10004, "shop.example.com")}
	for name, e := range map[string][]*pb.ClusterEntrance{
		"routes that are not JSON": {{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb, Routes: "tcp: {}"}},
		"no hostname":              {{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb, Routes: `{"tcp":{"routers":{},"services":{}}}`}},
		"an unknown field": {withRoutes(func(r map[string]any) {
			tcpRouter(r)["middlewares"] = []string{"x"}
		})},
		"another entrypoint": {withRoutes(func(r map[string]any) { tcpRouter(r)["entryPoints"] = []string{"entrance"} })},
		"terminating TLS":    {withRoutes(func(r map[string]any) { tcpRouter(r)["tls"] = map[string]any{"passthrough": false} })},
		"a rule on a path":   {withRoutes(func(r map[string]any) { tcpRouter(r)["rule"] = "HostSNI(`a.com`) && ClientIP(`1.2.3.4`)" })},
		"the proxy's own service": {withRoutes(func(r map[string]any) {
			tcpRouter(r)["service"] = "api@internal"
		})},
		"a router named for another App": {withRoutes(func(r map[string]any) {
			rs := r["tcp"].(map[string]any)["routers"].(map[string]any)
			rs["deplo-entrance-prj9"] = rs["deplo-entrance-prj1"]
		})},
		"a check route for more than the checks": {withRoutes(func(r map[string]any) {
			httpRouter(r)["rule"] = "Host(`shop.example.com`)"
		})},
		"a check route on websecure": {withRoutes(func(r map[string]any) { httpRouter(r)["entryPoints"] = []string{"websecure"} })},
		"another App's port": {{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb,
			Routes: routesFor("prj1", 10003, shopWeb, "shop.example.com")}},
		"a port twice":            {shop(), {AppId: "prj2", TlsPort: shopTLS, WebPort: 10004, Routes: routesFor("prj2", shopTLS, 10004, "b.com")}},
		"a low port":              {{AppId: "prj1", TlsPort: 443, WebPort: shopWeb, Routes: routesFor("prj1", 443, shopWeb, "shop.example.com")}},
		"a hostname for two Apps": {shop(), other},
		"an App twice":            {shop(), shop()},
		"a bad App id":            {{AppId: "../x", TlsPort: shopTLS, WebPort: shopWeb, Routes: shop().Routes}},
	} {
		if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", EntrancePort: 9443, Entrances: e}); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
	for name, req := range map[string]*pb.SetClusterForwardersRequest{
		"no entrypoint":   {ClusterId: "cl1", Entrances: []*pb.ClusterEntrance{shop()}},
		"served, no port": {ClusterId: "cl1", ServeEntrances: []string{"prj1"}},
	} {
		if _, err := validateForwarders(req); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
	if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", EntrancePort: 9443,
		Entrances: []*pb.ClusterEntrance{shop()}, ServeEntrances: []string{"prj2"}}); err != nil {
		t.Fatalf("a good table was refused: %v", err)
	}
}

func TestEntrances_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.forwarders.entrances") {
		t.Fatal("Hello does not advertise cluster.forwarders.entrances")
	}
}
