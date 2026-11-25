package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	name := answerName(appID)
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
			"services": map[string]any{name: map[string]any{"loadBalancer": map[string]any{"serversTransport": answerTransport,
				"servers": []any{map[string]any{"address": fmt.Sprintf("%s:%d", answerAlias, tls)}}}}},
		},
		"http": map[string]any{
			"routers": map[string]any{name + "-acme": map[string]any{"rule": webRule + " && " + answerChallenge,
				"entryPoints": []string{"web"}, "service": name + "-acme", "priority": 2000000001}},
			"services": map[string]any{name + "-acme": map[string]any{"loadBalancer": map[string]any{
				"servers": []any{map[string]any{"url": fmt.Sprintf("http://%s:%d", answerAlias, web)}}}}},
		},
	}
	raw, _ := json.Marshal(r)
	return string(raw)
}

func shop() *pb.ClusterAnswer {
	return &pb.ClusterAnswer{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb,
		Routes: routesFor("prj1", shopTLS, shopWeb, "shop.example.com", "www.shop.example.com")}
}

func appEntry(member string) lease.Entry {
	return lease.Entry{Kind: "app", ID: "prj1", Member: member, Slug: "shop"}
}

// quickAnswers shortens how long a passed-on connection waits for a ready server.
func quickAnswers(t *testing.T, wait time.Duration) {
	t.Helper()
	w, r, s, p := answerWait, answerRetry, answerSettle, proxySettle
	answerWait, answerRetry, answerSettle, proxySettle = wait, 50*time.Millisecond, 0, 0
	t.Cleanup(func() { answerWait, answerRetry, answerSettle, proxySettle = w, r, s, p })
}

// askPort connects as the answer forwarder's container does for a caller that dialed port: the port line, then its bytes.
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

// answerPair is two members: A passes the App's domains on, B runs the App. Each one's proxy is a backend
// that names its server, so an answer says which proxy it came from.
func answerPair(t *testing.T) (a, b *Service, fa, fb *fakeFwdHost, members []*pb.ClusterForwarderMember) {
	t.Helper()
	a, b, fa, fb, fpA, fpB := memberPair(t)
	members = []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	return a, b, fa, fb, members
}

// bothAnswer gives A and B the answer on proxies of their own, and B the App to answer for.
func bothAnswer(t *testing.T, a, b *Service, members []*pb.ClusterForwarderMember) {
	t.Helper()
	onA, onB := backend(t, "proxy-a"), backend(t, "proxy-b")
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: onA,
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: onB,
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
}

func TestAnswers_passADomainOnToTheProxyOfTheServerThatRunsTheApp(t *testing.T) {
	a, b, fa, _, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))

	s := fa.spec(t, answerAlias)
	if s.Network != answerNetwork || s.Port != answerListen {
		t.Fatalf("the answer forwarder is %+v", s)
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
	file, err := os.ReadFile(filepath.Join(a.traefikDir(), answerRoutesFile))
	if err != nil || !strings.Contains(string(file), "HostSNI(`shop.example.com`)") ||
		!strings.Contains(string(file), `"address": "deplo-answer:10001"`) {
		t.Fatalf("the proxy's routes file is %q (%v)", file, err)
	}
}

// eu, 24 Nov 2026: a domain added to one App cut every App passed on through the other servers for 1 to 2 s. A
// new table held the forwarders' lock while docker answered slowly, and every connection waited on it.
func TestAnswers_aNewTableWhileDockerIsSlowHoldsUpNoConnection(t *testing.T) {
	a, b, fa, fb, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	s := fa.spec(t, answerAlias)

	slow := make(chan struct{})
	defer func() {
		if slow != nil {
			close(slow)
		}
	}()
	for _, h := range []*fakeFwdHost{fa, fb} {
		h.mu.Lock()
		h.slow = slow
		h.mu.Unlock()
	}
	blog := &pb.ClusterAnswer{AppId: "prj2", TlsPort: 10003, WebPort: 10004, Routes: routesFor("prj2", 10003, 10004, "blog.example.com")}
	done := make(chan struct{}, 2)
	for _, srv := range []*Service{a, b} {
		go func() {
			_, _ = srv.SetClusterForwarders(context.Background(), &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
				AnswerPort: srv.forwarders().state().AnswerPort, Answers: []*pb.ClusterAnswer{shop(), blog}, ServeAnswers: []string{"prj1", "prj2"}})
			done <- struct{}{}
		}()
	}
	time.Sleep(100 * time.Millisecond)

	got := make(chan string, 1)
	go func() { got <- askPort(t, s, shopTLS) }()
	select {
	case g := <-got:
		if g != "proxy-b:PING" {
			t.Fatalf("while the new table waited on docker, shop reached %q", g)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shop's connection waited for the new table, which waited for docker")
	}
	close(slow)
	slow = nil
	<-done
	<-done
}

// After a failover the App may run on the server a visitor reached: its proxy gets the connection back.
func TestAnswers_answerOnTheirOwnProxyWhileTheAppRunsHere(t *testing.T) {
	a, b, fa, _, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvA"))
	if got := askPort(t, fa.spec(t, answerAlias), shopTLS); got != "proxy-a:PING" {
		t.Fatalf("it answered %q", got)
	}
}

func TestAnswers_receiverAnswersOnlyAnAppItWasToldToServeAndRuns(t *testing.T) {
	quickAnswers(t, 300*time.Millisecond)
	for name, tc := range map[string]struct {
		serve []string
		runs  string
	}{
		"not told to serve it": {nil, "srvB"},
		"another App":          {[]string{"prj2"}, "srvB"},
		"placed elsewhere":     {[]string{"prj1"}, "srvC"},
	} {
		t.Run(name, func(t *testing.T) {
			a, b, fa, _, members := answerPair(t)
			onB := backend(t, "proxy-b")
			apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: onB,
				Answers: []*pb.ClusterAnswer{shop()}})
			apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: onB,
				ServeAnswers: tc.serve})
			placeOn(t, a, "srvA", appEntry("srvB"))
			placeOn(t, b, "srvB", appEntry(tc.runs))
			if got := askPort(t, fa.spec(t, answerAlias), shopTLS); got != "" {
				t.Fatalf("the receiver answered %q", got)
			}
		})
	}
}

// Two servers run the App: the one a visitor reached is not ready, so the other answers.
func TestAnswers_fallBackToAnotherServerThatRunsTheApp(t *testing.T) {
	quickAnswers(t, 2*time.Second)
	a, b, fa, _, members := answerPair(t)
	bothAnswer(t, a, b, members)
	two := []lease.Entry{appEntry("srvA"), {Kind: "app", ID: "prj1", Replica: 1, Member: "srvB", Slug: "shop"}}
	placeOn(t, a, "srvA", two...)
	placeOn(t, b, "srvB", two...)
	fa.mu.Lock()
	fa.notReady["prj1"] = true
	fa.mu.Unlock()
	if got := askPort(t, fa.spec(t, answerAlias), shopTLS); got != "proxy-b:PING" {
		t.Fatalf("with A not ready it answered %q, want B", got)
	}
}

// Load balancing runs the App on every server (#228): each server a visitor reaches serves them from its own copy.
func TestAnswers_anAppOnEveryServerIsServedByTheCopyAVisitorReached(t *testing.T) {
	a, b, fa, fb, members := answerPair(t)
	bothAnswer(t, a, b, members)
	two := []lease.Entry{appEntry("srvA"), {Kind: "app", ID: "prj1", Replica: 1, Member: "srvB", Slug: "shop"}}
	placeOn(t, a, "srvA", two...)
	placeOn(t, b, "srvB", two...)
	for i := 0; i < 5; i++ {
		if got := askPort(t, fa.spec(t, answerAlias), shopTLS); got != "proxy-a:PING" {
			t.Fatalf("a visitor of A was served by %q", got)
		}
		if got := askPort(t, fb.spec(t, answerAlias), shopTLS); got != "proxy-b:PING" {
			t.Fatalf("a visitor of B was served by %q", got)
		}
	}
}

// A failover placed the App on B, which is still starting it: the visitor's connection waits for it.
func TestAnswers_waitForTheAppToBeReady(t *testing.T) {
	quickAnswers(t, 5*time.Second)
	a, b, fa, fb, members := answerPair(t)
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
	if got := askPort(t, fa.spec(t, answerAlias), shopTLS); got != "proxy-b:PING" {
		t.Fatalf("once B was ready it answered %q", got)
	}
	if waited := time.Since(start); waited < 300*time.Millisecond {
		t.Fatalf("it answered after %v, before B was ready", waited)
	}
}

// The App runs on B but its database fails its health check: waiting would not help, so the visitor reaches B's
// proxy at once and sees the App's own answer, never a connection held 30 s and closed (fise on eu-1, 22 Nov 2026).
func TestAnswers_aBrokenAppIsPassedOnAtOnce(t *testing.T) {
	quickAnswers(t, 5*time.Second)
	a, b, fa, fb, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	fb.mu.Lock()
	fb.broken["prj1"] = true
	fb.mu.Unlock()
	for _, via := range []*fakeFwdHost{fa, fb} {
		start := time.Now()
		if got := askPort(t, via.spec(t, answerAlias), shopTLS); got != "proxy-b:PING" {
			t.Fatalf("a broken App's visitor got %q", got)
		}
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("a broken App's visitor waited %v", waited)
		}
	}
}

func TestAnswers_closeAConnectionNoServerIsReadyFor(t *testing.T) {
	quickAnswers(t, 400*time.Millisecond)
	a, b, fa, fb, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	fb.mu.Lock()
	fb.notReady["prj1"] = true
	fb.mu.Unlock()
	start := time.Now()
	if got := askPort(t, fa.spec(t, answerAlias), shopTLS); got != "" {
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

func TestAnswers_oneForwarderCarriesEveryApp(t *testing.T) {
	a, _, fa, _, members := answerPair(t)
	blog := &pb.ClusterAnswer{AppId: "prj2", TlsPort: 10003, WebPort: 10004, Routes: routesFor("prj2", 10003, 10004, "blog.example.com")}
	file := func() string {
		raw, _ := os.ReadFile(filepath.Join(a.traefikDir(), answerRoutesFile))
		return string(raw)
	}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: 9443,
		Answers: []*pb.ClusterAnswer{shop(), blog}})
	first := fa.spec(t, answerAlias)
	if !strings.Contains(file(), "blog.example.com") || !strings.Contains(file(), "shop.example.com") {
		t.Fatalf("the file lacks an App: %s", file())
	}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: 9443,
		Answers: []*pb.ClusterAnswer{shop()}})
	fa.mu.Lock()
	n, removed := 0, slices.Clone(fa.removed)
	for _, s := range fa.specs {
		if s.Alias == answerAlias {
			n++
		}
	}
	fa.mu.Unlock()
	if n != 1 || len(removed) != 0 || fa.spec(t, answerAlias).Container != first.Container {
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

func TestAnswers_refuseABadTable(t *testing.T) {
	withRoutes := func(edit func(r map[string]any)) *pb.ClusterAnswer {
		var r map[string]any
		_ = json.Unmarshal([]byte(shop().Routes), &r)
		edit(r)
		raw, _ := json.Marshal(r)
		return &pb.ClusterAnswer{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb, Routes: string(raw)}
	}
	tcpRouter := func(r map[string]any) map[string]any {
		return r["tcp"].(map[string]any)["routers"].(map[string]any)["deplo-answer-prj1"].(map[string]any)
	}
	httpRouter := func(r map[string]any) map[string]any {
		return r["http"].(map[string]any)["routers"].(map[string]any)["deplo-answer-prj1-acme"].(map[string]any)
	}
	other := &pb.ClusterAnswer{AppId: "prj2", TlsPort: 10003, WebPort: 10004, Routes: routesFor("prj2", 10003, 10004, "shop.example.com")}
	for name, e := range map[string][]*pb.ClusterAnswer{
		"routes that are not JSON": {{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb, Routes: "tcp: {}"}},
		"no hostname":              {{AppId: "prj1", TlsPort: shopTLS, WebPort: shopWeb, Routes: `{"tcp":{"routers":{},"services":{}}}`}},
		"an unknown field": {withRoutes(func(r map[string]any) {
			tcpRouter(r)["middlewares"] = []string{"x"}
		})},
		"another entrypoint": {withRoutes(func(r map[string]any) { tcpRouter(r)["entryPoints"] = []string{"answer"} })},
		"terminating TLS":    {withRoutes(func(r map[string]any) { tcpRouter(r)["tls"] = map[string]any{"passthrough": false} })},
		"a rule on a path":   {withRoutes(func(r map[string]any) { tcpRouter(r)["rule"] = "HostSNI(`a.com`) && ClientIP(`1.2.3.4`)" })},
		"the proxy's own service": {withRoutes(func(r map[string]any) {
			tcpRouter(r)["service"] = "api@internal"
		})},
		"a router named for another App": {withRoutes(func(r map[string]any) {
			rs := r["tcp"].(map[string]any)["routers"].(map[string]any)
			rs["deplo-answer-prj9"] = rs["deplo-answer-prj1"]
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
		if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", AnswerPort: 9443, Answers: e}); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
	for name, req := range map[string]*pb.SetClusterForwardersRequest{
		"no entrypoint":   {ClusterId: "cl1", Answers: []*pb.ClusterAnswer{shop()}},
		"served, no port": {ClusterId: "cl1", ServeAnswers: []string{"prj1"}},
		"a cap too high":  {ClusterId: "cl1", AnswerMaxConns: 1 << 20},
		"a cap too low":   {ClusterId: "cl1", AnswerMaxConns: 2},
	} {
		if _, err := validateForwarders(req); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
	if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", AnswerPort: 9443,
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj2"}}); err != nil {
		t.Fatalf("a good table was refused: %v", err)
	}
}

func TestAnswers_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.forwarders.answers") {
		t.Fatal("Hello does not advertise cluster.forwarders.answers")
	}
}

// waitingFor opens n connections, and returns once this agent has total waiting.
func waitingFor(t *testing.T, f *forwarders, s forwardSpec, port uint32, n, total int) []chan string {
	t.Helper()
	out := make([]chan string, n)
	for i := range out {
		out[i] = make(chan string, 1)
		go func(ch chan string) { ch <- askPort(t, s, port) }(out[i])
	}
	eventually(t, "connections waiting", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.waiting >= total
	})
	return out
}

// Many visitors wait for an App that is placed here but not ready: one check for all of them, not one each.
func TestAnswers_oneReadinessCheckForEveryWaitingConnection(t *testing.T) {
	quickAnswers(t, 5*time.Second)
	a, b, fa, _, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvA"))
	fa.mu.Lock()
	fa.notReady["prj1"] = true
	fa.mu.Unlock()
	s := fa.spec(t, answerAlias)
	waiting := waitingFor(t, a.forwarders(), s, shopTLS, 40, 40)
	time.Sleep(600 * time.Millisecond)
	fa.mu.Lock()
	checks := fa.readyCalls
	delete(fa.notReady, "prj1")
	fa.mu.Unlock()
	// One look every 50 ms here, give or take the arrivals' own first tries: far from 40 connections' worth.
	if checks > 25 {
		t.Fatalf("%d readiness checks for 40 connections in 0.6 s", checks)
	}
	for i, ch := range waiting {
		if got := <-ch; got != "proxy-a:PING" {
			t.Fatalf("connection %d answered %q once the App was ready", i, got)
		}
	}
}

func TestAnswers_closeAConnectionPastTheWaitingCaps(t *testing.T) {
	quickAnswers(t, 5*time.Second)
	a, b, fa, _, members := answerPair(t)
	blog := &pb.ClusterAnswer{AppId: "prj2", TlsPort: 10003, WebPort: 10004, Routes: routesFor("prj2", 10003, 10004, "blog.example.com")}
	onA := backend(t, "proxy-a")
	// 12 connections, a quarter of which (3) may wait, and 2 for one App.
	table := &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: onA,
		Answers: []*pb.ClusterAnswer{shop(), blog}, ServeAnswers: []string{"prj1", "prj2"}, AnswerMaxConns: 12, AnswerMaxWaiting: 2}
	apply(t, a, table)
	_ = b
	placeOn(t, a, "srvA", appEntry("srvA"), lease.Entry{Kind: "app", ID: "prj2", Member: "srvA", Slug: "blog"})
	fa.mu.Lock()
	fa.notReady["prj1"], fa.notReady["prj2"] = true, true
	fa.mu.Unlock()
	s := fa.spec(t, answerAlias)
	waitingFor(t, a.forwarders(), s, shopTLS, 2, 2)
	start := time.Now()
	if got := askPort(t, s, shopTLS); got != "" || time.Since(start) > time.Second {
		t.Fatalf("a third connection for one App answered %q after %v, want closed at once", got, time.Since(start))
	}
	waitingFor(t, a.forwarders(), s, 10003, 1, 3)
	start = time.Now()
	if got := askPort(t, s, 10003); got != "" || time.Since(start) > time.Second {
		t.Fatalf("past the server's cap a connection answered %q after %v", got, time.Since(start))
	}
	// Deplo hears of both with the next table, once.
	res, err := a.SetClusterForwarders(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][2]uint32{}
	for _, r := range res.GetAnswerRefusals() {
		if !r.GetWaiting() || r.GetMemory() {
			t.Fatalf("a waiting cap reported as another: %+v", r)
		}
		got[r.GetAppId()] = [2]uint32{r.GetCount(), r.GetCap()}
		if r.GetFirstUnixMs() == 0 || r.GetLastUnixMs() < r.GetFirstUnixMs() {
			t.Fatalf("a refusal without its times: %+v", r)
		}
	}
	if got["prj1"] != [2]uint32{1, 2} || got[""] != [2]uint32{1, 3} || len(got) != 2 {
		t.Fatalf("Deplo was told %v", got)
	}
	if res, _ := a.SetClusterForwarders(context.Background(), table); len(res.GetAnswerRefusals()) != 0 {
		t.Fatalf("a refusal was reported twice: %v", res.GetAnswerRefusals())
	}
}

func TestAnswers_closeAConnectionPastTheForwardersCap(t *testing.T) {
	quickAnswers(t, 5*time.Second)
	a, b, fa, _, members := answerPair(t)
	bothAnswer(t, a, b, members)
	old := answerMaxConns
	answerMaxConns = 1
	t.Cleanup(func() { answerMaxConns = old })
	placeOn(t, a, "srvA", appEntry("srvA"))
	fa.mu.Lock()
	fa.notReady["prj1"] = true
	fa.mu.Unlock()
	s := fa.spec(t, answerAlias)
	waitingFor(t, a.forwarders(), s, shopTLS, 1, 1)
	start := time.Now()
	if got := askPort(t, s, shopWeb); got != "" || time.Since(start) > time.Second {
		t.Fatalf("past the forwarder's cap a connection answered %q after %v", got, time.Since(start))
	}
}

func TestAnswers_readyOnlyWhenEveryContainerIs(t *testing.T) {
	at := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	checked := at.Add(30 * time.Minute)
	ts := func(t time.Time) string { return t.Format(time.RFC3339Nano) }
	plain := func(status string) string {
		return `{"status":"` + status + `","started":"` + ts(at) + `","health":null}`
	}
	health := func(status string, streak int, exits ...int) string {
		var log []string
		for i, e := range exits {
			log = append(log, fmt.Sprintf(`{"End":"%s","ExitCode":%d}`, ts(checked.Add(time.Duration(i)*time.Minute)), e))
		}
		return fmt.Sprintf(`{"status":"running","started":"%s","health":{"Status":"%s","FailingStreak":%d,"Log":[%s]}}`,
			ts(at), status, streak, strings.Join(log, ","))
	}
	stuck := `{"status":"created","started":"0001-01-01T00:00:00Z","created":"` + ts(at) + `","health":null}`
	// A routing change recreates the container: its replacement waits, created, under the old one's id and name.
	old := `{"id":"be6b0bf9408e5a1c","name":"/deplo-shop-web-1","status":"running","started":"` + ts(at) + `","health":null}`
	next := `{"id":"77aa","name":"/be6b0bf9408e_deplo-shop-web-1","status":"created","started":"0001-01-01T00:00:00Z","created":"` + ts(time.Now()) + `","health":null}`
	other := `{"id":"77aa","name":"/0123456789ab_deplo-shop-web-1","status":"created","started":"0001-01-01T00:00:00Z","created":"` + ts(time.Now()) + `","health":null}`
	for name, tc := range map[string]struct {
		lines []string
		ok    bool
		since time.Time
		// unknown: healthy, with no kept check to say since when.
		unknown bool
	}{
		"one without a health check":                  {[]string{plain("running")}, true, at, false},
		"healthy since its oldest kept check":         {[]string{health("healthy", 0, 0, 0, 0)}, true, checked, false},
		"healthy after a failed check":                {[]string{health("healthy", 0, 1, 0, 0)}, true, time.Time{}, true},
		"a healthy one beside a starting one":         {[]string{health("healthy", 0, 0), health("starting", 0)}, false, time.Time{}, false},
		"no health check beside an unhealthy one":     {[]string{plain("running"), health("unhealthy", 3, 1, 1, 1)}, false, time.Time{}, false},
		"a job that ran beside a healthy one":         {[]string{plain("exited"), health("healthy", 0, 0)}, true, checked, false},
		"one restarting":                              {[]string{health("healthy", 0, 0), plain("restarting")}, false, time.Time{}, false},
		"one still being created":                     {[]string{plain("created")}, false, time.Time{}, false},
		"the one running while its replacement waits": {[]string{old, next}, true, at, false},
		"a waiting copy of another container":         {[]string{old, other}, false, time.Time{}, false},
		"broken: one never started":                   {[]string{plain("running"), stuck}, false, time.Time{}, false},
		"broken: unhealthy beside one just created":   {[]string{plain("created"), health("unhealthy", 3, 1, 1, 1)}, false, time.Time{}, false},
		"nothing running":                             {[]string{plain("exited")}, false, time.Time{}, false},
		"a line docker did not write":                 {[]string{"running " + ts(at)}, false, time.Time{}, false},
	} {
		r := readinessOf(tc.lines)
		if r.OK != tc.ok || (r.OK && (!r.Since.Equal(tc.since) || r.Health != tc.unknown)) {
			t.Errorf("%s: %+v", name, r)
		}
		if want := strings.HasPrefix(name, "broken") || strings.Contains(name, "unhealthy"); r.Broken != want {
			t.Errorf("%s: broken %v, want %v", name, r.Broken, want)
		}
	}
}

// Without a health check an App counts as ready only once its last container has run answerSettle.
func TestAnswers_withoutAHealthCheckReadyAfterItSettles(t *testing.T) {
	a, _, fa, _, _ := answerPair(t)
	f := a.forwarders()
	started := time.Now()
	_ = fa
	f.s.fwdOps.ready = func(context.Context, string) appReadiness { return appReadiness{OK: true, Since: started} }
	old, oldRetry := answerSettle, answerRetry
	answerSettle, answerRetry = 300*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { answerSettle, answerRetry = old, oldRetry })
	if f.ready(context.Background(), "prj1") {
		t.Fatal("ready the moment it started")
	}
	eventually(t, "ready once settled", func() bool { return f.ready(context.Background(), "prj1") })
	if time.Since(started) < 300*time.Millisecond {
		t.Fatalf("ready after %v", time.Since(started))
	}
}

func TestAnswers_rewriteTheRoutesFileInPlace(t *testing.T) {
	path := filepath.Join(t.TempDir(), answerRoutesFile)
	long, short := []byte(`{"tcp":{"routers":{"a":{},"b":{}}}}`+"\n"), []byte("{}\n")
	if err := rewriteInPlace(path, long); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	for _, want := range [][]byte{short, long, short} {
		if err := rewriteInPlace(path, want); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(path)
		after, _ := os.Stat(path)
		if string(got) != string(want) || !os.SameFile(before, after) {
			t.Fatalf("the file holds %q (same file: %v), want %q", got, os.SameFile(before, after), want)
		}
	}
}

// A cluster's cap is held to a quarter of this server's memory, at 64 KB a connection.
func TestAnswers_aServersMemoryHoldsTheClustersCap(t *testing.T) {
	old := answerHostMemory
	t.Cleanup(func() { answerHostMemory = old })
	for name, tc := range map[string]struct {
		memory int64
		cap    uint32
		conns  int
		bound  bool
	}{
		"512 MB, the default":   {512 << 20, 0, 2048, false},
		"256 MB, the default":   {256 << 20, 0, 1024, true},
		"512 MB, a cap of 8192": {512 << 20, 8192, 2048, true},
		"16 GB, a cap of 8192":  {16 << 30, 8192, 8192, false},
		"memory unknown, 8192":  {0, 8192, 8192, false},
	} {
		answerHostMemory = func() int64 { return tc.memory }
		f := &forwarders{st: forwardState{AnswerMaxConns: tc.cap}}
		conns, waiting, _, bound := f.answerCaps()
		if conns != tc.conns || waiting != tc.conns/4 || bound != tc.bound {
			t.Errorf("%s: %d connections, %d waiting, held by memory %v", name, conns, waiting, bound)
		}
	}
}

// c38: eu-2's proxy was recreated with fise and memos passed on to it from three members; every waiting connection
// dialed it again at each look and logged the refusal (3036 lines in 25 s), the agent took 70 to 88% of the core, and
// its lease renewals came 5.3 s apart. One refusal now holds for proxyDownFor, a probe included.
func TestAnswers_aProxyThatRefusesIsDialedOncePerHoldNotPerConnection(t *testing.T) {
	quickAnswers(t, 600*time.Millisecond)
	was := proxyDownFor
	proxyDownFor = 10 * time.Second
	t.Cleanup(func() { proxyDownFor = was })
	a, b, fa, _, members := answerPair(t)
	var dials atomic.Int32
	dial := b.fwdOps.proxyDial
	b.fwdOps.proxyDial = func(ctx context.Context, port uint32) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, port)
	}
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := uint32(closed.Addr().(*net.TCPAddr).Port)
	closed.Close()
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: backend(t, "proxy-a"),
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: down,
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	s := fa.spec(t, answerAlias)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := askPort(t, s, shopTLS); got != "" {
				t.Errorf("a visitor reached %q through a proxy that refuses", got)
			}
		}()
	}
	wg.Wait()
	// Those already dialing when the first refusal came may dial too; without the hold it was every one, at every look.
	if n := dials.Load(); n > 3 {
		t.Fatalf("B's refusing proxy was dialed %d times for 20 visitors in one hold", n)
	}
}

// holdingBackend is a proxy that keeps every connection open: it names itself, then echoes what it reads in capitals.
func holdingBackend(t *testing.T, name string) uint32 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.WriteString(c, name+":")
				buf := make([]byte, 64)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					_, _ = c.Write([]byte(strings.ToUpper(string(buf[:n]))))
				}
			}()
		}
	}()
	return uint32(ln.Addr().(*net.TCPAddr).Port)
}

// c39: a visitor's connection kept open across a move still reached the server the App left, and got its 404.
func TestAnswers_aConnectionKeptOpenIsClosedOnceTheAppRunsElsewhere(t *testing.T) {
	quickAnswers(t, 2*time.Second)
	was := localCheck
	localCheck = 20 * time.Millisecond
	t.Cleanup(func() { localCheck = was })
	a, b, fa, _, members := answerPair(t)
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: backend(t, "proxy-a"),
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: holdingBackend(t, "proxy-b"),
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	c, err := net.DialTimeout("unix", filepath.Join(fa.spec(t, answerAlias).Dir, fwdSocket), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(c, "PORT "+strconv.Itoa(int(shopTLS))+"\nping")
	got := make([]byte, len("proxy-b:PING"))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "proxy-b:PING" {
		t.Fatalf("the visitor read %q (%v)", got, err)
	}
	placeOn(t, b, "srvB", appEntry("srvA"))
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(c); err != nil {
		t.Fatalf("the connection to the server the App left stayed open: %v", err)
	}
}

// c39: a recreated proxy answered 404 until it had read its routes; c42: a visitor held through another server met it
// before. Its visitors wait from the moment it stops until it settled.
func TestAnswers_aRestartedProxyGetsNoVisitorUntilItSettles(t *testing.T) {
	a, _, _, _, _ := answerPair(t)
	f := a.forwarders()
	f.s.fwdOps.ready = func(context.Context, string) appReadiness {
		return appReadiness{OK: true, Since: time.Now().Add(-time.Hour)}
	}
	old, oldRetry := proxySettle, answerRetry
	proxySettle, answerRetry = 300*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { proxySettle, answerRetry = old, oldRetry })
	if !f.ready(context.Background(), "prj1") {
		t.Fatal("an App that ran for an hour is not ready")
	}
	f.proxyStopping(nil)
	if f.ready(context.Background(), "prj1") {
		t.Fatal("ready while its proxy restarts")
	}
	f.proxyRestarted()
	if f.ready(context.Background(), "prj1") {
		t.Fatal("ready the moment its proxy restarted")
	}
	eventually(t, "ready once the proxy settled", func() bool { return f.ready(context.Background(), "prj1") })
}

// c42: an App on one server waited the whole restart of that server's proxy. A standby proxy takes its visitors:
// the App stays ready and is reached through the standby's own answer entrypoint until the new proxy settled.
func TestAnswers_anAppOnOneServerIsAnsweredByTheStandbyWhileItsProxyRestarts(t *testing.T) {
	a, _, _, _, _ := answerPair(t)
	f := a.forwarders()
	f.s.fwdOps.ready = func(context.Context, string) appReadiness {
		return appReadiness{OK: true, Since: time.Now().Add(-time.Hour)}
	}
	f.mu.Lock()
	f.st.AnswerPort = 8443
	f.mu.Unlock()
	var dialed atomic.Uint32
	f.proxyStopping(func(_ context.Context, port uint32) (net.Conn, error) {
		dialed.Store(port)
		c, _ := net.Pipe()
		return c, nil
	})
	if !f.ready(context.Background(), "prj1") {
		t.Fatal("not ready while its proxy restarts, though a standby proxy answers")
	}
	c, err := f.dialProxy(context.Background(), answerTLS)
	if err != nil || dialed.Load() != 8443 {
		t.Fatalf("dialed %d (%v), want the standby's answer entrypoint", dialed.Load(), err)
	}
	c.Close()
	f.proxyRestarted()
	f.standbyGone()
	if f.ready(context.Background(), "prj1") {
		t.Fatal("ready the moment its proxy restarted, with no standby left")
	}
}

// c41: at the thaw of a 2-minute hang, what a kept connection had queued reached the stale copy before the agent's
// once-a-second look closed it. Each read passed on asks the lease first.
func TestAnswers_aCarriedVisitorPassesNothingOnceTheLeaseRanOut(t *testing.T) {
	quickAnswers(t, 2*time.Second)
	a, b, fa, _, members := answerPair(t)
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: backend(t, "proxy-a"),
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, AnswerPort: holdingBackend(t, "proxy-b"),
		Answers: []*pb.ClusterAnswer{shop()}, ServeAnswers: []string{"prj1"}})
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	c, err := net.DialTimeout("unix", filepath.Join(fa.spec(t, answerAlias).Dir, fwdSocket), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(c, "PORT "+strconv.Itoa(int(shopTLS))+"\nping")
	got := make([]byte, len("proxy-b:PING"))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "proxy-b:PING" {
		t.Fatalf("the visitor read %q (%v)", got, err)
	}
	g := b.leaseGuard()
	g.mu.Lock()
	g.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	g.mu.Unlock()
	_, _ = io.WriteString(c, "pong")
	_ = c.SetDeadline(time.Now().Add(500 * time.Millisecond))
	if rest, _ := io.ReadAll(c); strings.Contains(string(rest), "PONG") {
		t.Fatalf("a server whose lease ran out passed the visitor on to its copy: %q", rest)
	}
}
