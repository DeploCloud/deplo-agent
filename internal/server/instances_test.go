package server

import "testing"

// The exposed flag drives which container the console and the log viewer default
// to. Flagging the whole stack made that default meaningless.
func TestIsExposed(t *testing.T) {
	// A real compose stack: the app, its database, its cache. Every container's
	// NAME contains "-activepieces-", because the compose project is named after
	// the slug — only the app's SERVICE is actually the exposed one.
	cases := []struct {
		service, expose string
		want            bool
	}{
		{"activepieces", "activepieces", true},
		{"postgres", "activepieces", false},
		{"redis", "activepieces", false},
		// Nothing is exposed when the app has no primary domain.
		{"activepieces", "", false},
		{"postgres", "", false},
	}
	for _, c := range cases {
		if got := isExposed(c.service, c.expose); got != c.want {
			t.Errorf("isExposed(%q, %q) = %v, want %v", c.service, c.expose, got, c.want)
		}
	}
}

func TestParseInspectLines(t *testing.T) {
	// Line 1 is the bug that broke the console: Config.User is empty (the image
	// declares no USER) while WorkingDir is /app. The old tab-separated parse
	// TrimSpace'd away the leading tab and read the workdir AS the user, so the
	// console exec'd `docker exec -u /app`.
	stdout := `{"name":"/deplo-core-neur1","user":"","workdir":"/app","openStdin":false,"tty":false,"state":"restarting","restartCount":88,"health":""}
{"name":"/deplo-shop-db-1","user":"postgres","workdir":"/var/lib/postgresql","openStdin":true,"tty":true,"state":"running","restartCount":0,"health":"healthy"}
{"name":"/deplo-shop-api-1","user":"","workdir":"","openStdin":false,"tty":false,"state":"exited","restartCount":3,"health":"unhealthy"}`

	got := parseInspectLines(stdout)
	if len(got) != 3 {
		t.Fatalf("parsed %d containers, want 3", len(got))
	}

	app := got["deplo-core-neur1"]
	if app.User != "root" || app.Workdir != "/app" {
		t.Errorf("empty USER must default to root and keep the workdir; got user=%q workdir=%q", app.User, app.Workdir)
	}
	if app.State != "restarting" || app.RestartCount != 88 {
		t.Errorf("crash loop must survive the parse: state=%q restarts=%d", app.State, app.RestartCount)
	}
	if app.Health != "" {
		t.Errorf("no healthcheck must stay empty, not be read as healthy; got %q", app.Health)
	}

	db := got["deplo-shop-db-1"]
	if db.User != "postgres" || db.Workdir != "/var/lib/postgresql" || !db.Tty || !db.OpenStdin {
		t.Errorf("db fields mangled: %+v", db)
	}
	if db.Health != "healthy" {
		t.Errorf("db health = %q, want healthy", db.Health)
	}

	api := got["deplo-shop-api-1"]
	if api.User != "root" || api.Workdir != "/" {
		t.Errorf("empty user+workdir must default to root and /; got user=%q workdir=%q", api.User, api.Workdir)
	}
	if api.State != "exited" || api.Health != "unhealthy" {
		t.Errorf("api state=%q health=%q", api.State, api.Health)
	}
}

// A container that disappears between the `ps` and the `inspect` leaves docker
// printing an error and a non-zero code — the surviving lines must still parse,
// and the missing one must simply be absent rather than shifting the others.
func TestParseInspectLinesSkipsGarbage(t *testing.T) {
	stdout := `{"name":"/deplo-a","user":"","workdir":"","openStdin":false,"tty":false,"state":"running","restartCount":0,"health":""}

not json at all
{"name":"","user":"x","workdir":"/","openStdin":false,"tty":false,"state":"running","restartCount":0,"health":""}`
	got := parseInspectLines(stdout)
	if len(got) != 1 {
		t.Fatalf("parsed %d containers, want only the valid one: %+v", len(got), got)
	}
	if _, ok := got["deplo-a"]; !ok {
		t.Errorf("the valid container is missing: %+v", got)
	}
}

func TestServiceOf(t *testing.T) {
	cases := []struct {
		slug, container, want string
	}{
		// Compose container: deplo-<slug>-<service>-N -> <service>.
		{"myapp", "deplo-myapp-web-1", "web"},
		{"myapp", "deplo-myapp-worker-2", "worker"},
		// Multi-word service with a replica index.
		{"myapp", "deplo-myapp-api-server-1", "api-server"},
		// Single-image deploy: the bare deplo-<slug> -> slug.
		{"myapp", "deplo-myapp", "myapp"},
		// A service whose name itself ends in a non-numeric segment.
		{"myapp", "deplo-myapp-db", "db"},
		// An unrelated container name (no deplo- prefix at all).
		{"myapp", "some-other", "some-other"},
	}
	for _, c := range cases {
		if got := serviceOf(c.slug, c.container); got != c.want {
			t.Errorf("serviceOf(%q,%q) = %q, want %q", c.slug, c.container, got, c.want)
		}
	}
}

func TestTrimTrailingReplicaIndex(t *testing.T) {
	cases := map[string]string{
		"web-1":        "web",
		"api-server-2": "api-server",
		"db":           "db",
		"web-":         "web-",    // trailing dash, no index
		"web-abc":      "web-abc", // non-numeric suffix
		"x-10":         "x",
	}
	for in, want := range cases {
		if got := trimTrailingReplicaIndex(in); got != want {
			t.Errorf("trimTrailingReplicaIndex(%q) = %q, want %q", in, got, want)
		}
	}
}
