package server

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Runs the mongo script against fake tools: mongosh answers as a replica set member or a standalone,
// the others print their arguments.
func runMongoScript(t *testing.T, script string, replicaSet bool) string {
	t.Helper()
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	exit := "3"
	if replicaSet {
		exit = "0"
	}
	write("mongosh", "exit "+exit)
	write("mongodump", `echo "mongodump $*"`)
	write("mongorestore", `echo "mongorestore $*"`)
	cmd := exec.Command("sh", "-c", script, "sh", "mydb", "admin")
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin", "MONGO_PW=s3cret"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestMongoDump_replicaSetCarriesTheOplog(t *testing.T) {
	got := runMongoScript(t, mongoShell("mongodump --archive", true), true)
	if !strings.Contains(got, "--archive --oplog") || strings.Contains(got, "--db") {
		t.Errorf("a replica set dumps everything with its oplog, got %q", got)
	}
}

func TestMongoDump_standaloneKeepsThePerDatabaseDump(t *testing.T) {
	got := runMongoScript(t, mongoShell("mongodump --archive", true), false)
	if strings.Contains(got, "--oplog") || !strings.Contains(got, "--db=mydb") {
		t.Errorf("a standalone cannot dump an oplog, got %q", got)
	}
}

func TestMongoRestore_replaysTheOplogAndSparesTheUsers(t *testing.T) {
	got := runMongoScript(t, mongoShell("mongorestore --archive --drop", true), true)
	for _, want := range []string{"--oplogReplay", "--nsExclude=admin.*", "--drop"} {
		if !strings.Contains(got, want) {
			t.Errorf("restore missing %s: %q", want, got)
		}
	}
	if strings.Contains(got, "--db") {
		t.Errorf("a full archive restores without --db: %q", got)
	}
}
