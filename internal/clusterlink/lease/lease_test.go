package lease

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// The leader sends these exact names (deplo-cluster internal/lease); a rename here silently fences nothing.
func TestOrderIsTheLeadersWireShape(t *testing.T) {
	o, err := ReadOrder(strings.NewReader(`{"cluster":"clu_1","member":"srv_a","epoch":4,"placement":[{"kind":"app","id":"prj_1","replica":1,"member":"srv_a","slug":"web","singleWriter":true}]}` + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	e := o.Placement[0]
	if o.Cluster != "clu_1" || o.Member != "srv_a" || o.Epoch != 4 || e.Kind != "app" || e.ID != "prj_1" || e.Replica != 1 || e.Member != "srv_a" || e.Slug != "web" || !e.SingleWriter {
		t.Fatalf("%+v", o)
	}
}

func TestTimingAndReplyShape(t *testing.T) {
	if FenceAfter != 60*time.Second {
		t.Fatalf("fence after %v", FenceAfter)
	}
	var b bytes.Buffer
	if err := WriteReply(&b, Reply{Error: StaleEpoch, Epoch: 9}); err != nil || b.String() != `{"ok":false,"error":"stale_epoch","epoch":9}`+"\n" {
		t.Fatalf("%q %v", b.String(), err)
	}
}

func TestLeftNameIsTheFailoverTimeAndTheMember(t *testing.T) {
	at := time.Date(2026, 3, 1, 14, 32, 5, 0, time.FixedZone("cet", 3600))
	if got := LeftName(at, "srv_a", true); got != "-20260301T133205Z-srv_a.tar.gz.age" {
		t.Fatalf("encrypted: %q", got)
	}
	if got := LeftName(at, "srv_a", false); got != "-20260301T133205Z-srv_a.tar.gz" {
		t.Fatalf("plain: %q", got)
	}
}

// The leader sends these exact names (deplo-cluster internal/lease): a rename here saves nothing.
func TestAKeepAndAStoreAreTheLeadersWireShape(t *testing.T) {
	o, err := ReadOrder(strings.NewReader(`{"cluster":"clu_1","member":"srv_a","epoch":0,"placement":[],` +
		`"keep":[{"slug":"db-main","storeMember":"srv_b","storeFingerprint":"ab12","failedOverAt":"2026-03-01T13:32:05Z"}],` +
		`"stores":[{"from":"cd34","root":"/srv/backups","objectKey":"k"}]}` + "\n"))
	if err != nil || len(o.Keep) != 1 || len(o.Stores) != 1 {
		t.Fatalf("%+v (%v)", o, err)
	}
	if k := o.Keep[0]; k.StoreMember != "srv_b" || k.StoreFingerprint != "ab12" || k.FailedOverAt != "2026-03-01T13:32:05Z" {
		t.Fatalf("keep %+v", k)
	}
	if s := o.Stores[0]; s.From != "cd34" || s.Root != "/srv/backups" || s.ObjectKey != "k" {
		t.Fatalf("store %+v", s)
	}
	var b bytes.Buffer
	if err := WriteReply(&b, Reply{OK: true, Later: &Later{Saved: []Saved{{Slug: "db-main", ObjectKey: "k", SizeBytes: 7, At: "x"}}, Gone: []string{"db-main"}}}); err != nil ||
		b.String() != `{"ok":true,"later":{"saved":[{"slug":"db-main","objectKey":"k","sizeBytes":7,"at":"x"}],"gone":["db-main"]}}`+"\n" {
		t.Fatalf("%q %v", b.String(), err)
	}
}

func TestFenceSecondsAndStoppedWithinAreTheLeadersWireNames(t *testing.T) {
	o, err := ReadOrder(strings.NewReader(`{"cluster":"clu_1","epoch":4,"placement":[],"fenceSeconds":5}` + "\n"))
	if err != nil || o.FenceSeconds != 5 {
		t.Fatalf("%+v %v", o, err)
	}
	var b bytes.Buffer
	if err := WriteReply(&b, Reply{OK: true, StoppedWithin: 21}); err != nil || b.String() != `{"ok":true,"stoppedWithin":21}`+"\n" {
		t.Fatalf("%q %v", b.String(), err)
	}
}
