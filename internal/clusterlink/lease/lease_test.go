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
