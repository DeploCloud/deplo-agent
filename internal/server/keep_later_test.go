package server

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// eu, 24 Nov 2026: with the lab's disk busy, a state file's sync took seconds under the locks the cluster's answers,
// forwarders and restarts wait on. A file that hangs (a pipe nobody reads) holds no save up, and the next save lands
// once the disk takes writes again.
func TestKeepLater_aStateFileThatHangsHoldsNoSaveUp(t *testing.T) {
	stateWritesInline = false
	t.Cleanup(func() { stateWritesInline = true })
	dir := t.TempDir()
	file := filepath.Join(dir, "cluster-zone.json")
	if err := syscall.Mkfifo(file+".tmp", 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			keepLater(file, []byte{byte('0' + i%10)})
		}
		keepLater(file, []byte("newest"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a save waited on its file")
	}
	r, err := os.OpenFile(file+".tmp", os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	r.Close()
	laterWork.Wait()
	keepLater(file, []byte("newest"))
	laterWork.Wait()
	if b, _ := os.ReadFile(file); string(b) != "newest" {
		t.Fatalf("the file holds %q", b)
	}
}
