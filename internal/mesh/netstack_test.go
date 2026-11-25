package mesh

import (
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// A packet still on its way out of the netstack when it closes is dropped, never a send on a closed channel.
func TestNetstack_closingWhileItsOwnSideStillWrites(t *testing.T) {
	addr := netip.MustParseAddr("fdde:9100::a")
	to := net.UDPAddrFromAddrPort(netip.MustParseAddrPort("[fdde:9100::b]:9"))
	for round := 0; round < 50; round++ {
		dev, err := newNetTUN(addr, MTU)
		if err != nil {
			t.Fatal(err)
		}
		c, err := dev.ListenUDPAddrPort(netip.AddrPortFrom(addr, 9))
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		// A slow reader, as WireGuard is under load: writers wait on it with a packet in hand.
		wg.Add(1)
		go func() {
			defer wg.Done()
			bufs, sizes := [][]byte{make([]byte, MTU+64)}, []int{0}
			for {
				if _, err := dev.Read(bufs, sizes, 0); err != nil {
					return
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if _, err := c.WriteTo([]byte("alive"), to); err != nil {
						return
					}
				}
			}()
		}
		time.Sleep(time.Duration(1+round%5) * time.Millisecond)
		dev.Close()
		c.Close()
		wg.Wait()
	}
}
