//go:build linux

package server

import (
	"fmt"
	"os"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

const fwdTable = "deplo_forward"

// redirectAllPorts sends every TCP connection that reaches a forwarder container to its one listener, from
// inside that container's own network: the container itself holds no privilege to change it.
func redirectAllPorts(pid int, port uint32) error {
	if pid <= 0 || port == 0 || port > 65535 {
		return fmt.Errorf("no forwarder process or port")
	}
	ns, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil {
		return err
	}
	defer ns.Close()
	c, err := nftables.New(nftables.WithNetNSFd(int(ns.Fd())))
	if err != nil {
		return err
	}
	if tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4); err == nil {
		for _, t := range tables {
			if t.Name == fwdTable {
				c.DelTable(t)
			}
		}
	}
	t := c.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: fwdTable})
	ch := c.AddChain(&nftables.Chain{Name: "prerouting", Table: t, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityNATDest})
	c.AddRule(&nftables.Rule{Table: t, Chain: ch, Exprs: []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.IPPROTO_TCP}},
		&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(port))},
		&expr.Redir{RegisterProtoMin: 1},
	}})
	return c.Flush()
}
