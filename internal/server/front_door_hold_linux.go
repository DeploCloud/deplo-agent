//go:build linux

package server

import (
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

const holdTable = "deplo_proxy_hold"

// redirectFrontDoor sends every new TCP connection to this server's own addresses on a front port to the agent's
// hold port, ahead of Docker's own redirect to the proxy.
func redirectFrontDoor(ports map[uint16]uint16) (func(), error) {
	c, err := nftables.New()
	if err != nil {
		return nil, err
	}
	dropHoldTable(c)
	t := c.AddTable(&nftables.Table{Family: nftables.TableFamilyINet, Name: holdTable})
	ch := c.AddChain(&nftables.Chain{Name: "prerouting", Table: t, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATDest - 1)})
	for from, to := range ports {
		c.AddRule(&nftables.Rule{Table: t, Chain: ch, Exprs: []expr.Any{
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.IPPROTO_TCP}},
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: binaryutil.BigEndian.PutUint16(from)},
			&expr.Fib{Register: 1, FlagDADDR: true, ResultADDRTYPE: true},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: binaryutil.NativeEndian.PutUint32(unix.RTN_LOCAL)},
			&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(to)},
			&expr.Redir{RegisterProtoMin: 1},
		}})
	}
	if err := c.Flush(); err != nil {
		return nil, err
	}
	return clearFrontDoorHold, nil
}

// clearFrontDoorHold ends a hold, also one an agent that stopped mid-restart left behind.
func clearFrontDoorHold() {
	if c, err := nftables.New(); err == nil {
		dropHoldTable(c)
		_ = c.Flush()
	}
}

func dropHoldTable(c *nftables.Conn) {
	if tables, err := c.ListTablesOfFamily(nftables.TableFamilyINet); err == nil {
		for _, t := range tables {
			if t.Name == holdTable {
				c.DelTable(t)
			}
		}
	}
}
