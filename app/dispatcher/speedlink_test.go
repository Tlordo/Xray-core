package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
)

// levelPolicy is a policy.Manager whose levels carry both stats and a speed limit.
type levelPolicy struct{ speed map[uint32]policy.Speed }

func (levelPolicy) Type() interface{} { return policy.ManagerType() }
func (levelPolicy) Start() error      { return nil }
func (levelPolicy) Close() error      { return nil }
func (levelPolicy) ForSystem() policy.System {
	return policy.System{}
}

func (p levelPolicy) ForLevel(level uint32) policy.Session {
	s := policy.SessionDefault()
	s.Stats.UserUplink = true
	s.Stats.UserDownlink = true
	s.Speed = p.speed[level]
	return s
}

// A dispatched link of a user with a tier is throttled, and the counters see what passes.
func TestGetLinkThrottlesUserAndKeepsStats(t *testing.T) {
	sm, err := stats.NewManager(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	d := &DefaultDispatcher{
		policy: levelPolicy{speed: map[uint32]policy.Speed{25: {UplinkMbps: 1, DownlinkMbps: 1}}},
		stats:  sm,
	}
	email := "link-test@example.com"
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		Source: net.TCPDestination(net.LocalHostIP, 1234),
		User:   &protocol.MemoryUser{Email: email, Level: 25},
	})

	inbound, outbound := d.getLink(ctx)

	const size = 250000 // 125000 fit in the burst, the rest takes about a second
	received := make(chan int64, 1)
	go func() {
		var n int64
		for n < size {
			mb, err := outbound.Reader.ReadMultiBuffer()
			if err != nil {
				break
			}
			n += int64(mb.Len())
			buf.ReleaseMulti(mb)
		}
		received <- n
	}()

	start := time.Now()
	if err := inbound.Writer.WriteMultiBuffer(payload(size)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 800*time.Millisecond || d > 3*time.Second {
		t.Errorf("uplink of %d bytes at 1 Mbit/s took %v, want about 1s", size, d)
	}
	if n := <-received; n != size {
		t.Errorf("received %d bytes, want %d", n, size)
	}
	c := sm.GetCounter("user>>>" + email + ">>>traffic>>>uplink")
	if c == nil || c.Value() != size {
		t.Errorf("uplink counter = %v, want %d", c, size)
	}
}

// A user whose level has no speed limit is not slowed down.
func TestGetLinkUnlimitedLevel(t *testing.T) {
	sm, err := stats.NewManager(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	d := &DefaultDispatcher{policy: levelPolicy{}, stats: sm}
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		Source: net.TCPDestination(net.LocalHostIP, 1234),
		User:   &protocol.MemoryUser{Email: "link-unlimited@example.com", Level: 0},
	})
	inbound, outbound := d.getLink(ctx)
	go func() {
		for {
			mb, err := outbound.Reader.ReadMultiBuffer()
			if err != nil {
				return
			}
			buf.ReleaseMulti(mb)
		}
	}()
	start := time.Now()
	if err := inbound.Writer.WriteMultiBuffer(payload(2 << 20)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("unlimited link took %v", d)
	}
	if _, limited := UnwrapSpeedWriter(inbound.Writer); limited {
		t.Error("unlimited link reports a limit")
	}
}
