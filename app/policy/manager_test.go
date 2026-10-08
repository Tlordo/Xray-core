package policy_test

import (
	"context"
	"testing"
	"time"

	. "github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/features/policy"
)

func TestPolicy(t *testing.T) {
	manager, err := New(context.Background(), &Config{
		Level: map[uint32]*Policy{
			0: {
				Timeout: &Policy_Timeout{
					Handshake: &Second{
						Value: 2,
					},
				},
			},
		},
	})
	common.Must(err)

	pDefault := policy.SessionDefault()

	{
		p := manager.ForLevel(0)
		if p.Timeouts.Handshake != 2*time.Second {
			t.Error("expect 2 sec timeout, but got ", p.Timeouts.Handshake)
		}
		if p.Timeouts.ConnectionIdle != pDefault.Timeouts.ConnectionIdle {
			t.Error("expect ", pDefault.Timeouts.ConnectionIdle, " sec timeout, but got ", p.Timeouts.ConnectionIdle)
		}
	}

	{
		p := manager.ForLevel(1)
		if p.Timeouts.Handshake != pDefault.Timeouts.Handshake {
			t.Error("expect ", pDefault.Timeouts.Handshake, " sec timeout, but got ", p.Timeouts.Handshake)
		}
	}
}

func TestPolicySpeed(t *testing.T) {
	manager, err := New(context.Background(), &Config{
		Level: map[uint32]*Policy{
			50: {Speed: &Policy_Speed{UplinkMbps: 50, DownlinkMbps: 40}},
			0:  {Stats: &Policy_Stats{UserUplink: true}},
		},
	})
	common.Must(err)

	if s := manager.ForLevel(50).Speed; s.UplinkMbps != 50 || s.DownlinkMbps != 40 {
		t.Errorf("level 50 speed = %+v", s)
	}
	if s := manager.ForLevel(0).Speed; s.UplinkMbps != 0 || s.DownlinkMbps != 0 {
		t.Errorf("level 0 must be unlimited, got %+v", s)
	}
	if s := manager.ForLevel(7).Speed; s.UplinkMbps != 0 || s.DownlinkMbps != 0 {
		t.Errorf("an unknown level must be unlimited, got %+v", s)
	}
	if !manager.ForLevel(0).Stats.UserUplink {
		t.Error("stats flags were lost")
	}
}
