package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/infra/conf"
)

func TestBufferSize(t *testing.T) {
	cases := []struct {
		Input  int32
		Output int32
	}{
		{
			Input:  0,
			Output: 0,
		},
		{
			Input:  -1,
			Output: -1,
		},
		{
			Input:  1,
			Output: 1024,
		},
	}

	for _, c := range cases {
		bs := c.Input
		pConf := Policy{
			BufferSize: &bs,
		}
		p, err := pConf.Build()
		common.Must(err)
		if p.Buffer.Connection != c.Output {
			t.Error("expected buffer size ", c.Output, " but got ", p.Buffer.Connection)
		}
	}
}

func TestPolicySpeed(t *testing.T) {
	raw := `{
		"levels": {
			"0":  {"statsUserUplink": true, "statsUserDownlink": true},
			"50": {"statsUserUplink": true, "statsUserDownlink": true, "uplinkMbps": 50, "downlinkMbps": 60}
		}
	}`
	var pc PolicyConfig
	if err := json.Unmarshal([]byte(raw), &pc); err != nil {
		t.Fatal(err)
	}
	config, err := pc.Build()
	common.Must(err)

	if config.Level[0].Speed != nil {
		t.Error("a level without speed fields must not get a speed limit")
	}
	if !config.Level[0].Stats.UserUplink || !config.Level[50].Stats.UserDownlink {
		t.Error("stats flags were lost")
	}
	speed := config.Level[50].Speed
	if speed == nil || speed.UplinkMbps != 50 || speed.DownlinkMbps != 60 {
		t.Errorf("level 50 speed = %v", speed)
	}
}
