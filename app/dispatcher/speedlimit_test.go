package dispatcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/policy"
)

// sink counts and drops everything written to it.
type sink struct {
	n      atomic.Int64
	writes atomic.Int64
	err    error
}

func (s *sink) WriteMultiBuffer(mb buf.MultiBuffer) error {
	s.n.Add(int64(mb.Len()))
	s.writes.Add(1)
	buf.ReleaseMulti(mb)
	return s.err
}

func payload(n int) buf.MultiBuffer {
	var mb buf.MultiBuffer
	for n > 0 {
		b := buf.New()
		k := min(n, int(buf.Size))
		b.Extend(int32(k))
		mb = append(mb, b)
		n -= k
	}
	return mb
}

func testWriter(ctx context.Context, reg *speedRegistry, email string, s policy.Speed, uplink bool, w buf.Writer) *speedWriter {
	return &speedWriter{speedHandle: newSpeedHandle(ctx, reg, email, s), uplink: uplink, Writer: w}
}

func TestSpeedWriterUnlimited(t *testing.T) {
	reg := newSpeedRegistry()
	out := &sink{}
	w := testWriter(context.Background(), reg, "a@x", policy.Speed{}, true, out)
	start := time.Now()
	for i := 0; i < 50; i++ {
		if err := w.WriteMultiBuffer(payload(100 * 1024)); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("unlimited writer took %v", d)
	}
	if out.n.Load() != 50*100*1024 {
		t.Errorf("wrote %d bytes", out.n.Load())
	}
	if w.Limited() {
		t.Error("writer without a limit reports Limited")
	}
}

func TestSpeedWriterLimitsRate(t *testing.T) {
	t.Parallel()
	reg := newSpeedRegistry()
	out := &sink{}
	// 1 Mbit/s = 125000 B/s, the bucket holds one second of traffic.
	w := testWriter(context.Background(), reg, "a@x", policy.Speed{UplinkMbps: 1}, true, out)
	if !w.Limited() {
		t.Fatal("writer must report Limited")
	}
	start := time.Now()
	// 125000 pass at once, the other 125000 take a second.
	if err := w.WriteMultiBuffer(payload(250000)); err != nil {
		t.Fatal(err)
	}
	d := time.Since(start)
	if d < 800*time.Millisecond || d > 3*time.Second {
		t.Errorf("250000 bytes at 1 Mbit/s took %v, want about 1s", d)
	}
	if out.n.Load() != 250000 {
		t.Errorf("wrote %d bytes", out.n.Load())
	}
	if out.writes.Load() < 2 {
		t.Errorf("expected the buffer to be released in chunks, got %d writes", out.writes.Load())
	}
}

func TestSpeedWriterDirectionsAreIndependent(t *testing.T) {
	t.Parallel()
	reg := newSpeedRegistry()
	s := policy.Speed{UplinkMbps: 1, DownlinkMbps: 0}
	up := testWriter(context.Background(), reg, "a@x", s, true, &sink{})
	down := &sink{}
	dw := testWriter(context.Background(), reg, "a@x", s, false, down)
	start := time.Now()
	if err := dw.WriteMultiBuffer(payload(1 << 20)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Error("the unlimited direction was slowed down")
	}
	if !up.Limited() || dw.Limited() {
		t.Errorf("Limited: up=%v down=%v", up.Limited(), dw.Limited())
	}
}

// All connections of one user draw from one bucket.
func TestSpeedSharedByConnectionsOfUser(t *testing.T) {
	t.Parallel()
	reg := newSpeedRegistry()
	s := policy.Speed{UplinkMbps: 1}
	var wg sync.WaitGroup
	total := &sink{}
	start := time.Now()
	for i := 0; i < 4; i++ {
		w := testWriter(context.Background(), reg, "shared@x", s, true, total)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 4 x 62500 = 250000 bytes: half of it fits in the burst.
			if err := w.WriteMultiBuffer(payload(62500)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	d := time.Since(start)
	if d < 800*time.Millisecond || d > 3*time.Second {
		t.Errorf("4 connections of one user took %v, want about 1s", d)
	}
	if total.n.Load() != 250000 {
		t.Errorf("wrote %d bytes", total.n.Load())
	}
}

func TestSpeedNotSharedBetweenUsers(t *testing.T) {
	t.Parallel()
	reg := newSpeedRegistry()
	s := policy.Speed{UplinkMbps: 1}
	var wg sync.WaitGroup
	start := time.Now()
	for _, email := range []string{"u1@x", "u2@x", "u3@x"} {
		w := testWriter(context.Background(), reg, email, s, true, &sink{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.WriteMultiBuffer(payload(100000)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("users slowed each other down: %v", d)
	}
}

func TestSpeedLevelChangeAppliesToLiveConnections(t *testing.T) {
	reg := newSpeedRegistry()
	old := testWriter(context.Background(), reg, "a@x", policy.Speed{}, true, &sink{})
	if old.Limited() {
		t.Fatal("must start unlimited")
	}
	// A new connection of the same user arrives with a tier.
	_ = testWriter(context.Background(), reg, "a@x", policy.Speed{UplinkMbps: 25, DownlinkMbps: 25}, true, &sink{})
	if !old.Limited() {
		t.Error("the live connection did not pick up the new limit")
	}
	if got := old.user.uplink.bps.Load(); got != 25*bytesPerMbps {
		t.Errorf("uplink rate %d, want %d", got, 25*bytesPerMbps)
	}
	if got := old.user.uplink.limiter.Load().Burst(); got != 25*bytesPerMbps {
		t.Errorf("burst %d, want one second of traffic", got)
	}
	// And back to unlimited.
	_ = testWriter(context.Background(), reg, "a@x", policy.Speed{}, true, &sink{})
	if old.Limited() {
		t.Error("the limit was not removed")
	}
}

func TestSpeedBurstHasFloor(t *testing.T) {
	if got := burstFor(1000); got != minSpeedBurst {
		t.Errorf("burstFor(1000) = %d", got)
	}
	if got := burstFor(3_125_000); got != 3_125_000 {
		t.Errorf("burstFor(25 Mbit/s) = %d", got)
	}
	if got := burstFor(1 << 40); got <= 0 {
		t.Errorf("burstFor(huge) = %d", got)
	}
}

func TestSpeedWriterContextCancelled(t *testing.T) {
	t.Parallel()
	reg := newSpeedRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	out := &sink{}
	w := testWriter(ctx, reg, "a@x", policy.Speed{UplinkMbps: 1}, true, out)
	done := make(chan error, 1)
	go func() { done <- w.WriteMultiBuffer(payload(10 << 20)) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected an error after the context is cancelled")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer is stuck after cancel")
	}
	if out.n.Load() >= 10<<20 {
		t.Error("everything was written despite the cancel")
	}
}

func TestSpeedWriterPropagatesWriteError(t *testing.T) {
	reg := newSpeedRegistry()
	boom := errors.New("boom")
	w := testWriter(context.Background(), reg, "a@x", policy.Speed{UplinkMbps: 100}, true, &sink{err: boom})
	if err := w.WriteMultiBuffer(payload(100000)); !errors.Is(err, boom) {
		t.Errorf("got %v", err)
	}
}

func TestSpeedRegistrySweepsIdleUsers(t *testing.T) {
	reg := newSpeedRegistry()
	now := time.Unix(1_000_000, 0)
	reg.now = func() time.Time { return now }

	reg.acquire("idle@x", policy.Speed{UplinkMbps: 50})
	w := testWriter(context.Background(), reg, "busy@x", policy.Speed{UplinkMbps: 50}, true, &sink{})
	if reg.size() != 2 {
		t.Fatalf("size %d", reg.size())
	}

	now = now.Add(speedIdleTTL / 2)
	_ = w.WriteMultiBuffer(payload(10)) // keeps busy@x alive
	now = now.Add(speedIdleTTL/2 + speedSweepEvery)
	_ = w.WriteMultiBuffer(payload(10))
	reg.acquire("new@x", policy.Speed{}) // triggers the sweep
	if reg.size() != 2 {
		t.Errorf("expected busy and new to stay, size %d", reg.size())
	}
	reg.mu.Lock()
	_, idle := reg.users["idle@x"]
	reg.mu.Unlock()
	if idle {
		t.Error("idle user was not evicted")
	}

	// A connection that outlived its limiter gets a new one with its own rates.
	now = now.Add(2 * speedIdleTTL)
	reg.acquire("trigger@x", policy.Speed{})
	if !w.user.dead.Load() {
		t.Fatal("busy user should have been evicted after a long silence")
	}
	if !w.Limited() {
		t.Error("evicted connection lost its limit")
	}
	if w.user.dead.Load() {
		t.Error("connection did not re-acquire")
	}
}

func TestSpeedReacquireKeepsNewerRates(t *testing.T) {
	reg := newSpeedRegistry()
	now := time.Unix(1_000_000, 0)
	reg.now = func() time.Time { return now }
	old := testWriter(context.Background(), reg, "a@x", policy.Speed{}, true, &sink{})
	now = now.Add(2 * speedIdleTTL)
	reg.acquire("other@x", policy.Speed{}) // evicts a@x
	// A newer connection of a@x arrives with a tier.
	_ = testWriter(context.Background(), reg, "a@x", policy.Speed{UplinkMbps: 50}, true, &sink{})
	if !old.Limited() {
		t.Error("old connection ignored the limit set by the newer one")
	}
}

func TestUnwrapSpeedWriter(t *testing.T) {
	reg := newSpeedRegistry()
	inner := &sink{}
	if w, limited := UnwrapSpeedWriter(inner); w != buf.Writer(inner) || limited {
		t.Error("a plain writer must be returned untouched")
	}
	sw := testWriter(context.Background(), reg, "a@x", policy.Speed{UplinkMbps: 10}, true, inner)
	w, limited := UnwrapSpeedWriter(sw)
	if w != buf.Writer(inner) || !limited {
		t.Errorf("unwrap = %v, %v", w, limited)
	}
	sw = testWriter(context.Background(), reg, "b@x", policy.Speed{}, true, inner)
	if _, limited := UnwrapSpeedWriter(sw); limited {
		t.Error("a user without a limit must not report one")
	}
}

type sliceReader struct{ chunks []int }

func (r *sliceReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if len(r.chunks) == 0 {
		return nil, errors.New("eof")
	}
	n := r.chunks[0]
	r.chunks = r.chunks[1:]
	return payload(n), nil
}

func (r *sliceReader) ReadMultiBufferTimeout(time.Duration) (buf.MultiBuffer, error) {
	return r.ReadMultiBuffer()
}

func TestSpeedReaderLimitsUplink(t *testing.T) {
	t.Parallel()
	reg := newSpeedRegistry()
	r := &speedReader{
		speedHandle: newSpeedHandle(context.Background(), reg, "a@x", policy.Speed{UplinkMbps: 1}),
		Reader:      &sliceReader{chunks: []int{125000, 125000}},
	}
	start := time.Now()
	for i := 0; i < 2; i++ {
		mb, err := r.ReadMultiBuffer()
		if err != nil {
			t.Fatal(err)
		}
		buf.ReleaseMulti(mb)
	}
	if d := time.Since(start); d < 800*time.Millisecond || d > 3*time.Second {
		t.Errorf("reading 250000 bytes at 1 Mbit/s took %v, want about 1s", d)
	}
}
