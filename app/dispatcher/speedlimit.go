package dispatcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/policy"
)

const (
	// bytesPerMbps is the number of bytes per second in one megabit per second.
	bytesPerMbps = 1000 * 1000 / 8
	// minSpeedBurst is the smallest token bucket size, in bytes.
	minSpeedBurst = 64 * 1024
	// speedChunk is how many bytes are released to the pipe after one wait.
	// It must stay well below minSpeedBurst.
	speedChunk = 32 * 1024
	// speedIdleTTL is how long a user without any limited traffic keeps its limiter.
	speedIdleTTL = 10 * time.Minute
	// speedSweepEvery is how often idle limiters are looked for.
	speedSweepEvery = time.Minute
)

// speedLimiters is the set of per-user limiters. All connections of one user
// (identified by email) share the same pair of token buckets, so the limit is
// per user and not per connection.
var speedLimiters = newSpeedRegistry()

// speedDirection is the token bucket of one direction of one user.
type speedDirection struct {
	mu      sync.Mutex
	bps     atomic.Int64                 // bytes per second, 0 means unlimited
	limiter atomic.Pointer[rate.Limiter] // created with the first limit
}

func newSpeedDirection() *speedDirection {
	return &speedDirection{}
}

// burstFor returns the bucket size for a rate: about one second of traffic, at least minSpeedBurst.
func burstFor(bps int64) int {
	if bps < minSpeedBurst {
		return minSpeedBurst
	}
	if bps > 1<<30 {
		return 1 << 30
	}
	return int(bps)
}

// set changes the rate of the bucket; 0 switches the limit off.
func (d *speedDirection) set(bps int64) {
	if d.bps.Load() == bps {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.bps.Load() == bps {
		return
	}
	if bps > 0 {
		if l := d.limiter.Load(); l != nil {
			l.SetBurst(burstFor(bps))
			l.SetLimit(rate.Limit(bps))
		} else {
			// A new bucket starts full, so a fresh user gets a whole burst.
			d.limiter.Store(rate.NewLimiter(rate.Limit(bps), burstFor(bps)))
		}
	}
	d.bps.Store(bps)
}

func (d *speedDirection) limited() bool {
	return d.bps.Load() > 0
}

// wait blocks until n bytes may pass, or ctx is done.
func (d *speedDirection) wait(ctx context.Context, n int) error {
	l := d.limiter.Load()
	if l == nil {
		return nil
	}
	for n > 0 {
		step := min(n, l.Burst())
		if err := l.WaitN(ctx, step); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The burst may have been lowered between Burst() and WaitN().
			if l.Burst() < step {
				continue
			}
			return err
		}
		n -= step
	}
	return nil
}

// userSpeed holds the limiters of one user.
type userSpeed struct {
	uplink   *speedDirection
	downlink *speedDirection
	lastUsed atomic.Int64 // unix nanoseconds
	dead     atomic.Bool  // removed from the registry, holders must acquire again
}

func (u *userSpeed) touch(now time.Time) {
	u.lastUsed.Store(now.UnixNano())
}

func (u *userSpeed) apply(s policy.Speed) {
	u.uplink.set(int64(s.UplinkMbps) * bytesPerMbps)
	u.downlink.set(int64(s.DownlinkMbps) * bytesPerMbps)
}

type speedRegistry struct {
	mu        sync.Mutex
	users     map[string]*userSpeed
	lastSweep time.Time
	now       func() time.Time
}

func newSpeedRegistry() *speedRegistry {
	return &speedRegistry{users: make(map[string]*userSpeed), now: time.Now}
}

// acquire returns the limiters of the user, creating them when needed, and sets
// their rates from the level policy of the connection that is being created.
// The newest connection wins: after a user's level changes, the next new
// connection moves the rate of all the user's live connections too.
func (r *speedRegistry) acquire(email string, speed policy.Speed) *userSpeed {
	now := r.now()
	r.mu.Lock()
	if now.Sub(r.lastSweep) >= speedSweepEvery {
		r.sweepLocked(now)
	}
	u := r.users[email]
	if u == nil {
		u = &userSpeed{uplink: newSpeedDirection(), downlink: newSpeedDirection()}
		r.users[email] = u
	}
	u.touch(now)
	r.mu.Unlock()
	u.apply(speed)
	return u
}

// reacquire is acquire for a connection whose limiters were swept while it was
// idle: a missing user is created from the connection's own rates, but the rates
// of an existing one are left alone because a newer connection may have set them.
func (r *speedRegistry) reacquire(email string, speed policy.Speed) *userSpeed {
	now := r.now()
	r.mu.Lock()
	u := r.users[email]
	if u == nil {
		u = &userSpeed{uplink: newSpeedDirection(), downlink: newSpeedDirection()}
		u.apply(speed)
		r.users[email] = u
	}
	u.touch(now)
	r.mu.Unlock()
	return u
}

// sweepLocked forgets the users that have not moved limited traffic for speedIdleTTL.
func (r *speedRegistry) sweepLocked(now time.Time) {
	r.lastSweep = now
	for email, u := range r.users {
		if now.Sub(time.Unix(0, u.lastUsed.Load())) > speedIdleTTL {
			u.dead.Store(true)
			delete(r.users, email)
		}
	}
}

func (r *speedRegistry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.users)
}

// speedHandle is what a connection holds to find the limiters of its user.
type speedHandle struct {
	ctx      context.Context
	registry *speedRegistry
	email    string
	speed    policy.Speed // rates this connection was created with
	user     *userSpeed
}

func newSpeedHandle(ctx context.Context, registry *speedRegistry, email string, speed policy.Speed) speedHandle {
	return speedHandle{ctx: ctx, registry: registry, email: email, speed: speed, user: registry.acquire(email, speed)}
}

func (h *speedHandle) direction(uplink bool) *speedDirection {
	if h.user.dead.Load() {
		// The limiters were swept while this connection was idle.
		h.user = h.registry.reacquire(h.email, h.speed)
	}
	if uplink {
		return h.user.uplink
	}
	return h.user.downlink
}

// speedWriter throttles a buf.Writer with the shared token bucket of a user.
type speedWriter struct {
	speedHandle
	uplink bool
	Writer buf.Writer
}

func newSpeedWriter(ctx context.Context, email string, speed policy.Speed, uplink bool, w buf.Writer) *speedWriter {
	return &speedWriter{speedHandle: newSpeedHandle(ctx, speedLimiters, email, speed), uplink: uplink, Writer: w}
}

// Limited reports whether the writer currently applies a rate limit.
func (w *speedWriter) Limited() bool {
	return w.direction(w.uplink).limited()
}

// WriteMultiBuffer implements buf.Writer.
func (w *speedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	d := w.direction(w.uplink)
	w.user.touch(w.registry.now())
	if !d.limited() {
		return w.Writer.WriteMultiBuffer(mb)
	}
	for !mb.IsEmpty() {
		var chunk buf.MultiBuffer
		var size int32
		for len(mb) > 0 && size < speedChunk {
			b := mb[0]
			mb[0] = nil
			mb = mb[1:]
			chunk = append(chunk, b)
			size += b.Len()
		}
		if err := d.wait(w.ctx, int(size)); err != nil {
			buf.ReleaseMulti(chunk)
			buf.ReleaseMulti(mb)
			return err
		}
		if err := w.Writer.WriteMultiBuffer(chunk); err != nil {
			buf.ReleaseMulti(mb)
			return err
		}
	}
	return nil
}

// Close implements common.Closable.
func (w *speedWriter) Close() error {
	return common.Close(w.Writer)
}

// Interrupt implements common.Interruptible.
func (w *speedWriter) Interrupt() {
	common.Interrupt(w.Writer)
}

// UnwrapSpeedWriter looks through the speed limiter. It returns the writer
// below it and whether a limit is active right now. Writers that are not
// limiters are returned as they are.
//
// Code that bypasses the writer chain (splice) must not do so while the limit is active.
func UnwrapSpeedWriter(w buf.Writer) (buf.Writer, bool) {
	if sw, ok := w.(*speedWriter); ok {
		return sw.Writer, sw.Limited()
	}
	return w, false
}

// speedReader throttles reads of a buf.TimeoutReader. It is used for links
// where the uplink is read by the dispatcher instead of written by it.
type speedReader struct {
	speedHandle
	Reader buf.TimeoutReader
}

func newSpeedReader(ctx context.Context, email string, speed policy.Speed, r buf.TimeoutReader) *speedReader {
	return &speedReader{speedHandle: newSpeedHandle(ctx, speedLimiters, email, speed), Reader: r}
}

func (r *speedReader) throttle(mb buf.MultiBuffer) {
	d := r.direction(true)
	r.user.touch(r.registry.now())
	if mb.IsEmpty() || !d.limited() {
		return
	}
	// A cancelled context ends the connection on its own; ignoring the error here is safe.
	_ = d.wait(r.ctx, int(mb.Len()))
}

// ReadMultiBuffer implements buf.Reader.
func (r *speedReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.Reader.ReadMultiBuffer()
	r.throttle(mb)
	return mb, err
}

// ReadMultiBufferTimeout implements buf.TimeoutReader.
func (r *speedReader) ReadMultiBufferTimeout(d time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.Reader.ReadMultiBufferTimeout(d)
	r.throttle(mb)
	return mb, err
}

// Interrupt implements common.Interruptible.
func (r *speedReader) Interrupt() {
	common.Interrupt(r.Reader)
}
