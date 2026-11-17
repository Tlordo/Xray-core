package splithttp

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Uploads over HTTP/1.1 are pipelined: requests are written into a pooled connection one after
// another without waiting for their responses. Each connection therefore has a reader of its own
// that drains the responses, so they never pile up in the socket and stall the server or CDN, and
// that marks the connection dead as soon as the other side closes it or answers with an error.
//
// Servers also close a keep-alive connection that was idle (Yandex Cloud CDN: 15 s) or served
// enough requests (1000), so a connection is retired well before either.
const (
	h1MaxIdle     = 8 * time.Second
	h1MaxRequests = 400
	// h1RetireDelay lets the requests already written to a retired connection get through.
	h1RetireDelay = 10 * time.Second
	// h1WriteTimeout bounds one request write: a connection that takes longer is stuck.
	h1WriteTimeout = 10 * time.Second
)

type H1Conn struct {
	RespBufReader *bufio.Reader
	net.Conn

	lastUsed time.Time
	requests int
	dead     atomic.Bool
}

// stale reports whether the connection is closed or the server may have closed it by now.
func (c *H1Conn) stale(now time.Time) bool {
	return c.dead.Load() || c.requests >= h1MaxRequests || (c.requests > 0 && now.Sub(c.lastUsed) > h1MaxIdle)
}

// retire closes the connection once its last requests had time to arrive.
func (c *H1Conn) retire() {
	time.AfterFunc(h1RetireDelay, func() { c.Close() })
}

// write sends one whole request, giving up (and closing the connection) after h1WriteTimeout.
func (c *H1Conn) write(b []byte) error {
	c.SetWriteDeadline(time.Now().Add(h1WriteTimeout))
	_, err := c.Write(b)
	if err != nil {
		c.Close()
		return err
	}
	c.SetWriteDeadline(time.Time{})
	c.lastUsed = time.Now()
	c.requests++
	return nil
}

func (c *H1Conn) Close() error {
	c.dead.Store(true)
	return c.Conn.Close()
}

// drainResponses reads and drops the responses until the connection ends or a response says
// it should not be used any more; the connection is closed then.
func (c *H1Conn) drainResponses() {
	defer c.Close()
	for {
		resp, err := http.ReadResponse(c.RespBufReader, nil)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Close {
			return
		}
	}
}

func NewH1Conn(conn net.Conn) *H1Conn {
	c := &H1Conn{
		RespBufReader: bufio.NewReader(conn),
		Conn:          conn,
	}
	go c.drainResponses()
	return c
}
