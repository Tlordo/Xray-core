package splithttp

import (
	"bufio"
	"net"
	"time"
)

// A pooled upload connection is written to without reading the responses, so a request sent
// after the server closed the connection is lost without an error. Servers close a keep-alive
// connection that was idle (Yandex Cloud CDN: 15 s) or served enough requests (1000), so a
// connection is retired well before either.
const (
	h1MaxIdle     = 8 * time.Second
	h1MaxRequests = 400
	// h1RetireDelay lets the requests already written to a retired connection get through.
	h1RetireDelay = 10 * time.Second
)

type H1Conn struct {
	UnreadedResponsesCount int
	RespBufReader          *bufio.Reader
	net.Conn

	lastUsed time.Time
	requests int
}

// stale reports whether the server may have closed the connection by now.
func (c *H1Conn) stale(now time.Time) bool {
	return c.requests >= h1MaxRequests || (c.requests > 0 && now.Sub(c.lastUsed) > h1MaxIdle)
}

// retire closes the connection once its last requests had time to arrive.
func (c *H1Conn) retire() {
	time.AfterFunc(h1RetireDelay, func() { c.Close() })
}

func NewH1Conn(conn net.Conn) *H1Conn {
	return &H1Conn{
		RespBufReader: bufio.NewReader(conn),
		Conn:          conn,
	}
}
