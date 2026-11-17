package splithttp

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

func TestFlushPadReaderDropsPadding(t *testing.T) {
	big := make([]byte, 3*flushPadMaxFrame+17)
	rand.Read(big)

	var wire []byte
	wire = append(wire, newFlushPadFrame()...)
	wire = appendFlushFrames(wire, []byte("hello"))
	wire = append(wire, newFlushPadFrame()...)
	wire = append(wire, newFlushPadFrame()...)
	wire = appendFlushFrames(wire, big)
	wire = appendFlushFrames(wire, nil)
	wire = append(wire, newFlushPadFrame()...)

	got, err := io.ReadAll(newFlushPadReader(io.NopCloser(bytes.NewReader(wire))))
	if err != nil {
		t.Fatal(err)
	}
	if want := append([]byte("hello"), big...); !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}

func TestFlushPadReaderReportsCutFrame(t *testing.T) {
	wire := appendFlushFrames(nil, []byte("hello"))
	_, err := io.ReadAll(newFlushPadReader(io.NopCloser(bytes.NewReader(wire[:len(wire)-2]))))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestServerConnPadsAfterQuietTime(t *testing.T) {
	rec := httptest.NewRecorder()
	conn := &httpServerConn{Instance: done.New(), ResponseWriter: rec, flushPad: 10 * time.Millisecond}

	if n, err := conn.Write([]byte("abc")); n != 3 || err != nil {
		t.Fatal(n, err)
	}
	conn.Lock()
	sent := rec.Body.Len()
	conn.Unlock()
	if want := 2 + 3; sent != want {
		t.Fatalf("right after the write: %d bytes on the wire, want %d", sent, want)
	}

	time.Sleep(200 * time.Millisecond) // both padding frames: 10 ms and 60 ms after the write
	conn.Lock()
	wire := bytes.Clone(rec.Body.Bytes())
	conn.Unlock()
	if len(wire) < sent+2*(2+16) {
		t.Fatalf("%d bytes on the wire after the quiet time, want two padding frames after %d", len(wire), sent)
	}
	got, err := io.ReadAll(newFlushPadReader(io.NopCloser(bytes.NewReader(wire))))
	if err != nil || string(got) != "abc" {
		t.Fatalf("got %q, %v", got, err)
	}

	// No more padding until new data, and none after Close.
	time.Sleep(100 * time.Millisecond)
	conn.Lock()
	if rec.Body.Len() != len(wire) {
		t.Fatalf("padding kept coming: %d bytes, was %d", rec.Body.Len(), len(wire))
	}
	conn.Unlock()
	conn.Write([]byte("d"))
	conn.Close()
	time.Sleep(100 * time.Millisecond)
	conn.Lock()
	defer conn.Unlock()
	if want := len(wire) + 2 + 1; rec.Body.Len() != want {
		t.Fatalf("after Close: %d bytes on the wire, want %d", rec.Body.Len(), want)
	}
}

func TestServerConnUnframedWithoutRequest(t *testing.T) {
	rec := httptest.NewRecorder()
	conn := &httpServerConn{Instance: done.New(), ResponseWriter: rec}
	conn.Write([]byte("abc"))
	time.Sleep(50 * time.Millisecond)
	if rec.Body.String() != "abc" {
		t.Fatalf("got %q", rec.Body.String())
	}
}

func TestFlushPadHeaders(t *testing.T) {
	c := &Config{Headers: map[string]string{"x-flush-pad": "40", "X-Other": "1"}}

	if h := c.GetRequestHeader(); h.Get(FlushPadConfigHeader) != "" || h.Get("X-Other") != "1" {
		t.Fatalf("config header leaked into the request: %v", h)
	}

	down, _ := http.NewRequest("GET", "http://example.com/path/session", nil)
	c.FillStreamRequest(down, "session", "")
	if got := down.Header.Get(FlushPadRequestHeader); got != "40" {
		t.Fatalf("download request asks for %q, want 40", got)
	}
	if d, ok := wantsFlushPad(down); !ok || d != 40*time.Millisecond {
		t.Fatal(d, ok)
	}

	up, _ := http.NewRequest("POST", "http://example.com/path/session", bytes.NewReader(nil))
	c.FillStreamRequest(up, "session", "")
	if up.Header.Get(FlushPadRequestHeader) != "" {
		t.Fatal("upload request asks for padding")
	}

	plain, _ := http.NewRequest("GET", "http://example.com/path/session", nil)
	(&Config{}).FillStreamRequest(plain, "session", "")
	if _, ok := wantsFlushPad(plain); ok {
		t.Fatal("a client without the setting asks for padding")
	}
}

func TestH1ConnStale(t *testing.T) {
	now := time.Now()
	dead := &H1Conn{}
	dead.dead.Store(true)
	for _, tc := range []struct {
		name string
		conn *H1Conn
		want bool
	}{
		{"new", &H1Conn{}, false},
		{"just used", &H1Conn{requests: 1, lastUsed: now.Add(-time.Second)}, false},
		{"idle", &H1Conn{requests: 1, lastUsed: now.Add(-h1MaxIdle - time.Second)}, true},
		{"many requests", &H1Conn{requests: h1MaxRequests, lastUsed: now}, true},
		{"closed by the other side", dead, true},
	} {
		if got := tc.conn.stale(now); got != tc.want {
			t.Errorf("%s: stale = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// h1Server answers every request on one TCP connection with a padded 200 (like an XHTTP server
// behind a CDN), closes it after closeAfter requests (0: never), and reports what it served.
func h1Server(t *testing.T, closeAfter int) (addr string, served chan int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	served = make(chan int, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		br := bufio.NewReader(c)
		n := 0
		for closeAfter == 0 || n < closeAfter {
			req, err := http.ReadRequest(br)
			if err != nil {
				break
			}
			io.Copy(io.Discard, req.Body)
			n++
			resp := "HTTP/1.1 200 OK\r\nX-Padding: " + strings.Repeat("X", 1000) + "\r\nContent-Length: 0\r\n\r\n"
			if _, err := c.Write([]byte(resp)); err != nil {
				break
			}
		}
		served <- n
	}()
	return ln.Addr().String(), served
}

func pipelinedRequest(t *testing.T) []byte {
	req, _ := http.NewRequest("GET", "http://example.com/path/session/0", nil)
	var b bytes.Buffer
	req.Write(&b)
	return b.Bytes()
}

// Without reading the responses, a few thousand pipelined requests fill both socket buffers and
// the writes block for good; the connection's own reader keeps them flowing.
func TestH1ConnDrainsPipelinedResponses(t *testing.T) {
	addr, _ := h1Server(t, 0)
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := NewH1Conn(raw)
	defer c.Close()
	req := pipelinedRequest(t)
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; i < 5000; i++ {
		if err := c.write(req); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d writes in 20 s", i)
		}
	}
	if c.stale(time.Now()) != true {
		t.Fatal("a connection used for 5000 requests is not retired")
	}
}

// When the other side closes the connection, it is marked dead without any write noticing.
func TestH1ConnNoticesClose(t *testing.T) {
	addr, served := h1Server(t, 3)
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := NewH1Conn(raw)
	req := pipelinedRequest(t)
	for i := 0; i < 3; i++ {
		if err := c.write(req); err != nil {
			t.Fatal(err)
		}
	}
	<-served
	for end := time.Now().Add(5 * time.Second); !c.dead.Load(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("closed connection not marked dead")
		}
	}
	if !c.stale(time.Now()) {
		t.Fatal("dead connection is not stale")
	}
}
