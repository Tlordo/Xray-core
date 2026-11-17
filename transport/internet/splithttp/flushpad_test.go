package splithttp

import (
	"bytes"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
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
	for _, tc := range []struct {
		name string
		conn H1Conn
		want bool
	}{
		{"new", H1Conn{}, false},
		{"just used", H1Conn{requests: 1, lastUsed: now.Add(-time.Second)}, false},
		{"idle", H1Conn{requests: 1, lastUsed: now.Add(-h1MaxIdle - time.Second)}, true},
		{"many requests", H1Conn{requests: h1MaxRequests, lastUsed: now}, true},
	} {
		if got := tc.conn.stale(now); got != tc.want {
			t.Errorf("%s: stale = %v, want %v", tc.name, got, tc.want)
		}
	}
}
