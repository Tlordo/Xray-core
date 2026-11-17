package splithttp

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Flush padding keeps the download stream moving through a CDN that buffers the response.
//
// Some CDNs (Yandex Cloud CDN) randomly hold the last piece of a streamed response until the
// origin sends something else, so a small reply that nothing follows never reaches the client.
// When the client asks for it, the server frames the download stream and, once the stream has
// been quiet for a moment, sends a padding frame that pushes the held piece out. The client
// drops padding frames.
//
// A client opts in with the header FlushPadConfigHeader in its "headers" setting; the value is
// the quiet time in milliseconds. A client built with this code never sends that header: it
// sends FlushPadRequestHeader on the download request instead, and reads frames only if the
// server answers with FlushPadResponseHeader. A stock client sends the configured header as it
// is, the server ignores it, and the stream stays unframed.
//
// Frame: 2 bytes big endian, the top bit marks padding, the other 15 bits are the length,
// then that many bytes.
const (
	FlushPadConfigHeader   = "X-Flush-Pad"
	FlushPadRequestHeader  = "X-Flush-Pad-Req"
	FlushPadResponseHeader = "X-Flush-Pad-Ok"

	flushPadFlag     = 0x8000
	flushPadMaxFrame = 0x7fff
	flushPadDefault  = 30 * time.Millisecond
	flushPadMin      = 5 * time.Millisecond
	flushPadMax      = 2 * time.Second
	// The second padding frame covers a first one that arrived too close to the data to push it.
	flushPadSecondFactor = 6
	flushPadCount        = 2
)

// flushPadDelay reads the quiet time a header value asks for.
func flushPadDelay(value string) time.Duration {
	ms, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || ms <= 0 {
		return flushPadDefault
	}
	return min(max(time.Duration(ms)*time.Millisecond, flushPadMin), flushPadMax)
}

// flushPadSetting returns the configured quiet time, if the client's headers ask for padding.
func (c *Config) flushPadSetting() (string, bool) {
	for k, v := range c.Headers {
		if strings.EqualFold(k, FlushPadConfigHeader) {
			return v, true
		}
	}
	return "", false
}

// wantsFlushPad reports the quiet time a download request asks for.
func wantsFlushPad(request *http.Request) (time.Duration, bool) {
	value := request.Header.Get(FlushPadRequestHeader)
	if value == "" {
		return 0, false
	}
	return flushPadDelay(value), true
}

// appendFlushFrames appends b to dst as data frames.
func appendFlushFrames(dst, b []byte) []byte {
	for len(b) > 0 {
		n := min(len(b), flushPadMaxFrame)
		dst = binary.BigEndian.AppendUint16(dst, uint16(n))
		dst = append(dst, b[:n]...)
		b = b[n:]
	}
	return dst
}

// newFlushPadFrame makes one padding frame of 16 to 79 random bytes.
func newFlushPadFrame() []byte {
	var size [1]byte
	rand.Read(size[:])
	n := 16 + int(size[0]&0x3f)
	frame := make([]byte, 2+n)
	binary.BigEndian.PutUint16(frame, uint16(flushPadFlag|n))
	rand.Read(frame[2:])
	return frame
}

// flushPadReader strips the framing and the padding from a download stream.
type flushPadReader struct {
	body io.ReadCloser
	br   *bufio.Reader
	left int
}

func newFlushPadReader(body io.ReadCloser) *flushPadReader {
	return &flushPadReader{body: body, br: bufio.NewReaderSize(body, 32*1024)}
}

func (r *flushPadReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.left == 0 {
		var head [2]byte
		if _, err := io.ReadFull(r.br, head[:]); err != nil {
			return 0, err
		}
		v := binary.BigEndian.Uint16(head[:])
		n := int(v &^ flushPadFlag)
		if v&flushPadFlag != 0 {
			if _, err := r.br.Discard(n); err != nil {
				return 0, err
			}
			continue
		}
		r.left = n
	}
	n, err := r.br.Read(p[:min(len(p), r.left)])
	r.left -= n
	if err == io.EOF && r.left > 0 {
		err = io.ErrUnexpectedEOF // the stream ended inside a frame
	}
	return n, err
}

func (r *flushPadReader) Close() error {
	return r.body.Close()
}
