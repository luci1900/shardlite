package e2eapp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// gRPC without a gRPC library. On the wire it is HTTP/2 with content-type
// application/grpc, a body of length-prefixed messages (a flag byte, which is
// 0 for uncompressed, and a 4-byte big-endian length) and the status in the
// trailers. That is all the node has to proxy, so the test app and its
// clients speak it directly.

// WriteGRPCMessage writes one message.
func WriteGRPCMessage(w io.Writer, msg []byte) error {
	b := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(b[1:], uint32(len(msg)))
	_, err := w.Write(append(b, msg...))
	return err
}

// ReadGRPCMessage reads one message. io.EOF at a message boundary is the end of the stream.
func ReadGRPCMessage(r io.Reader) ([]byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	if h[0] != 0 {
		return nil, errors.New("compressed gRPC messages are not supported")
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n > 1<<24 {
		return nil, errors.New("gRPC message too large")
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return msg, nil
}

// GRPCStatusError is a call that ended with a non-zero gRPC status.
type GRPCStatusError struct {
	Code    int
	Message string
}

func (e *GRPCStatusError) Error() string { return fmt.Sprintf("grpc status %d: %s", e.Code, e.Message) }

// GRPCReply starts a gRPC response on the server side: headers and the
// declaration of the trailers. Finish sets them.
func GRPCReply(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "application/grpc")
	h.Set("Trailer", "Grpc-Status, Grpc-Message")
}

// GRPCFinish ends a response with a status.
func GRPCFinish(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Grpc-Status", strconv.Itoa(code))
	w.Header().Set("Grpc-Message", url.PathEscape(msg))
}

// GRPCClient calls a server over h2c.
type GRPCClient struct {
	HTTP *http.Client // must speak h2c; see NewH2CClient
	Base string       // http://host:port
}

// NewH2CClient returns an HTTP client that speaks HTTP/2 without TLS.
func NewH2CClient() *http.Client {
	tr := &http.Transport{MaxIdleConnsPerHost: 64}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetUnencryptedHTTP2(true)
	return &http.Client{Transport: tr}
}

// Call sends one request message and returns every response message. header
// carries things like Shardlite-Target. Messages are delivered to onMsg as
// they arrive, if it is set. The call fails with a *GRPCStatusError if the
// status is not 0.
func (c *GRPCClient) Call(ctx context.Context, method string, header http.Header, req []byte, onMsg func([]byte)) ([][]byte, error) {
	var body bytes.Buffer
	WriteGRPCMessage(&body, req)
	q, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+method, &body)
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		q.Header[k] = vs
	}
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Te", "trailers")
	resp, err := c.HTTP.Do(q)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	var msgs [][]byte
	for {
		m, err := ReadGRPCMessage(resp.Body)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return msgs, err
		}
		if onMsg != nil {
			onMsg(m)
		}
		msgs = append(msgs, m)
	}
	// The status is in the trailers, or in the headers of a trailers-only response.
	status := resp.Trailer.Get("Grpc-Status")
	message := resp.Trailer.Get("Grpc-Message")
	if status == "" {
		status, message = resp.Header.Get("Grpc-Status"), resp.Header.Get("Grpc-Message")
	}
	if status == "" {
		return msgs, errors.New("the response has no grpc-status")
	}
	if code, _ := strconv.Atoi(status); code != 0 {
		msg, _ := url.PathUnescape(message)
		return msgs, &GRPCStatusError{Code: code, Message: msg}
	}
	return msgs, nil
}

// WithTimeout is a context for one call.
func WithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
