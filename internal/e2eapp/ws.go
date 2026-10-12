package e2eapp

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// A minimal WebSocket (RFC 6455) for the test app and its clients, so the
// repository doesn't depend on a WebSocket library. It handles what the tests
// need: the handshake, text and binary messages (with fragmentation), ping,
// pong and close.

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	opCont   = 0x0
	opText   = 0x1
	opBinary = 0x2
	opClose  = 0x8
	opPing   = 0x9
	opPong   = 0xA
)

// WSConn is one end of a WebSocket.
type WSConn struct {
	c      net.Conn
	br     *bufio.Reader
	client bool // clients mask their frames
}

func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// AcceptWS completes the handshake of an upgrade request and takes over the connection.
func AcceptWS(w http.ResponseWriter, r *http.Request) (*WSConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") ||
		r.Header.Get("Sec-WebSocket-Version") != "13" || r.Header.Get("Sec-WebSocket-Key") == "" {
		http.Error(w, "not a WebSocket handshake", http.StatusBadRequest)
		return nil, errors.New("not a WebSocket handshake")
	}
	h, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "cannot hijack", http.StatusInternalServerError)
		return nil, errors.New("cannot hijack")
	}
	c, rw, err := h.Hijack()
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", wsAccept(r.Header.Get("Sec-WebSocket-Key")))
	if err := rw.Flush(); err != nil {
		c.Close()
		return nil, err
	}
	return &WSConn{c: c, br: rw.Reader}, nil
}

// DialWS connects to addr (host:port), asks for path with the given extra
// headers and completes the handshake.
func DialWS(addr, path string, header http.Header) (*WSConn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	var kb [16]byte
	rand.Read(kb[:])
	key := base64.StdEncoding.EncodeToString(kb[:])
	var sb strings.Builder
	fmt.Fprintf(&sb, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n", path, addr, key)
	for k, vs := range header {
		for _, v := range vs {
			fmt.Fprintf(&sb, "%s: %s\r\n", k, v)
		}
	}
	sb.WriteString("\r\n")
	if _, err := io.WriteString(c, sb.String()); err != nil {
		c.Close()
		return nil, err
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		c.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Accept") != wsAccept(key) {
		c.Close()
		return nil, fmt.Errorf("websocket handshake failed: %s", resp.Status)
	}
	return &WSConn{c: c, br: br, client: true}, nil
}

// SetReadDeadline sets a deadline on reads from the connection.
func (w *WSConn) SetReadDeadline(t time.Time) error { return w.c.SetReadDeadline(t) }

// Close closes the connection without a closing handshake.
func (w *WSConn) Close() error { return w.c.Close() }

func (w *WSConn) writeFrame(op byte, payload []byte) error {
	hdr := []byte{0x80 | op, 0}
	n := len(payload)
	switch {
	case n < 126:
		hdr[1] = byte(n)
	case n < 1<<16:
		hdr[1] = 126
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(n))
	default:
		hdr[1] = 127
		hdr = binary.BigEndian.AppendUint64(hdr, uint64(n))
	}
	if w.client {
		hdr[1] |= 0x80
		var mask [4]byte
		rand.Read(mask[:])
		hdr = append(hdr, mask[:]...)
		masked := make([]byte, n)
		for i := range payload {
			masked[i] = payload[i] ^ mask[i%4]
		}
		payload = masked
	}
	_, err := w.c.Write(append(hdr, payload...))
	return err
}

// WriteText sends one text message.
func (w *WSConn) WriteText(s string) error { return w.writeFrame(opText, []byte(s)) }

func (w *WSConn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(w.br, h[:]); err != nil {
		return
	}
	fin, op = h[0]&0x80 != 0, h[0]&0x0f
	masked := h[1]&0x80 != 0
	n := uint64(h[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err = io.ReadFull(w.br, b[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err = io.ReadFull(w.br, b[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	if n > 1<<24 {
		err = errors.New("websocket frame too large")
		return
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(w.br, mask[:]); err != nil {
			return
		}
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(w.br, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return
}

// ReadText returns the next text or binary message, answering pings on the way.
// io.EOF means the peer closed.
func (w *WSConn) ReadText() (string, error) {
	var msg []byte
	for {
		fin, op, p, err := w.readFrame()
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				err = io.EOF
			}
			return "", err
		}
		switch op {
		case opPing:
			if err := w.writeFrame(opPong, p); err != nil {
				return "", err
			}
		case opPong:
		case opClose:
			w.writeFrame(opClose, p)
			return "", io.EOF
		case opText, opBinary, opCont:
			msg = append(msg, p...)
			if fin {
				return string(msg), nil
			}
		}
	}
}
