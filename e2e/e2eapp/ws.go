package e2eapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// ws serves a WebSocket on a shard. A text message is recorded in the shard and
// echoed as "ok:"+message once committed, except these commands:
//
//	count          replies with the number of rows in the shard
//	big:N          replies with a text message of N bytes
//	ping           pings the client and replies "pinged" (or the error)
//	close:CODE:why closes the connection with that code and reason
//
// A binary message is echoed back as it is, without a commit.
func (a *App) ws(w http.ResponseWriter, r *http.Request, s *shardReq) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(64 << 20)
	ctx := context.Background()
	var endErr error
	defer func() {
		code := websocket.CloseStatus(endErr)
		reason := ""
		if ce, ok := errors.AsType[websocket.CloseError](endErr); ok {
			reason = ce.Reason
		}
		a.debug.mu.Lock()
		a.debug.WSEnded++
		a.debug.WSClose = append(a.debug.WSClose, WSCloseRec{Shard: s.name, Code: int(code), Reason: reason})
		a.debug.mu.Unlock()
		c.CloseNow()
	}()
	for {
		typ, msg, err := c.Read(ctx)
		if err != nil {
			endErr = err
			return
		}
		if typ == websocket.MessageBinary {
			if err := c.Write(ctx, websocket.MessageBinary, msg); err != nil {
				endErr = err
				return
			}
			continue
		}
		text := string(msg)
		switch {
		case text == "count":
			toks, _ := s.tokensOf(ctx)
			err = c.Write(ctx, websocket.MessageText, []byte(strconv.Itoa(len(toks))))
		case strings.HasPrefix(text, "big:"):
			n, _ := strconv.Atoi(text[4:])
			err = c.Write(ctx, websocket.MessageText, []byte(strings.Repeat("x", n)))
		case text == "mark": // marks the shard for deletion when it deactivates
			reply := "marked"
			if _, merr := s.db.sql.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _shardlite_delete (x)`); merr != nil {
				reply = "err:" + merr.Error()
			}
			err = c.Write(ctx, websocket.MessageText, []byte(reply))
		case text == "ping":
			go func() {
				pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				reply := "pinged"
				if err := c.Ping(pctx); err != nil {
					reply = "ping failed: " + err.Error()
				}
				c.Write(ctx, websocket.MessageText, []byte(reply))
			}()
		case strings.HasPrefix(text, "close:"):
			codeStr, why, _ := strings.Cut(text[6:], ":")
			code, _ := strconv.Atoi(codeStr)
			endErr = c.Close(websocket.StatusCode(code), why)
			if endErr == nil {
				endErr = websocket.CloseError{Code: websocket.StatusCode(code), Reason: why}
			}
			return
		default:
			if _, _, ierr := s.insert(ctx, text); ierr != nil {
				err = c.Write(ctx, websocket.MessageText, []byte("err:"+ierr.Error()))
			} else {
				err = c.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf("ok:%s", text)))
			}
		}
		if err != nil {
			endErr = err
			return
		}
	}
}
