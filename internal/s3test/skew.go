package s3test

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

// Skew is a proxy in front of the store that moves the time in every response's
// Date header by an offset the test sets, which is how a node sees the clock of
// its host drift from the store's: the node's own clock cannot be moved from
// outside, and the store's time is all the node compares it with. Point the
// node's SHARDLITE_S3_ENDPOINT at URL. Nothing else about the exchange changes
// (the request keeps the Host it was signed for).
type Skew struct {
	URL    string
	offset atomic.Int64
	srv    *httptest.Server
}

// StartSkew proxies to the store at target until the test ends.
func StartSkew(t testing.TB, target string) *Skew {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	s := &Skew{}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ModifyResponse = func(r *http.Response) error {
		if d, err := http.ParseTime(r.Header.Get("Date")); err == nil {
			r.Header.Set("Date", d.Add(time.Duration(s.offset.Load())).UTC().Format(http.TimeFormat))
		}
		return nil
	}
	s.srv = httptest.NewServer(proxy)
	s.URL = s.srv.URL
	t.Cleanup(s.srv.Close)
	return s
}

// Set makes the store's clock read d ahead of the real one (behind if negative)
// from the next response on. Changing it is a step.
func (s *Skew) Set(d time.Duration) { s.offset.Store(int64(d)) }
