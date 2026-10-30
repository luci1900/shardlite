package s3test_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/s3test"
)

func TestSkewShiftsTheDateHeader(t *testing.T) {
	t.Parallel()
	const date = "Tue, 15 Nov 1994 08:12:31 GMT"
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", date)
		w.Header().Set("X-Host", r.Host)
		w.Write([]byte("body"))
	}))
	defer up.Close()
	s := s3test.StartSkew(t, up.URL)
	get := func() *http.Response {
		resp, err := http.Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if got := get().Header.Get("Date"); got != date {
		t.Fatalf("unskewed Date %q, want %q", got, date)
	}
	s.Set(90 * time.Second)
	resp := get()
	if got := resp.Header.Get("Date"); got != "Tue, 15 Nov 1994 08:14:01 GMT" {
		t.Fatalf("Date %q, want 90s ahead", got)
	}
	if resp.Header.Get("X-Host") != s.URL[len("http://"):] {
		t.Fatalf("the store saw Host %q, want the one the request was signed for", resp.Header.Get("X-Host"))
	}
	s.Set(-time.Minute)
	if got := get().Header.Get("Date"); got != "Tue, 15 Nov 1994 08:11:31 GMT" {
		t.Fatalf("Date %q, want a minute behind", got)
	}
}
