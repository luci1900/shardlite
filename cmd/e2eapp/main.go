// Command e2eapp runs the end-to-end test app (see internal/e2eapp).
//
//	e2eapp -listen 127.0.0.1:0 -control 127.0.0.1:7000
//
// It prints "listening ADDR" once it is serving.
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/luci1900/shardlite/internal/e2eapp"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to serve on")
	control := flag.String("control", "127.0.0.1:7000", "the node's control API")
	idle := flag.Duration("idle-close", 500*time.Millisecond, "close a shard's database after this long unused")
	flag.Parse()

	app := e2eapp.New(e2eapp.Options{Control: *control, IdleClose: *idle})
	defer app.Close()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: app.Handler(), Protocols: protos, ReadHeaderTimeout: 10 * time.Second}
	fmt.Printf("listening %s\n", ln.Addr())
	fmt.Fprintln(os.Stderr, srv.Serve(ln))
	os.Exit(1)
}
