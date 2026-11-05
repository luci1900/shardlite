//go:build !fault

package main

import "github.com/luci1900/shardlite/internal/node"

// faultOptions arms nothing: the fault settings exist only in a binary built
// with the fault tag (fault.go).
func faultOptions(func(string) string) ([]node.RunOption, error) { return nil, nil }
