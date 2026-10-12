//go:build !linux && !darwin

package node

// filesystemOf: the node targets Linux (macOS to develop). Elsewhere nothing is checked.
func filesystemOf(path string) (string, error) { return "", nil }

const hasTmpfs = false
