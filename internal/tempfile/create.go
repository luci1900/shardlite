package tempfile

import "os"

// createRemoved makes a file in dir and removes its name. A crash between the
// two leaves a file behind, which the node's sweep of the directory at startup
// removes.
func createRemoved(dir string) (*os.File, error) {
	f, err := os.CreateTemp(dir, "log-*")
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
