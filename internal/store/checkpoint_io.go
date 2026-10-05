package store

import "os"

// Operations are injectable inside this package for deterministic crash-window
// tests. Production uses the durable implementations with no alternate policy.
type checkpointOperations struct {
	writeFile   func(string, []byte) error
	replaceFile func(string, string) error
	openFile    func(string) (*os.File, error)
}

func (ops checkpointOperations) write(path string, body []byte) error {
	if ops.writeFile != nil {
		return ops.writeFile(path, body)
	}
	return writeDurableFile(path, body)
}

func (ops checkpointOperations) replace(from, to string) error {
	if ops.replaceFile != nil {
		return ops.replaceFile(from, to)
	}
	return durableReplace(from, to)
}

func (ops checkpointOperations) open(path string) (*os.File, error) {
	if ops.openFile != nil {
		return ops.openFile(path)
	}
	return os.OpenFile(path, os.O_RDWR, filePerm)
}
