package fio

import "os"

// FileIO is a struct for file io operations.
type FileIO struct {
	// file descriptor
	fd *os.File
}

// NewFileIOManager creates a new FileIO object.
func NewFileIOManager(filePath string) (*FileIO, error) {
	fd, err := os.OpenFile(filePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (f FileIO) Read(bytes []byte, offset int64) (int, error) {
	return f.fd.ReadAt(bytes, offset)
}

func (f FileIO) Write(bytes []byte) (int, error) {
	return f.fd.Write(bytes)
}

func (f FileIO) Sync() error {
	return f.fd.Sync()
}

func (f FileIO) Close() error {
	return f.fd.Close()
}
