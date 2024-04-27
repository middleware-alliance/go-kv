package fio

const DataFilePerm = 0644

type IOManager interface {
	// Read reads data from the file at the given offset.
	Read([]byte, int64) (int, error)

	// Write writes data to the file at the current offset.
	Write([]byte) (int, error)

	// Sync flushes any buffered data to disk.
	Sync() error

	// Close closes the file.
	Close() error

	// Size sizes the file to the given size.
	Size() (int64, error)
}

// NewIOManager creates a new IOManager for the given file name.
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
