package data

type LogRecordType uint8

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord represents a record in the log.
// It contains the key, value, and type of the record.
// The key and value are byte slices, and the type is a LogRecordType.
type LogRecord struct {
	Key   []byte        // key of the record
	Value []byte        // value of the record
	Type  LogRecordType // type of the record
}

// EncodeLogRecord encodes a log record into a byte slice.
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// Encode the record into a byte slice
	return nil, 0
}

// LogRecordPos represents the position of a log record in a file.
type LogRecordPos struct {
	Fid    uint32 // file id of the log file
	Offset int64  // offset in the file
}
