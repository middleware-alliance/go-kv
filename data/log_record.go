package data

import "encoding/binary"

type LogRecordType uint8

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// maxLogRecordHeaderSize is the size of the header of a log record in bytes.
// crc type key size value size
// 4  + 1     + 5       +5    = 15 bytes
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord represents a record in the log.
// It contains the key, value, and type of the record.
// The key and value are byte slices, and the type is a LogRecordType.
type LogRecord struct {
	Key   []byte        // key of the record
	Value []byte        // value of the record
	Type  LogRecordType // type of the record
}

type logRecordHeader struct {
	crc        uint32        // crc32 of the record
	recordType LogRecordType // type of the LogRecord
	keySize    uint32        // length of the key
	valueSize  uint32        // length of the value
}

// EncodeLogRecord encodes a log record into a byte slice.
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// Encode the record into a byte slice
	return nil, 0
}

// DecodeLogRecord decodes a log record Header from a byte slice.
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC computes the crc of a log record.
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}

// LogRecordPos represents the position of a log record in a file.
type LogRecordPos struct {
	Fid    uint32 // file id of the log file
	Offset int64  // offset in the file
}
