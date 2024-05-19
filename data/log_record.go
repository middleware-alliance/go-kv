package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType uint8

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxFinished
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
// +---------------------------------------------------------------------------------------+
// | crc32 | record type | key size 			   | value size     		 | key | value |
// +---------------------------------------------------------------------------------------+
// | 4     | 1           | Variable length (max 5) | Variable length (max 5) | n   | n     |
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// init header with zeros
	header := make([]byte, maxLogRecordHeaderSize)

	// the five is stored record type
	header[4] = byte(record.Type)
	index := 5
	// after the record type, the key size and value size are stored
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	var size = index + len(record.Key) + len(record.Value)
	encBytes := make([]byte, size)

	// copy header to the encoded bytes
	copy(encBytes[:index], header[:index])
	// copy key and value to the encoded bytes
	copy(encBytes[index:], record.Key)
	copy(encBytes[index+len(record.Key):], record.Value)

	// compute crc of the encoded bytes
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	//fmt.Printf("header length: %d, crc: %d\n", index, crc)

	return encBytes, int64(size)
}

// EncodeLogRecordPos encodes a log record position into a byte slice.
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	encBytes := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(encBytes[index:], int64(pos.Fid))
	index += binary.PutVarint(encBytes[index:], pos.Offset)
	return encBytes[:index]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: offset,
	}
}

// DecodeLogRecord decodes a log record Header from a byte slice.
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// check if the buffer is long enough to contain the header
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: LogRecordType(buf[4]),
	}

	var index = 5
	// get actual key and value sizes
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC computes the crc of a log record.
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	// compute crc of the header
	crc := crc32.ChecksumIEEE(header[:])
	// compute crc of the key and value
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}

// LogRecordPos represents the position of a log record in a file.
type LogRecordPos struct {
	Fid    uint32 // file id of the log file
	Offset int64  // offset in the file
}

// TransactionRecord represents a transaction record.
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
