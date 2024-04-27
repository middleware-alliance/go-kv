package data

import (
	"errors"
	"fmt"
	"go-kv/fio"
	"hash/crc32"
	"io"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record may be corrupted")
)

const (
	// DataFileNamePrefix is the prefix of data file names.
	DataFileNameSuffix = ".data"
)

// DataFile is a struct that represents a data file.
type DataFile struct {
	FileId    uint32        // unique identifier of the file
	WriteOff  int64         // offset at which the file was last written to
	IoManager fio.IOManager // IO manager for the file
}

// OpenDataFile opens a data file with the given fileId in the given directory.
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// file name is the fileId with the suffix ".data"
	fileName := fmt.Sprintf("%s%09d", dirPath, fileId) + DataFileNameSuffix
	// create a new file IO manager for the file
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord reads the data from the data file at the given offset.
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// if the offset is beyond the file size, return read file end
	headerBytes := int64(maxLogRecordHeaderSize)
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// read Header from the data file
	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// decode the header
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// read the key and value length from the data file
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}
	// read the record kv content from the data file
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// decode the key and value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// validate the crc of the record
	if header.crc != getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize]) {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// ReadNBytes reads the data from the data file at the given offset.
func (df *DataFile) ReadNBytes(size, offset int64) (content []byte, err error) {
	content = make([]byte, size)
	_, err = df.IoManager.Read(content, offset)
	return
}

// Write writes the given data to the data file.
func (df *DataFile) Write(data []byte) error {
	// write the data to the data file
	n, err := df.IoManager.Write(data)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// Sync flushes any unwritten data to disk.
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close closes the data file.
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
