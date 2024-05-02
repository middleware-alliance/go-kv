package data

import (
	"hash/crc32"
	"reflect"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	tests := []struct {
		name    string
		record  *LogRecord
		want    []byte
		wantLen int64
	}{
		{
			name: "normal record",
			record: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("bitcask-go"),
				Type:  LogRecordNormal,
			},
			want:    []byte{104, 82, 240, 150, 0, 8, 20, 110, 97, 109, 101, 98, 105, 116, 99, 97, 115, 107, 45, 103, 111},
			wantLen: 21,
		},
		{
			name: "value is nil record",
			record: &LogRecord{
				Key:  []byte("name"),
				Type: LogRecordNormal,
			},
			want:    []byte{9, 252, 88, 14, 0, 8, 0, 110, 97, 109, 101},
			wantLen: 11,
		},
		{
			name: "delete record",
			record: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("bitcask-go"),
				Type:  LogRecordDeleted,
			},
			want:    []byte{43, 153, 86, 17, 1, 8, 20, 110, 97, 109, 101, 98, 105, 116, 99, 97, 115, 107, 45, 103, 111},
			wantLen: 21,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, length := EncodeLogRecord(tt.record)
			if !reflect.DeepEqual(enc, tt.want) {
				t.Errorf("EncodeLogRecord() enc = %v, want %v", enc, tt.want)
			}
			if tt.wantLen != length {
				t.Errorf("EncodeLogRecord() length = %v, want %v", length, tt.wantLen)
			}
		})
	}
}

func Test_decodeLogRecordHeader(t *testing.T) {
	tests := []struct {
		name    string
		buf     []byte
		want    *logRecordHeader
		wantLen int64
	}{
		{
			name:    "read normal record header",
			buf:     []byte{104, 82, 240, 150, 0, 8, 20},
			want:    &logRecordHeader{crc: 2532332136, recordType: LogRecordNormal, keySize: 4, valueSize: 10},
			wantLen: 7,
		},
		{
			name:    "read nil value record header",
			buf:     []byte{9, 252, 88, 14, 0, 8, 0},
			want:    &logRecordHeader{crc: 240712713, recordType: LogRecordNormal, keySize: 4, valueSize: 0},
			wantLen: 7,
		},
		{
			name:    "read delete record header",
			buf:     []byte{43, 153, 86, 17, 1, 8, 20},
			want:    &logRecordHeader{crc: 290887979, recordType: LogRecordDeleted, keySize: 4, valueSize: 10},
			wantLen: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, length := decodeLogRecordHeader(tt.buf)
			if !reflect.DeepEqual(header, tt.want) {
				t.Errorf("decodeLogRecordHeader() header = %v, want %v", header, tt.want)
			}
			if length != tt.wantLen {
				t.Errorf("decodeLogRecordHeader() length = %v, want %v", length, tt.wantLen)
			}
		})
	}
}

func Test_getLogRecordCRC(t *testing.T) {
	type args struct {
		lr     *LogRecord
		header []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "normal crc",
			args: args{
				lr: &LogRecord{
					Key:   []byte("name"),
					Value: []byte("bitcask-go"),
					Type:  LogRecordNormal,
				},
				header: []byte{104, 82, 240, 150, 0, 8, 20},
			},
			want: 2532332136,
		},
		{
			name: "nil value crc",
			args: args{
				lr: &LogRecord{
					Key:  []byte("name"),
					Type: LogRecordNormal,
				},
				header: []byte{9, 252, 88, 14, 0, 8, 0},
			},
			want: 240712713,
		},
		{
			name: "delete crc",
			args: args{
				lr: &LogRecord{
					Key:   []byte("name"),
					Value: []byte("bitcask-go"),
					Type:  LogRecordDeleted,
				},
				header: []byte{43, 153, 86, 17, 1, 8, 20},
			},
			want: 290887979,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if crc := getLogRecordCRC(tt.args.lr, tt.args.header[crc32.Size:]); crc != tt.want {
				t.Errorf("getLogRecordCRC() = %v, want %v", crc, tt.want)
			}
		})
	}
}
