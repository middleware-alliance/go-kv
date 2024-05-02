package data

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	if err != nil {
		t.Error(err)
	}
	if dataFile == nil {
		t.Error("dataFile is nil")
	}
	tests := []struct {
		name     string
		dataFile *DataFile
		wantErr  error
	}{
		{
			name:     "Test Close",
			dataFile: dataFile,
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err = tt.dataFile.Close(); !errors.Is(tt.wantErr, err) {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	if err != nil {
		t.Error(err)
	}
	if dataFile == nil {
		t.Error("dataFile is nil")
	}
	tests := []struct {
		name     string
		dataFile *DataFile
		wantErr  error
	}{
		{
			name:     "Test Sync",
			dataFile: dataFile,
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err = tt.dataFile.Sync(); !errors.Is(tt.wantErr, err) {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	if err != nil {
		t.Error(err)
	}
	if dataFile == nil {
		t.Error("dataFile is nil")
	}
	tests := []struct {
		name     string
		dataFile *DataFile
		content  []byte
		wantErr  error
	}{
		{
			name:     "Test Write",
			dataFile: dataFile,
			content:  []byte("hello world"),
			wantErr:  nil,
		},
		{
			name:     "Test Write1",
			dataFile: dataFile,
			content:  []byte("hello world1"),
			wantErr:  nil,
		},
		{
			name:     "Test Write2",
			dataFile: dataFile,
			content:  []byte("hello world2"),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err = tt.dataFile.Write(tt.content); !errors.Is(err, tt.wantErr) {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			t.Log(tt.dataFile.WriteOff)
		})
	}
}

func TestOpenDataFile(t *testing.T) {
	type args struct {
		dirPath string
		fileId  uint32
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "Test OpenDataFile",
			args: args{
				dirPath: os.TempDir(),
				fileId:  0,
			},
			wantErr: nil,
		},
		{
			name: "Test OpenDataFile1",
			args: args{
				dirPath: os.TempDir(),
				fileId:  1,
			},
			wantErr: nil,
		},
		{
			name: "Test OpenDataFile1",
			args: args{
				dirPath: os.TempDir(),
				fileId:  1,
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log(tt.args.dirPath)
			got, err := OpenDataFile(tt.args.dirPath, tt.args.fileId)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("OpenDataFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("OpenDataFile() got = %v, want not nil", got)
			}
		})
	}
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	// remove the old data file
	fileName := fmt.Sprintf("%s%09d", os.TempDir(), 1) + DataFileNameSuffix
	err := os.Remove(fileName)
	if err != nil {
		t.Error(err)
	}

	dataFile, err := OpenDataFile(os.TempDir(), 1)
	if err != nil {
		t.Error(err)
	}

	oneRecord := &LogRecord{
		Type:  LogRecordNormal,
		Key:   []byte("key"),
		Value: []byte("bitcast kv go"),
	}
	writeOneLogRecord, writeOneSize := EncodeLogRecord(oneRecord)
	err = dataFile.Write(writeOneLogRecord)
	if err != nil {
		t.Error(err)
	}

	twoRecord := &LogRecord{
		Type:  LogRecordNormal,
		Key:   []byte("key"),
		Value: []byte("a new bitcast kv go"),
	}
	writeTwoLogRecord, writeTwoSize := EncodeLogRecord(twoRecord)
	err = dataFile.Write(writeTwoLogRecord)
	if err != nil {
		t.Error(err)
	}

	deleteRecord := &LogRecord{
		Type:  LogRecordDeleted,
		Key:   []byte("key"),
		Value: []byte(""),
	}
	deleteLogRecord, deleteSize := EncodeLogRecord(deleteRecord)
	err = dataFile.Write(deleteLogRecord)
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		name     string
		dataFile *DataFile
		offset   int64
		want     *LogRecord
		size     int64
	}{
		{
			name:     "Test one ReadLogRecord",
			dataFile: dataFile,
			offset:   0,
			want:     oneRecord,
			size:     writeOneSize,
		},
		{
			name:     "Test two ReadLogRecord",
			dataFile: dataFile,
			offset:   writeOneSize,
			want:     twoRecord,
			size:     writeTwoSize,
		},
		{
			name:     "Test three delete ReadLogRecord",
			dataFile: dataFile,
			offset:   writeOneSize + writeTwoSize,
			want:     deleteRecord,
			size:     deleteSize,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readRecord, size, err := tt.dataFile.ReadLogRecord(tt.offset)
			if err != nil {
				t.Errorf("ReadLogRecord() error = %v", err)
			}
			if !reflect.DeepEqual(readRecord, tt.want) {
				t.Errorf("ReadLogRecord() readRecord = %v, want %v", readRecord, tt.want)
			}
			if size != tt.size {
				t.Errorf("ReadLogRecord() size = %v, want %v", size, tt.size)
			}
		})
	}
}
