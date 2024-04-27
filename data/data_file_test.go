package data

import (
	"errors"
	"os"
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
