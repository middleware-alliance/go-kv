package fio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// To run this test, go to the fio directory and run the following command:
// go test -v ./
// go test -v -run TestFileIO_Close

func destroyFile(filePath string) {
	err := os.RemoveAll(filePath)
	if err != nil {
		panic("os.Remove() error = %v" + err.Error())
	}
}

func TestFileIO_Close(t *testing.T) {
	type fields struct {
		filePath string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name: "TestNewFileIO",
			fields: fields{
				filePath: filepath.Join("/tmp", "a.data"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFileIOManager(tt.fields.filePath)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("NewFileIOManager() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got == nil {
				t.Errorf("NewFileIOManager() got = %v, want not nil", got)
			}
			defer destroyFile(tt.fields.filePath)
			err = got.Close()
			if err != nil {
				t.Errorf("Close() error = %v", err)
			}
		})
	}
}

func TestFileIO_Read(t *testing.T) {
	type fields struct {
		fd *FileIO
	}

	fd, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	if err != nil {
		t.Errorf("NewFileIOManager() error = %v", err)
	}

	defer destroyFile(fd.fd.Name())

	_, err = fd.Write([]byte("hello"))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	_, err = fd.Write([]byte(" world"))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	type args struct {
		bytes  []byte
		offset int64
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        int
		wantContent []byte
		wantErr     error
	}{
		{
			name: "TestRead 1",
			fields: fields{
				fd: fd,
			},
			args: args{
				bytes:  make([]byte, 5),
				offset: 0,
			},
			want:        5,
			wantContent: []byte("hello"),
			wantErr:     nil,
		},
		{
			name: "TestRead 2",
			fields: fields{
				fd: fd,
			},
			args: args{
				bytes:  make([]byte, 6),
				offset: 5,
			},
			want:        6,
			wantContent: []byte(" world"),
			wantErr:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.fields.fd
			got, err := f.Read(tt.args.bytes, tt.args.offset)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Read() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			if string(tt.wantContent) != string(tt.args.bytes[:got]) {
				t.Errorf("Read() content = %v, want %v", string(tt.args.bytes[:got]), string(tt.wantContent))
			}
		})
	}

	err = fd.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestFileIO_Sync(t *testing.T) {
	fd, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	if err != nil {
		t.Errorf("NewFileIOManager() error = %v", err)
	}
	defer destroyFile(fd.fd.Name())

	tests := []struct {
		name    string
		fields  *FileIO
		wantErr error
	}{
		{
			name:    "sync test",
			fields:  fd,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.fields
			if err = f.Sync(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileIO_Write(t *testing.T) {
	type fields struct {
		fd *FileIO
	}
	type args struct {
		bytes []byte
	}
	fd, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	if err != nil {
		t.Errorf("NewFileIOManager() error = %v", err)
	}
	defer destroyFile(fd.fd.Name())

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr error
	}{
		{
			name: "TestWrite",
			fields: fields{
				fd: fd,
			},
			args: args{
				bytes: []byte("hello world"),
			},
			want:    11,
			wantErr: nil,
		},
		{
			name: "TestWrite Nil String",
			fields: fields{
				fd: fd,
			},
			args: args{
				bytes: []byte(""),
			},
			want:    0,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.fd.Write(tt.args.bytes)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
		})
	}

	err = fd.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestNewFileIO(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "TestNewFileIO",
			args: args{
				filePath: filepath.Join("/tmp", "a.data"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFileIOManager(tt.args.filePath)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("NewFileIOManager() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got == nil {
				t.Errorf("NewFileIOManager() got = %v, want not nil", got)
			}
			err = os.Remove(got.fd.Name())
			if err != nil {
				t.Errorf("os.Remove() error = %v", err)
			}
			err = got.Close()
			if err != nil {
				t.Errorf("Close() error = %v", err)
			}
		})
	}
}
