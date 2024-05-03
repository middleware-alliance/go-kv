package go_kv

import (
	"errors"
	"go-kv/utils"
	"os"
	"reflect"
	"testing"
)

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "bitcask-go-get")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("os.RemoveAll() error = %v", err)
		}
	}(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	if err != nil {
		t.Errorf("Open() error = %v", err)
	}
	tests := []struct {
		name    string
		key     []byte
		putFn   func()
		getFn   func()
		wantErr bool
	}{
		{
			name: "delete one normal key-value",
			key:  utils.GetTestKey(11),
			putFn: func() {
				err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			},
			getFn: func() {
				_, err = db.Get(utils.GetTestKey(11))
				if !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("Get() error = %v", err)
				}
			},
			wantErr: false,
		},
		{
			name:    "delete one key-value which is not exist",
			key:     utils.GetTestKey(12),
			putFn:   func() {},
			getFn:   func() {},
			wantErr: false,
		},
		{
			name:    "delete one key which is nil",
			key:     nil,
			putFn:   func() {},
			getFn:   func() {},
			wantErr: true,
		},
		{
			name: "after key deleted, and put key again",
			key:  utils.GetTestKey(11),
			putFn: func() {
				err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			},
			getFn: func() {
				err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}

				_, err := db.Get(utils.GetTestKey(11))
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// put key-value function
			tt.putFn()
			// delete key function
			if err = db.Delete(tt.key); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			// get key function
			tt.getFn()
		})
	}
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "bitcask-go-get")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("os.RemoveAll() error = %v", err)
		}
	}(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	if err != nil {
		t.Errorf("Open() error = %v", err)
	}
	tests := []struct {
		name    string
		key     []byte
		putFn   func()
		wantErr bool
	}{
		{
			name:    "get one normal key-value",
			key:     utils.GetTestKey(11),
			putFn:   func() { _ = db.Put(utils.GetTestKey(11), utils.RandomValue(24)) },
			wantErr: false,
		},
		{
			name:    "get one key-value which is not exist",
			key:     utils.GetTestKey(12),
			putFn:   func() {},
			wantErr: true,
		},
		{
			name: "get key with same put key but different value",
			key:  utils.GetTestKey(11),
			putFn: func() {
				_ = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
				_ = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
			},
			wantErr: false,
		},
		{
			name: "get key with first put key, but second put key is deleted",
			key:  utils.GetTestKey(11),
			putFn: func() {
				_ = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
				_ = db.Delete(utils.GetTestKey(11))
			},
			wantErr: true,
		},
		{
			name: "from older file get key-value",
			key:  utils.GetTestKey(101),
			putFn: func() {
				for i := 100; i < 1000000; i++ {
					err = db.Put(utils.GetTestKey(i), utils.RandomValue(128))
					if err != nil {
						t.Errorf("Put() error = %v", err)
					}
				}
				if 2 != len(db.olderFiles) {
					t.Errorf("olderFiles length = %d, want %d", len(db.olderFiles), 2)
				}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// put key-value function
			tt.putFn()
			gotValue, err := db.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(gotValue) == 0 && !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("Get() gotValue = %v, want not empty", gotValue)
			}
		})
	}
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "bitcask-go-put")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("os.RemoveAll() error = %v", err)
		}
	}(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	if err != nil {
		t.Errorf("Open() error = %v", err)
	}
	tests := []struct {
		name    string
		key     []byte
		value   []byte
		wantErr bool
	}{
		{
			name:    "put one normal key-value",
			key:     utils.GetTestKey(1),
			value:   utils.RandomValue(24),
			wantErr: false,
		},
		{
			name:    "put same key-value again",
			key:     utils.GetTestKey(1),
			value:   utils.RandomValue(24),
			wantErr: false,
		},
		{
			name:    "key is nil",
			key:     nil,
			value:   utils.RandomValue(24),
			wantErr: true,
		},
		{
			name:    "value is nil",
			key:     utils.GetTestKey(1),
			value:   nil,
			wantErr: false,
		},
		{
			name: "roll file when data file is full",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "roll file when data file is full" {
				for i := 0; i < 1000000; i++ {
					err = db.Put(utils.GetTestKey(i), utils.RandomValue(128))
					if err != nil {
						t.Errorf("Put() error = %v", err)
					}
				}
				if 2 != len(db.olderFiles) {
					t.Errorf("olderFiles length = %d, want %d", len(db.olderFiles), 2)
				}
				return
			}

			if err = db.Put(tt.key, tt.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			gotValue, err := db.Get(tt.key)
			if err != nil {
				t.Errorf("Get() error = %v", err)
			}
			if !(len(gotValue) == len(tt.value) || reflect.DeepEqual(gotValue, tt.value)) {
				t.Errorf("Get() gotValue = %v, want %v", gotValue, tt.value)
			}
		})
	}

}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "go-kv")
	opts.DirPath = dir
	tests := []struct {
		name    string
		options Options
		wantErr error
	}{
		{
			name:    "test_open_with_default_options",
			options: opts,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Open(tt.options)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_checkOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{
			name:    "test_check_options_with_valid_options",
			options: DefaultOptions,
			wantErr: false,
		},
		{
			name: "test_check_options_with_invalid_options",
			options: Options{
				DirPath: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkOptions(tt.options); (err != nil) != tt.wantErr {
				t.Errorf("checkOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
