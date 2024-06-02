package index

import (
	"go-kv/data"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func init() {
	//os.Mkdir("/tmp/bplustree", 0777)
	// if /temp/bplustree doesn't exist, create it
	if _, err := os.Stat("/tmp/bplustree"); os.IsNotExist(err) {
		err = os.Mkdir("/tmp/bplustree", 0777)
		if err != nil {
			return
		}
	}
}

func TestNewBPlusTree(t *testing.T) {
	tests := []struct {
		name    string
		dirPath string
	}{
		{
			name:    "TestNewBPlusTree",
			dirPath: filepath.Join("/tmp/bplustree"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBPlusTree(tt.dirPath, false); got == nil {
				t.Errorf("NewBPlusTree() = %v", got)
			}
			err := os.RemoveAll(tt.dirPath)
			if err != nil {
				t.Errorf("Failed to remove directory %s: %v", tt.dirPath, err)
				return
			}
		})
	}
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join("/tmp/bplustree")
	B := NewBPlusTree(path, false)
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("Failed to remove directory %s: %v", path, err)
			return
		}
	}()

	tests := []struct {
		name  string
		setup func()
		key   []byte
		want  bool
	}{
		{
			name:  "TestBPlusTree_Delete_not_exist",
			setup: nil,
			key:   []byte("not_exist"),
			want:  false,
		},
		{
			name: "TestBPlusTree_Delete_exist",
			setup: func() {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
			},
			key:  []byte("key2"),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			if got := B.Delete(tt.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join("/tmp/bplustree")
	B := NewBPlusTree(path, false)
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("Failed to remove directory %s: %v", path, err)
			return
		}
	}()

	tests := []struct {
		name  string
		setup func()
		key   []byte
		want  *data.LogRecordPos
	}{
		{
			name:  "TestBPlusTree_Get_not_exist",
			setup: nil,
			key:   []byte("not_exist"),
			want:  nil,
		},
		{
			name: "TestBPlusTree_Get_exist",
			setup: func() {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
			},
			key:  []byte("key2"),
			want: &data.LogRecordPos{Offset: 200, Fid: 20},
		},
		{
			name: "TestBPlusTree_Put_And_Delete_And_Get",
			setup: func() {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
				B.Delete([]byte("key2"))
			},
			key:  []byte("key2"),
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			if got := B.Get(tt.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBPlusTree_Iterator(t *testing.T) {
	deletePath := func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("Failed to remove directory %s: %v", path, err)
			return
		}
	}

	tests := []struct {
		name    string
		setup   func(B *BPlusTree)
		reverse bool
	}{
		{
			name:    "TestBPlusTree_Iterator_empty",
			setup:   nil,
			reverse: false,
		},
		{
			name: "TestBPlusTree_iterator_all_key",
			setup: func(B *BPlusTree) {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
			},
			reverse: false,
		},
		{
			name: "TestBPlusTree_iterator_all_key",
			setup: func(B *BPlusTree) {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
			},
			reverse: true,
		},
		{
			name: "TestBPlusTree_iterator_one_key_delete",
			setup: func(B *BPlusTree) {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
				B.Delete([]byte("key2"))
			},
			reverse: false,
		},
		{
			name: "TestBPlusTree_iterator_one_key_delete",
			setup: func(B *BPlusTree) {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
				B.Delete([]byte("key2"))
			},
			reverse: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("/tmp/bplustree" + strconv.Itoa(rand.IntN(10)))
			if _, err := os.Stat(path); os.IsNotExist(err) {
				err = os.Mkdir(path, 0777)
				if err != nil {
					return
				}
			}
			B := NewBPlusTree(path, false)
			if tt.setup != nil {
				tt.setup(B)
			}
			iterator := B.Iterator(tt.reverse)
			for iterator.Rewind(); iterator.Valid(); iterator.Next() {
				t.Log(string(iterator.Key()), iterator.Value())
			}
			deletePath(path)
		})
	}
}

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join("/tmp/bplustree")
	B := NewBPlusTree(path, false)
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("Failed to remove directory %s: %v", path, err)
			return
		}
	}()

	tests := []struct {
		name string
		key  []byte
		pos  *data.LogRecordPos
		want bool
	}{
		{
			name: "TestBPlusTree_Put_key1",
			key:  []byte("key1"),
			pos:  &data.LogRecordPos{Offset: 100, Fid: 10},
			want: true,
		},
		{
			name: "TestBPlusTree_Put_key2",
			key:  []byte("key2"),
			pos:  &data.LogRecordPos{Offset: 200, Fid: 20},
			want: true,
		},
		{
			name: "TestBPlusTree_Put_key3",
			key:  []byte("key3"),
			pos:  &data.LogRecordPos{Offset: 300, Fid: 30},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := B.Put(tt.key, tt.pos); got != tt.want {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBPlusTree_Size(t *testing.T) {
	deletePath := func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Errorf("Failed to remove directory %s: %v", path, err)
			return
		}
	}

	tests := []struct {
		name  string
		setup func(B *BPlusTree)
		want  int
	}{
		{
			name:  "TestBPlusTree_Size_empty",
			setup: nil,
			want:  0,
		},
		{
			name: "TestBPlusTree_Size_one_key",
			setup: func(B *BPlusTree) {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
			},
			want: 3,
		},
		{
			name: "TestBPlusTree_Size_one_key_delete",
			setup: func(B *BPlusTree) {
				B.Put([]byte("key1"), &data.LogRecordPos{Offset: 100, Fid: 10})
				B.Put([]byte("key2"), &data.LogRecordPos{Offset: 200, Fid: 20})
				B.Put([]byte("key3"), &data.LogRecordPos{Offset: 300, Fid: 30})
				B.Delete([]byte("key2"))
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("/tmp/bplustree" + strconv.Itoa(rand.IntN(10)))
			if _, err := os.Stat(path); os.IsNotExist(err) {
				err = os.Mkdir(path, 0777)
				if err != nil {
					return
				}
			}
			B := NewBPlusTree(path, false)
			if tt.setup != nil {
				tt.setup(B)
			}
			if got := B.Size(); got != tt.want {
				t.Errorf("Size() = %v, want %v", got, tt.want)
			}
			deletePath(path)
		})
	}
}
