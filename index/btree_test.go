package index

import (
	"github.com/google/btree"
	"go-kv/data"
	"reflect"
	"sync"
	"testing"
)

type fields struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

var commonFields fields

func resetFields() {
	commonFields = fields{
		tree: btree.New(3),
		lock: new(sync.RWMutex),
	}
}

func TestBTree_Delete(t *testing.T) {
	type args struct {
		key []byte
	}
	resetFields()
	commonFields.tree.ReplaceOrInsert(&Item{
		Key: []byte("key1"),
		pos: &data.LogRecordPos{
			Fid:    1,
			Offset: 100,
		},
	})
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			"cast1",
			commonFields,
			args{},
			false,
		},
		{
			"cast2",
			commonFields,
			args{
				[]byte("key1"),
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			B := BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := B.Delete(tt.args.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Get(t *testing.T) {
	type args struct {
		key []byte
	}
	resetFields()
	commonFields.tree.ReplaceOrInsert(&Item{
		Key: []byte("key1"),
		pos: &data.LogRecordPos{
			Fid:    1,
			Offset: 100,
		},
	})

	commonFields.tree.ReplaceOrInsert(&Item{
		Key: []byte("key1"),
		pos: &data.LogRecordPos{
			Fid:    1,
			Offset: 101,
		},
	})

	tests := []struct {
		name   string
		fields fields
		args   args
		want   *data.LogRecordPos
	}{
		{
			"cast1",
			commonFields,
			args{},
			nil,
		},
		{
			"cast2",
			commonFields,
			args{
				[]byte("key1"),
			},
			&data.LogRecordPos{
				Fid:    1,
				Offset: 101,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			B := BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := B.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Put(t *testing.T) {
	type args struct {
		key []byte
		pos *data.LogRecordPos
	}
	resetFields()
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			"cast1",
			commonFields,
			args{
				[]byte("key1"),
				&data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				},
			},
			false,
		},
		{
			"cast2",
			commonFields,
			args{
				[]byte("key1"),
				&data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			B := BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := B.Put(tt.args.key, tt.args.pos); got != tt.want {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Iterator(t *testing.T) {
	tests := []struct {
		name    string
		pre     func() *BTree
		reverse bool
		valid   func(t *testing.T, it Iterator)
	}{
		{
			name:    "BTree is empty",
			reverse: false,
			pre: func() *BTree {
				return NewBTree()
			},
			valid: func(t *testing.T, it Iterator) {
				if it.Valid() {
					t.Errorf("Valid() = %v", it)
				}
			},
		},
		{
			name:    "BTree has one item",
			reverse: false,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				if !it.Valid() {
					t.Errorf("Valid() = %v", it)
				}
				if it.Key() == nil || it.Value() == nil {
					t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
				}
				it.Next()
				if it.Valid() {
					t.Errorf("Valid() = %v", it)
				}
			},
		},
		{
			name:    "BTree has many item",
			reverse: false,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key2"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key3"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				for it.Rewind(); it.Valid(); it.Next() {
					if it.Key() == nil || it.Value() == nil {
						t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
					}
					t.Log(string(it.Key()), it.Value())
				}
			},
		},
		{
			name:    "BTree has many item reverse",
			reverse: false,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key2"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key3"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				for it.Rewind(); it.Valid(); it.Next() {
					if it.Key() == nil || it.Value() == nil {
						t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
					}
					t.Log(string(it.Key()), it.Value())
				}
			},
		},
		{
			name:    "BTree has many item seek",
			reverse: true,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key2"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key3"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				for it.Seek([]byte("key2")); it.Valid(); it.Next() {
					if it.Key() == nil || it.Value() == nil {
						t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
					}
					t.Log(string(it.Key()), it.Value())
				}
			},
		},
		{
			name:    "BTree has many item seek",
			reverse: true,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key2"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key3"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				for it.Seek([]byte("key2")); it.Valid(); it.Next() {
					if it.Key() == nil || it.Value() == nil {
						t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
					}
					t.Log(string(it.Key()), it.Value())
				}
			},
		},
		{
			name:    "BTree has many item seek",
			reverse: true,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key2"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key3"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				for it.Seek([]byte("00")); it.Valid(); it.Next() {
					if it.Key() == nil || it.Value() == nil {
						t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
					}
					t.Log(string(it.Key()), it.Value())
				}
			},
		},
		{
			name:    "BTree has many item seek",
			reverse: true,
			pre: func() *BTree {
				bt := NewBTree()
				bt.Put([]byte("key1"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key2"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				bt.Put([]byte("key3"), &data.LogRecordPos{
					Fid:    1,
					Offset: 100,
				})
				return bt
			},
			valid: func(t *testing.T, it Iterator) {
				for it.Seek([]byte("zz")); it.Valid(); it.Next() {
					if it.Key() == nil || it.Value() == nil {
						t.Errorf("Key() = %v, Value() = %v", it.Key(), it.Value())
					}
					t.Log(string(it.Key()), it.Value())
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := tt.pre()
			it := bt.Iterator(tt.reverse)
			tt.valid(t, it)
		})
	}
}
