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
