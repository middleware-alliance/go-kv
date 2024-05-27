package index

import (
	art "github.com/plar/go-adaptive-radix-tree"
	"go-kv/data"
	"reflect"
	"testing"
)

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "TestAdaptiveRadixTree_Delete_not_exist_key",
			fn: func() {
				exist := art.Delete([]byte("not exist key"))
				if exist {
					t.Errorf("Delete() = %v, want %v", exist, false)
				}
			},
		},
		{
			name: "TestAdaptiveRadixTree_Delete_key",
			fn: func() {
				art.Put([]byte("key"), &data.LogRecordPos{Fid: 1, Offset: 10})
				exist := art.Delete([]byte("key"))
				if !exist {
					t.Errorf("Delete() = %v, want %v", exist, true)
				}
				var got any = art.Get([]byte("key"))
				pos, ok := got.(*data.LogRecordPos)
				if ok && pos != nil {
					t.Errorf("Get() = %v, want %v", pos, nil)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	tests := []struct {
		name   string
		key    []byte
		set    func()
		assert func(any)
	}{
		{
			name: "TestAdaptiveRadixTree_Get_key1",
			key:  []byte("key1"),
			set: func() {
				art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 10})
			},
			assert: func(got any) {
				if got == nil {
					t.Errorf("Get() = %v", got)
				}
			},
		},
		{
			name: "TestAdaptiveRadixTree_Get_not_exist_key",
			key:  []byte("not_exist_key"),
			assert: func(got any) {
				pos, ok := got.(*data.LogRecordPos)
				if ok && pos != nil {
					t.Errorf("Get() = %v", got)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set != nil {
				tt.set()
			}
			got := art.Get(tt.key)
			tt.assert(got)
		})
	}
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "TestAdaptiveRadixTree_Iterator",
			fn: func() {
				art := NewART()
				art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 10})
				art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 20})
				art.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 30})
				it := art.Iterator(true)
				for it.Rewind(); it.Valid(); it.Next() {
					key := it.Key()
					pos := it.Value()
					t.Log(string(key), pos)
				}
				it.Close()
			},
		},
		{
			name: "TestAdaptiveRadixTree_Iterator",
			fn: func() {
				art := NewART()
				art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 10})
				art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 20})
				art.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 30})
				it := art.Iterator(false)
				for it.Rewind(); it.Valid(); it.Next() {
					key := it.Key()
					pos := it.Value()
					t.Log(string(key), pos)
				}
				it.Close()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn()
		})
	}
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	tests := []struct {
		name string
		key  []byte
		pos  *data.LogRecordPos
	}{
		{
			name: "TestAdaptiveRadixTree_Put_key1",
			key:  []byte("key1"),
			pos: &data.LogRecordPos{
				Fid:    1,
				Offset: 10,
			},
		},
		{
			name: "TestAdaptiveRadixTree_Put_key2",
			key:  []byte("key2"),
			pos: &data.LogRecordPos{
				Fid:    1,
				Offset: 20,
			},
		},
		{
			name: "TestAdaptiveRadixTree_Put_key3",
			key:  []byte("key3"),
			pos: &data.LogRecordPos{
				Fid:    1,
				Offset: 30,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := art.Put(tt.key, tt.pos); !got {
				t.Errorf("Put() = %s", tt.key)
			}
		})
	}
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "TestAdaptiveRadixTree_Size",
			fn: func() {
				art := NewART()
				if got := art.Size(); got != 0 {
					t.Errorf("Size() = %v, want %v", got, 0)
				}
				art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 10})
				art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 20})
				art.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 30})
				if got := art.Size(); got != 3 {
					t.Errorf("Size() = %v, want %v", got, 3)
				}
			},
		},
		{
			name: "TestAdaptiveRadixTree_Size",
			fn: func() {
				art := NewART()
				if got := art.Size(); got != 0 {
					t.Errorf("Size() = %v, want %v", got, 0)
				}
				art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 10})
				art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 20})
				art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 30})
				if got := art.Size(); got != 2 {
					t.Errorf("Size() = %v, want %v", got, 2)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn()
		})
	}
}

func Test_artIterator_Close(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			artIter.Close()
		})
	}
}

func Test_artIterator_Key(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := artIter.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_artIterator_Next(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			artIter.Next()
		})
	}
}

func Test_artIterator_Rewind(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			artIter.Rewind()
		})
	}
}

func Test_artIterator_Seek(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			artIter.Seek(tt.args.key)
		})
	}
}

func Test_artIterator_Valid(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := artIter.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_artIterator_Value(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   *data.LogRecordPos
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			artIter := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := artIter.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newArtIterator(t *testing.T) {
	type args struct {
		tree    art.Tree
		reverse bool
	}
	tests := []struct {
		name string
		args args
		want *artIterator
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newArtIterator(tt.args.tree, tt.args.reverse); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newArtIterator() = %v, want %v", got, tt.want)
			}
		})
	}
}
