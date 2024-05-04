package go_kv

import (
	"go-kv/utils"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer destroyDB(db)
	iterator := db.NewIterator(DefaultIteratorOptions)
	if iterator == nil {
		t.Fatal("iterator is nil")
	}
	defer iterator.Close()
	if iterator.Valid() {
		t.Fatal("iterator should be invalid")
	}
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer destroyDB(db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}

	iterator := db.NewIterator(DefaultIteratorOptions)
	if iterator == nil {
		t.Fatal("iterator is nil")
	}
	defer iterator.Close()
	if !iterator.Valid() {
		t.Fatal("iterator should be invalid")
	}
	t.Log(iterator.Valid())
	t.Log(string(iterator.Key()))
	value, err := iterator.Value()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(value))
	if string(value) != string(utils.GetTestKey(10)) {
		t.Fatal("value should be equal to the key")
	}
}

func TestDB_Iterator_Multiple_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer destroyDB(db)

	err = db.Put([]byte("key1"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key3"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key6"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key4"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key2"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key5"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key7"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key8"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key11"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key111"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key1111"), utils.GetTestKey(10))
	if err != nil {
		t.Fatal(err)
	}

	// Test forward iterator
	iterator := db.NewIterator(DefaultIteratorOptions)
	if iterator == nil {
		t.Fatal("iterator is nil")
	}
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		t.Log("key = " + string(iterator.Key()))
		value, err := iterator.Value()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("value = " + string(value))
	}
	t.Log("")

	// Test seek iterator
	iterator.Rewind()
	for iterator.Seek([]byte("key5")); iterator.Valid(); iterator.Next() {
		t.Log("key = " + string(iterator.Key()))
		value, err := iterator.Value()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("value = " + string(value))
	}
	t.Log("")

	// Test reverse iterator
	iteratorReverse := db.NewIterator(IteratorOptions{
		Reverse: true,
	})
	defer iteratorReverse.Close()
	for iteratorReverse.Rewind(); iteratorReverse.Valid(); iteratorReverse.Next() {
		t.Log("key = " + string(iteratorReverse.Key()))
		value, err := iteratorReverse.Value()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("value = " + string(value))
	}
	t.Log("")

	// Test seek iterator
	iteratorReverse.Rewind()
	for iteratorReverse.Seek([]byte("key5")); iteratorReverse.Valid(); iteratorReverse.Next() {
		t.Log("key = " + string(iteratorReverse.Key()))
		value, err := iteratorReverse.Value()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("value = " + string(value))
	}
	t.Log("")

	// Test iterator with prefix
	iteratorPrefix := db.NewIterator(IteratorOptions{
		Prefix: []byte("key1"),
	})
	defer iteratorPrefix.Close()
	for iteratorPrefix.Rewind(); iteratorPrefix.Valid(); iteratorPrefix.Next() {
		t.Log("key = " + string(iteratorPrefix.Key()))
		value, err := iteratorPrefix.Value()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("value = " + string(value))
	}

}
