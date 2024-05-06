package go_kv

import (
	"errors"
	"go-kv/utils"
	"os"
	"testing"
)

func TestWriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir

	tests := []struct {
		name string
		pre  func() *DB
		post func(*DB)
	}{
		{
			name: "TestWriteBatch not commited read",
			pre: func() *DB {
				db, err := Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}
				return db
			},
			post: func(db *DB) {
				defer destroyDB(db)
				// write batch and not commited read
				batch := db.NewWriteBatch(DefaultWriteBatchOptions)
				err := batch.Put(utils.GetTestKey(1), utils.RandomValue(10))
				if err != nil {
					t.Error(err)
				}
				err = batch.Delete(utils.GetTestKey(2))
				if err != nil {
					t.Error(err)
				}

				_, err = db.Get(utils.GetTestKey(1))
				if !errors.Is(err, ErrKeyNotFound) {
					t.Error(err)
				}
			},
		},
		{
			name: "TestWriteBatch commited read",
			pre: func() *DB {
				db, err := Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}
				return db
			},
			post: func(db *DB) {
				defer destroyDB(db)
				// write batch and not commited read
				batch := db.NewWriteBatch(DefaultWriteBatchOptions)
				err := batch.Put(utils.GetTestKey(1), utils.RandomValue(10))
				if err != nil {
					t.Error(err)
				}
				err = batch.Delete(utils.GetTestKey(2))
				if err != nil {
					t.Error(err)
				}

				_, err = db.Get(utils.GetTestKey(1))
				if !errors.Is(err, ErrKeyNotFound) {
					t.Error(err)
				}

				err = batch.Commit()
				if err != nil {
					t.Error(err)
				}

				value, err := db.Get(utils.GetTestKey(1))
				if err != nil {
					t.Error(err)
				}
				t.Log(string(value))
			},
		},
		{
			name: "TestWriteBatch delete commited read",
			pre: func() *DB {
				db, err := Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}
				return db
			},
			post: func(db *DB) {
				defer destroyDB(db)
				// write batch and not commited read
				batch := db.NewWriteBatch(DefaultWriteBatchOptions)
				err := batch.Put(utils.GetTestKey(1), utils.RandomValue(10))
				if err != nil {
					t.Error(err)
				}
				err = batch.Delete(utils.GetTestKey(1))
				if err != nil {
					t.Error(err)
				}

				err = batch.Commit()
				if err != nil {
					t.Error(err)
				}

				value, err := db.Get(utils.GetTestKey(1))
				if !errors.Is(err, ErrKeyNotFound) {
					t.Error(err)
				}
				if value != nil {
					t.Error("value should be nil")
				}
			},
		},
		{
			name: "TestWriteBatch delete commited restart read",
			pre: func() *DB {
				db, err := Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}
				return db
			},
			post: func(db *DB) {
				defer destroyDB(db)
				err := db.Put(utils.GetTestKey(1), utils.RandomValue(10))
				if err != nil {
					t.Error(err)
				}

				// write batch and not commited read
				batch := db.NewWriteBatch(DefaultWriteBatchOptions)
				err = batch.Put(utils.GetTestKey(2), utils.RandomValue(10))
				if err != nil {
					t.Error(err)
				}
				err = batch.Delete(utils.GetTestKey(1))
				if err != nil {
					t.Error(err)
				}

				err = batch.Commit()
				if err != nil {
					t.Error(err)
				}

				err = batch.Put(utils.GetTestKey(3), utils.RandomValue(10))
				if err != nil {
					t.Error(err)
				}
				err = batch.Commit()
				if err != nil {
					t.Error(err)
				}
				if db.seqNo != 2 {
					t.Error("seqNo should be 2")
				}

				// restart db
				err = db.Close()
				if err != nil {
					t.Error(err)
				}
				// reopen db
				db, err = Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}

				value, err := db.Get(utils.GetTestKey(1))
				if !errors.Is(err, ErrKeyNotFound) {
					t.Error(err)
				}
				if value != nil {
					t.Error("value should be nil")
				}
			},
		},
		{
			name: "TestWriteBatch not finished commit read",
			pre: func() *DB {
				db, err := Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}
				return db
			},
			post: func(db *DB) {
				defer destroyDB(db)

				// write batch and not commited read
				batch := db.NewWriteBatch(DefaultWriteBatchOptions)
				for i := 0; i < 100; i++ {
					err := batch.Put(utils.GetTestKey(i), utils.RandomValue(10))
					if err != nil {
						t.Error(err)
					}
				}

				// not commited
				/*err = batch.Commit()
				if err != nil {
					t.Error(err)
				}*/

				// restart db
				err := db.Close()
				if err != nil {
					t.Error(err)
				}
				// reopen db
				db, err = Open(opts)
				if err != nil {
					t.Error(err)
				}
				if db == nil {
					t.Error("db is nil")
				}

				keys := db.ListKeys()

				if len(keys) != 0 {
					t.Error("keys should be empty")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.post(tt.pre())
		})
	}
}
