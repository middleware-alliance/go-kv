package go_kv

import (
	"encoding/binary"
	"go-kv/data"
	"sync"
	"sync/atomic"
)

const nonTransactionalSeqNo uint64 = 0

var txFinKey = []byte("tx-fin")

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // key -> log record to be written to disk
}

// NewWriteBatch creates a new WriteBatch object with the given options.
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       options,
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put adds a key-value pair to the WriteBatch.
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// temporary storage for the log record
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: value,
	}
	return nil
}

// Delete adds a delete operation to the WriteBatch.
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// check if the key exists in the index
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		// key not found in index, nothing to delete
		if wb.pendingWrites[string(key)] != nil {
			// key found in pending writes, mark it as deleted
			delete(wb.pendingWrites, string(key))
			return nil
		}
		// key not found in pending writes, nothing to delete
		return nil
	}

	// temporary storage for the log record
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	return nil
}

// Commit writes the pending writes to disk.
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// write the pending writes to disk
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// serial commit
	wb.db.mut.Lock()
	defer wb.db.mut.Unlock()

	// get current transaction id
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// write the log records to disk
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {
		// encode the key with the sequence number
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}

		positions[string(logRecord.Key)] = logRecordPos
	}

	// write the transaction finish record to disk
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txFinKey, seqNo),
		Type: data.LogRecordTxFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// sync the active file if required
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// update the index with the new positions
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		switch record.Type {
		case data.LogRecordDeleted:
			wb.db.index.Delete(record.Key)
		case data.LogRecordNormal:
			wb.db.index.Put(record.Key, pos)
		}
	}

	// clean up the pending writes
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// logRecordKeyWithSeq returns the key of a log record with the given sequence number.
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, len(key)+n)
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// parseLogRecordKey parses the sequence number and original key from a log record key.
func parseLogRecordKey(key []byte) (seqNo uint64, origKey []byte) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return seqNo, realKey
}
