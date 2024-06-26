package go_kv

import (
	"errors"
	"go-kv/data"
	"go-kv/index"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	SeqNoKey = "seq.no"
)

// DB represents a key-value store.
type DB struct {
	options    Options
	mut        *sync.RWMutex
	activeFile *data.DataFile            // active data file
	olderFiles map[uint32]*data.DataFile // older data files, only read
	index      index.Indexer             // memory index

	loadDataFileIds []int // created by loadDataFiles(), only used for loadIndexFromDataFiles(), not used in other methods

	seqNo uint64 // transaction sequence number for log records

	isMerging bool // flag for merging data files

	seqNoFileExists bool // flag for seqNoFile existence
	isInitial       bool // flag for initial database creation
}

// Open opens a (bitcask) database with the given options.
// It returns an error if the options are invalid.
func Open(options Options) (*DB, error) {
	// check options for validity
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// check if data directory exists, create if not
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// init DB
	db := &DB{
		options:    options,
		mut:        new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
	}

	// load merge data files
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// load data files from disk
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+Tree index type, not load hint file and load memory index from data files
	if options.IndexType != BPlusTree {
		// load hint file
		if err = db.loadIndexFormHintFile(); err != nil {
			return nil, err
		}

		// load memory index from data files
		if err = db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	// get transaction sequence number , if index type is B+Tree, seqNo is 0
	if options.IndexType == BPlusTree {
		if err = db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// checkOptions checks the options for validity.
// It returns an error if the options are invalid.
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database DirPath is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database DataFileSize is not positive")
	}
	return nil
}

// loadDataFiles loads all data files from disk.
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			continue
		}
		splitNames := strings.Split(entry.Name(), ".")
		fileId, err := strconv.Atoi(splitNames[0])
		// if file name is not a valid number, return error
		if err != nil {
			return ErrDataDirectoryCorrupted
		}
		fileIds = append(fileIds, fileId)
	}

	// sort fileIds in ascending order
	sort.Ints(fileIds)
	db.loadDataFileIds = fileIds

	// load data files from disk
	for idx, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if idx == len(fileIds)-1 {
			// set active data file to the latest one
			db.activeFile = dataFile
		} else {
			// set older data files
			db.olderFiles[dataFile.FileId] = dataFile
		}
	}

	return nil
}

// loadIndexFromDataFiles loads the memory index from data files.
func (db *DB) loadIndexFromDataFiles() error {
	// if no data file found, return nil
	if len(db.loadDataFileIds) == 0 {
		return nil
	}

	// is merging data files
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}

		hasMerge = true
		nonMergeFileId = fid
	}

	// define a function to update memory index from a log record
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) error {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			return ErrIndexUpdateFailed
		}
		return nil
	}

	// temporary storage for transactional log records
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	currentSeqNo := nonTransactionalSeqNo

	// load all data files, handle file order records content
	for idx, fId := range db.loadDataFileIds {
		var fieldId = uint32(fId)
		// if merging data files(file id < non-merge file id), skip all data files before non-merge file id
		if hasMerge && fieldId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fieldId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fieldId]
		}

		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil && err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			// construct log record from record content, update memory index
			logRecordPos := &data.LogRecordPos{
				Fid:    fieldId,
				Offset: offset,
			}

			// parse log record key to extract sequence number
			seqNo, origKey := parseLogRecordKey(record.Key)
			if seqNo == nonTransactionalSeqNo {
				// non-transactional log record, update memory index directly
				if err = updateIndex(origKey, record.Type, logRecordPos); err != nil {
					return err
				}
			} else {
				// transactional log record, store in temporary storage
				if record.Type == data.LogRecordTxFinished {
					for _, trRecord := range transactionRecords[seqNo] {
						if err = updateIndex(trRecord.Record.Key, trRecord.Record.Type, trRecord.Pos); err != nil {
							return err
						}
					}
					delete(transactionRecords, seqNo)
				} else {
					record.Key = origKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: record,
						Pos:    logRecordPos,
					})
				}
			}

			// update current transaction sequence number
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// update offset for next iteration
			offset += size
		}

		// update active data file writeOff
		if idx == len(db.loadDataFileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// update transaction sequence number
	db.seqNo = currentSeqNo
	return nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mut.Lock()
	defer db.mut.Unlock()
	// index close
	if err := db.index.Close(); err != nil {
		return err
	}

	// save current transaction sequence number to seqNoFile
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(SeqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encodeLogRecord, _ := data.EncodeLogRecord(record)
	if err = seqNoFile.Write(encodeLogRecord); err != nil {
		return err
	}
	if err = seqNoFile.Sync(); err != nil {
		return err
	}

	// close active data file
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// close older data files
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync flushes the database to disk.
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mut.Lock()
	defer db.mut.Unlock()

	// flush active data file
	return db.activeFile.Sync()
}

// Put inserts a key-value pair into the database.
// It returns an error if the key is empty.
// It returns an error if the index update failed.
// It returns an error if there is an error writing to disk.
func (db *DB) Put(key, value []byte) error {
	// key and value validation
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// new log record
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionalSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log record to active data file
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// update memory index
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Delete deletes a key-value pair from the database.
func (db *DB) Delete(key []byte) error {
	// key validation
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// validate key existence
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// new log record
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionalSeqNo),
		Type: data.LogRecordDeleted,
	}

	// append log record to active data file
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// update memory index
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// appendLogRecordWithLock appends a log record to the active data file.
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mut.Lock()
	defer db.mut.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord appends a log record to the active data file.
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// if active file is full, create a new one
	// if active file is not full, append log record to active file
	if db.activeFile == nil {
		// create new data file
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// encode log record
	encRecord, size := data.EncodeLogRecord(logRecord)
	// if active file is full, create a new one
	// if active file is not full, append log record to active file
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// persistence logic, sync current memory buffer to disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// current active file becomes older file
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// create new data file
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	// write log record to active file
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// if you need immediately flush to disk, call Sync()
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// construct log record position with memory index, return it
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// setActiveFile sets the active data file to the latest one.
// It returns an error if there is no data file to set as active.
// Access this method needs db.mut is required.
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// open new data dataFile
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

// Get retrieves the value of a key from the database.
// It returns an error if the key is empty.
// It returns an error if the index lookup failed.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mut.RLock()
	defer db.mut.RUnlock()

	// key validation
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// lookup log record position from memory index
	logRecordPos := db.index.Get(key)
	// if not found key, return error
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	/*// lookup log record from data file identified by logRecordPos
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// if data file not found, return error
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// read log record from data file offset by logRecordPos
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// log record validation, if log record is deleted, return error
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil*/
	return db.getValueByPosition(logRecordPos)
}

// getValueByPosition retrieves the value of a key from the database by log record position.
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// lookup log record from data file identified by logRecordPos
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// if data file not found, return error
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// read log record from data file offset by logRecordPos
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// log record validation, if log record is deleted, return error
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// ListKeys retrieves all keys in the database.
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(true)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold applies a function to all key-value pairs in the database.
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mut.RLock()
	defer db.mut.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(key, value) {
			break
		}
	}

	return nil
}

// loadSeqNo loads the current transaction sequence number from seqNoFile.
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); err != nil {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}
