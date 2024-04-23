package go_kv

import (
	"errors"
	"go-kv/data"
	"go-kv/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB represents a key-value store.
type DB struct {
	options    Options
	mut        *sync.RWMutex
	activeFile *data.DataFile            // active data file
	olderFiles map[uint32]*data.DataFile // older data files, only read
	index      index.Indexer             // memory index

	loadDataFileIds []int // created by loadDataFiles(), only used for loadIndexFromDataFiles(), not used in other methods
}

// Open opens a (bitcask) database with the given options.
// It returns an error if the options are invalid.
func Open(options Options) (*DB, error) {
	// check options for validity
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// check if data directory exists, create if not
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// init DB
	db := &DB{
		options:    options,
		mut:        new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// load data files from disk
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// load memory index from data files
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
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

	// load all data files, handle file order records content
	for idx, fId := range db.loadDataFileIds {
		var fieldId = uint32(fId)
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
			var ok bool
			if record.Type == data.LogRecordDeleted {
				ok = db.index.Delete(record.Key)
			} else {
				ok = db.index.Put(record.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}

			// update offset for next iteration
			offset += size
		}

		// update active data file writeOff
		if idx == len(db.loadDataFileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
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
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log record to active data file
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// update memory index
	if ok := db.index.Put(key, pos); ok {
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
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// append log record to active data file
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// update memory index
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// appendLogRecord appends a log record to the active data file.
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mut.Lock()
	defer db.mut.Unlock()

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
