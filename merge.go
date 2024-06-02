package go_kv

import (
	"go-kv/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge merges the data from the source DB into the destination DB.
// clear unused data in destination DB.
// generate Hint file.
func (db *DB) Merge() error {
	// validate source DB
	if db.activeFile == nil {
		return nil
	}

	db.mut.Lock()
	defer func() {
		db.isMerging = false
	}()

	// check if merge is in progress
	if db.isMerging {
		db.mut.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true

	// sync active file to disk
	if err := db.activeFile.Sync(); err != nil {
		db.mut.Unlock()
		return err
	}

	// let current active file to old file
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// open new active file
	if err := db.setActiveFile(); err != nil {
		db.mut.Unlock()
		return err
	}
	// recorde shelfId and fileId of new active file
	nonMergeFileId := db.activeFile.FileId

	// get all need merge files
	var needMergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		needMergeFiles = append(needMergeFiles, file)
	}
	db.mut.Unlock()

	// order need merge files by file id, ascending order
	sort.Slice(needMergeFiles, func(i, j int) bool {
		return needMergeFiles[i].FileId < needMergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// if merge directory exists, remove it
	if _, err := os.Stat(mergePath); err == nil {
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// create merge directory
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// open a new tmp db
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// open Hint file index
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// iterate over need merge files and merge them into new active file
	for _, dataFile := range needMergeFiles {
		var offset int64 = 0
		for {
			// read data from source file
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// parse data get real key
			_, origKey := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(origKey)
			// compare log record position with index position
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// clear transaction prefix
				logRecord.Key = logRecordKeyWithSeq(origKey, nonTransactionalSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// write current position index to Hint file
				err = hintFile.WriteHintRecord(origKey, pos)
				if err != nil {
					return err
				}
			}
			// move offset to next record
			offset += size
		}
	}

	// sync merge db to disk
	if err = hintFile.Sync(); err != nil {
		return err
	}
	if err = mergeDB.Sync(); err != nil {
		return err
	}

	// write merge finished key to new active file
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err = mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err = mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// getMergePath returns the path of the merge directory.
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// loadMergeFiles loads the merge files from the merge directory.
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// check if merge directory exists
	if _, err := os.Stat(mergePath); err != nil {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// check if merge is in progress, if not finished, return
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// if merge is in progress, return
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// delete old data files
	var fileId uint32
	for ; fileId <= nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err != nil {
			if err = os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// rename merge files to data files
	for _, fileName := range mergeFileNames {
		srcFileName := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err = os.Rename(srcFileName, destPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// loadIndexFormHintFile loads hint file to load index.
func (db *DB) loadIndexFormHintFile() error {
	// check if hint file exists
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// open hint index file
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// read file index
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset) // read hint record
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// decode log record index position
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)

		// move offset to next record
		offset += size
	}

	return nil
}
