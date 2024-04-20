package data

// LogRecordPos represents the position of a log record in a file.
type LogRecordPos struct {
	Fid    uint32 // file id of the log file
	Offset int64  // offset in the file
}
