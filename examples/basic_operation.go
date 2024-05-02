package main

import (
	"fmt"
	go_kv "go-kv"
)

func main() {
	opts := go_kv.DefaultOptions
	opts.DirPath = "/tmp/go-kv"
	db, err := go_kv.Open(opts)
	if err != nil {
		panic(err)
	}

	// Set a key-value pair
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	// Get the value of a key
	value, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = " + string(value)) // Output: bitcask

	// Delete a key-value pair
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	// Close the database
}
