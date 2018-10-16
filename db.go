package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"syscall"
)

type Database interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
	Close() error
}

type table struct {
	store map[string]string
	mut   *sync.Mutex

	pageSize int
	filename string
	fd       int
	file     *os.File
	data     []byte
}

func NewDatabase(filename string) (Database, error) {
	t := &table{
		store: make(map[string]string),
		mut:   &sync.Mutex{},

		filename: filename,
		pageSize: os.Getpagesize(),
	}

	err := t.openFile()
	if err != nil {
		return nil, err
	}

	fileInfo, err := os.Stat(t.filename)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file \"%s\": %s", filename, err.Error())
	}

	size := int(fileInfo.Size())
	if size == 0 {
		size = t.pageSize
	}

	err = t.mmapFile(size)
	if err != nil {
		return nil, err
	}

	// err = syscall.Madvise(t.data, syscall.MADV_SEQUENTIAL|syscall.MADV_WILLNEED)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	err = json.Unmarshal(t.data, &t.store)
	if err != nil {
		return nil, fmt.Errorf("failed to load data from file \"%s\": %s", filename, err.Error())
	}

	return t, nil
}

func (t *table) openFile() error {
	file, err := os.OpenFile(t.filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("failed to open file for database: %s", err.Error())
	}

	t.fd = int(file.Fd())
	t.file = file

	return nil
}

func (t *table) mmapFile(size int) error {
	var err error
	t.data, err = syscall.Mmap(
		t.fd,
		0,
		size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("failed to mmap file \"%s\": %s", t.filename, err.Error())
	}

	return nil
}

func (t *table) Get(key string) (string, error) {
	var val string
	var ok bool

	t.mut.Lock()
	val, ok = t.store[key]
	t.mut.Unlock()
	if !ok {
		return "", fmt.Errorf("failed get for key \"%s\": no value", key)
	}

	return val, nil
}

func (t *table) Set(key string, value string) error {
	t.mut.Lock()
	t.store[key] = value

	err := t.writeToFile()

	t.mut.Unlock()

	return err
}

func (t *table) Delete(key string) error {
	t.mut.Lock()
	if _, ok := t.store[key]; !ok {
		t.mut.Unlock()
		return fmt.Errorf("failed to delete for key \"%s\": no value", key)
	}

	delete(t.store, key)
	t.writeToFile()
	t.mut.Unlock()
	return nil
}

func (t *table) writeToFile() error {
	data, err := json.Marshal(t.store)
	if err != nil {
		return fmt.Errorf("failed to marshall datastore: %s", err.Error())
	}

	size := len(data)
	if len(t.data) < size {
		err = t.file.Close()
		if err != nil {
			return fmt.Errorf("failed to close file for resizing: %s", err.Error())
		}

		t.openFile()

		err := syscall.Ftruncate(t.fd, int64(size))
		if err != nil {
			return fmt.Errorf("failed to resize file: %s", err.Error())
		}

		err = t.mmapFile(size)
		if err != nil {
			return fmt.Errorf("failed to mmap after resize: %s", err.Error())
		}
	}

	copy(t.data, data)

	return nil
}

func (t *table) Close() error {
	err := t.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close database file: %s", err.Error())
	}

	err = syscall.Munmap(t.data)
	if err != nil {
		return fmt.Errorf("failed to unmap memory: %s", err.Error())
	}

	return nil
}
