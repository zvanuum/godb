package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
)

type Database interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
}

type table struct {
	store map[string]string
	mut   *sync.Mutex

	filename string
	fd       int
	file     *os.File
	data     []byte
}

func NewDatabase(filename string) (Database, error) {
	t := &table{
		store:    make(map[string]string),
		mut:      &sync.Mutex{},
		filename: filename,
	}

	err := t.openFile()
	if err != nil {
		return nil, err
	}

	fstat, err := os.Stat(t.filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to stat file %s: %s", filename, err.Error())
	}

	size := int(fstat.Size())
	if size == 0 {
		size = 256
	}

	err = t.mmapFile(size)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *table) openFile() error {
	file, err := os.OpenFile(t.filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("Failed to open file for database: %s", err.Error())
	}

	t.fd = int(file.Fd())
	t.file = file
	// defer t.file.Close()

	return nil
}

func (t *table) mmapFile(size int) error {
	data, err := syscall.Mmap(
		t.fd,
		0,
		size,
		syscall.PROT_WRITE|syscall.PROT_READ,
		syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("Failed to mmap file %s: %s", t.filename, err.Error())
	}

	t.data = data

	return nil
}

func (t *table) Get(key string) (string, error) {
	var val string
	var ok bool

	t.mut.Lock()
	val, ok = t.store[key]
	t.mut.Unlock()
	if !ok {
		return "", fmt.Errorf("No value exists to get for key %s", key)
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
		return fmt.Errorf("No value exists to delete for key %s", key)
	}

	delete(t.store, key)
	t.writeToFile()
	t.mut.Unlock()
	return nil
}

func (t *table) writeToFile() error {
	data, err := json.Marshal(t.store)
	if err != nil {
		return fmt.Errorf("Failed to marshall datastore: %s", err.Error())
	}

	size := len(data)
	if len(t.data) < size {
		log.Println("Resizing file")
		t.file.Close()
		t.openFile()

		err := syscall.Ftruncate(t.fd, int64(size))
		if err != nil {
			return fmt.Errorf("Failed to resize file: %s", err.Error())
		}

		err = t.mmapFile(size)
		if err != nil {
			return fmt.Errorf("Failed to mmap after resize: %s", err.Error())
		}
	}

	copy(t.data, data)

	return nil
}
