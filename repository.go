package main

import (
	"fmt"
	"sync"
)

type Repository interface {
	Get(string) (string, error)
	Set(string, string)
	Delete(string) error
}

type keyValueStore struct {
	store map[string]string
	mut   *sync.Mutex
}

func NewRepository() Repository {
	return &keyValueStore{
		store: make(map[string]string),
		mut:   &sync.Mutex{},
	}
}

func (kvs *keyValueStore) Get(key string) (string, error) {
	var val string
	var ok bool

	kvs.mut.Lock()
	val, ok = kvs.store[key]
	kvs.mut.Unlock()

	if !ok {
		return "", fmt.Errorf("No value exists to get for key %s", key)
	}

	return val, nil
}

func (kvs *keyValueStore) Set(key string, value string) {
	kvs.mut.Lock()
	kvs.store[key] = value
	kvs.mut.Unlock()
}

func (kvs *keyValueStore) Delete(key string) error {
	kvs.mut.Lock()
	if _, ok := kvs.store[key]; !ok {
		kvs.mut.Unlock()
		return fmt.Errorf("No value exists to delete for key %s", key)
	}

	delete(kvs.store, key)
	kvs.mut.Unlock()
	return nil
}
