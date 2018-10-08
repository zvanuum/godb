package main

import (
	"fmt"
)

type Repository interface {
	Get(string) (string, error)
	Set(string, string)
	Delete(string) error
}

type keyValueStore struct {
	store map[string]string
}

func NewRepository() Repository {
	return &keyValueStore{
		store: make(map[string]string),
	}
}

func (kvs *keyValueStore) Get(key string) (string, error) {
	var val string
	var ok bool

	if val, ok = kvs.store[key]; !ok {
		return "", fmt.Errorf("no value exists to get for key %s", key)
	}

	return val, nil
}

func (kvs *keyValueStore) Set(key string, value string) {
	kvs.store[key] = value
}

func (kvs *keyValueStore) Delete(key string) error {
	if _, ok := kvs.store[key]; !ok {
		return fmt.Errorf("no value exists to delete for key %s", key)
	}

	delete(kvs.store, key)
	return nil
}
