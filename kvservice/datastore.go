package kvservice

import (
	"maps"
	"sync"
)

type DataStore struct {
	sync.Mutex
	data map[string]string
}

func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[string]string),
	}
}

func (ds *DataStore) Get(key string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	value, ok := ds.data[key]
	return value, ok
}

func (ds *DataStore) Put(key, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	v, ok := ds.data[key]
	ds.data[key] = value
	return v, ok
}

func (ds *DataStore) Append(key, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	v, ok := ds.data[key]
	ds.data[key] += value
	return v, ok
}

func (ds *DataStore) CAS(key, compare, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	prevValue, ok := ds.data[key]
	if ok && prevValue == compare {
		ds.data[key] = value
	}
	return prevValue, ok
}

func (ds *DataStore) CopyAll() map[string]string {
	ds.Lock()
	defer ds.Unlock()

	out := make(map[string]string, len(ds.data))
	maps.Copy(out, ds.data)
	return out
}

func (ds *DataStore) RestoreAll(data map[string]string) {
	ds.Lock()
	defer ds.Unlock()

	ds.data = make(map[string]string, len(data))
	maps.Copy(ds.data, data)
}
