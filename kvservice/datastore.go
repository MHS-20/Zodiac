package kvservice

import (
	"maps"
	"strconv"
	"strings"
	"sync"

	"github.com/MHS-20/Zodiac/api"
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

func (ds *DataStore) putLocked(key, value string) (string, bool) {
	v, ok := ds.data[key]
	ds.data[key] = value
	return v, ok
}

func (ds *DataStore) Put(key, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()
	return ds.putLocked(key, value)
}

func (ds *DataStore) appendLocked(key, value string) (string, bool) {
	v, ok := ds.data[key]
	ds.data[key] += value
	return v, ok
}

func (ds *DataStore) Append(key, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()
	return ds.appendLocked(key, value)
}

func (ds *DataStore) casLocked(key, compare, value string) (string, bool) {
	prevValue, ok := ds.data[key]
	if ok && prevValue == compare {
		ds.data[key] = value
	}
	return prevValue, ok
}

func (ds *DataStore) CAS(key, compare, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()
	return ds.casLocked(key, compare, value)
}

func (ds *DataStore) deleteLocked(key string) (string, bool) {
	v, ok := ds.data[key]
	delete(ds.data, key)
	return v, ok
}

func (ds *DataStore) Delete(key string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()
	return ds.deleteLocked(key)
}

func (ds *DataStore) List(prefix string) map[string]string {
	ds.Lock()
	defer ds.Unlock()

	result := make(map[string]string)
	for k, v := range ds.data {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result
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

func (ds *DataStore) evalCondition(c api.TxnCondition) bool {
	v, ok := ds.data[c.Key]
	switch c.Compare {
	case api.CompareEQ:
		return ok && v == c.Value
	case api.CompareNEQ:
		return !ok || v != c.Value
	case api.CompareGTE:
		if !ok {
			return false
		}
		vi, errV := strconv.ParseInt(v, 10, 64)
		ci, errC := strconv.ParseInt(c.Value, 10, 64)
		if errV != nil || errC != nil {
			return v >= c.Value
		}
		return vi >= ci
	case api.CompareLTE:
		if !ok {
			return false
		}
		vi, errV := strconv.ParseInt(v, 10, 64)
		ci, errC := strconv.ParseInt(c.Value, 10, 64)
		if errV != nil || errC != nil {
			return v <= c.Value
		}
		return vi <= ci
	case api.CompareExists:
		return ok
	case api.CompareNotExists:
		return !ok
	default:
		return false
	}
}

func (ds *DataStore) Txn(conds []api.TxnCondition, success, failure []api.TxnOp) (bool, []api.TxnOpResult) {
	ds.Lock()
	defer ds.Unlock()

	allMet := true
	for _, c := range conds {
		if !ds.evalCondition(c) {
			allMet = false
			break
		}
	}

	branch := success
	if !allMet {
		branch = failure
	}

	results := make([]api.TxnOpResult, 0, len(branch))
	for _, op := range branch {
		r := api.TxnOpResult{Key: op.Key}
		switch op.Op {
		case api.TxnOpPut:
			r.PrevValue, r.KeyFound = ds.putLocked(op.Key, op.Value)
		case api.TxnOpDelete:
			r.PrevValue, r.KeyFound = ds.deleteLocked(op.Key)
		case api.TxnOpCAS:
			r.PrevValue, r.KeyFound = ds.casLocked(op.Key, op.CompareValue, op.Value)
		case api.TxnOpAppend:
			r.PrevValue, r.KeyFound = ds.appendLocked(op.Key, op.Value)
		}
		results = append(results, r)
	}
	return allMet, results
}
