package kvservice

import (
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MHS-20/Zodiac/api"
)

type Lease struct {
	ID        int64
	TTL       int64
	ExpiresAt time.Time
	Keys      map[string]struct{}
}

type DataStore struct {
	sync.Mutex
	data        map[string]string
	leases      map[int64]*Lease
	keyLease    map[string]int64
	leaseIDSeq  int64
}

func NewDataStore() *DataStore {
	return &DataStore{
		data:       make(map[string]string),
		leases:     make(map[int64]*Lease),
		keyLease:   make(map[string]int64),
		leaseIDSeq: 0,
	}
}

func (ds *DataStore) Get(key string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	value, ok := ds.data[key]
	return value, ok
}

func (ds *DataStore) attachKeyToLease(key string, leaseID int64) {
	ds.detachKeyFromLease(key)
	l, ok := ds.leases[leaseID]
	if !ok {
		return
	}
	l.Keys[key] = struct{}{}
	ds.keyLease[key] = leaseID
}

func (ds *DataStore) detachKeyFromLease(key string) {
	oldLeaseID, ok := ds.keyLease[key]
	if !ok {
		return
	}
	delete(ds.keyLease, key)
	if l, ok := ds.leases[oldLeaseID]; ok {
		delete(l.Keys, key)
	}
}

func (ds *DataStore) putLocked(key, value string, leaseID int64) (string, bool) {
	ds.detachKeyFromLease(key)
	v, ok := ds.data[key]
	ds.data[key] = value
	if leaseID != 0 {
		ds.attachKeyToLease(key, leaseID)
	}
	return v, ok
}

func (ds *DataStore) Put(key, value string, leaseID int64) (string, bool) {
	ds.Lock()
	defer ds.Unlock()
	return ds.putLocked(key, value, leaseID)
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
	ds.detachKeyFromLease(key)
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

func (ds *DataStore) GrantLease(ttl int64) int64 {
	ds.Lock()
	defer ds.Unlock()
	ds.leaseIDSeq++
	id := ds.leaseIDSeq
	ds.leases[id] = &Lease{
		ID:        id,
		TTL:       ttl,
		ExpiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
		Keys:      make(map[string]struct{}),
	}
	return id
}

func (ds *DataStore) KeepAliveLease(id int64) bool {
	ds.Lock()
	defer ds.Unlock()
	l, ok := ds.leases[id]
	if !ok {
		return false
	}
	l.ExpiresAt = time.Now().Add(time.Duration(l.TTL) * time.Second)
	return true
}

func (ds *DataStore) RevokeLease(id int64) []string {
	ds.Lock()
	defer ds.Unlock()
	l, ok := ds.leases[id]
	if !ok {
		return nil
	}
	delete(ds.leases, id)
	keys := make([]string, 0, len(l.Keys))
	for k := range l.Keys {
		keys = append(keys, k)
		delete(ds.data, k)
		delete(ds.keyLease, k)
	}
	return keys
}

func (ds *DataStore) CheckExpiredLeases() []int64 {
	ds.Lock()
	defer ds.Unlock()
	now := time.Now()
	var expired []int64
	for id, l := range ds.leases {
		if now.After(l.ExpiresAt) {
			expired = append(expired, id)
		}
	}
	return expired
}

func (ds *DataStore) LeaseCount() int {
	ds.Lock()
	defer ds.Unlock()
	return len(ds.leases)
}

func (ds *DataStore) GetLeaseSnapshot() (map[int64]leaseSnapshot, map[string]int64, int64) {
	ds.Lock()
	defer ds.Unlock()
	ls := make(map[int64]leaseSnapshot, len(ds.leases))
	for id, l := range ds.leases {
		keys := make([]string, 0, len(l.Keys))
		for k := range l.Keys {
			keys = append(keys, k)
		}
		ls[id] = leaseSnapshot{
			ID:        l.ID,
			TTL:       l.TTL,
			ExpiresAt: l.ExpiresAt,
			Keys:      keys,
		}
	}
	kl := make(map[string]int64, len(ds.keyLease))
	for k, v := range ds.keyLease {
		kl[k] = v
	}
	return ls, kl, ds.leaseIDSeq
}

func (ds *DataStore) RestoreFromLeaseSnapshot(ls map[int64]leaseSnapshot, kl map[string]int64, nextID int64) {
	ds.Lock()
	defer ds.Unlock()
	leases := make(map[int64]*Lease, len(ls))
	for id, s := range ls {
		keys := make(map[string]struct{}, len(s.Keys))
		for _, k := range s.Keys {
			keys[k] = struct{}{}
		}
		leases[id] = &Lease{
			ID:        s.ID,
			TTL:       s.TTL,
			ExpiresAt: s.ExpiresAt,
			Keys:      keys,
		}
	}
	ds.leases = leases
	ds.keyLease = kl
	ds.leaseIDSeq = nextID
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
			r.PrevValue, r.KeyFound = ds.putLocked(op.Key, op.Value, op.LeaseID)
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
