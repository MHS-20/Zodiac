package kvservice

import "testing"

func checkPutPrev(t *testing.T, ds *DataStore, k string, v string, prev string, hasPrev bool) {
	t.Helper()
	prevVal, ok := ds.Put(k, v)
	if hasPrev != ok || prevVal != prev {
		t.Errorf("prevVal=%s, ok=%v; want %s,%v", prevVal, ok, prev, hasPrev)
	}
}

func checkGet(t *testing.T, ds *DataStore, k string, v string, found bool) {
	t.Helper()
	gotV, ok := ds.Get(k)
	if found != ok || v != gotV {
		t.Errorf("gotV=%s, ok=%v; want %s,%v", gotV, ok, v, found)
	}
}

func checkList(t *testing.T, ds *DataStore, prefix string, want map[string]string) {
	t.Helper()
	got := ds.List(prefix)
	if len(got) != len(want) {
		t.Errorf("List(%q) returned %d pairs, want %d: %v", prefix, len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("List(%q)[%q] = %q, want %q", prefix, k, got[k], v)
		}
	}
}

func TestList(t *testing.T) {
	ds := NewDataStore()
	ds.Put("/nodes/1/cpu", "4")
	ds.Put("/nodes/1/mem", "16")
	ds.Put("/nodes/2/cpu", "8")
	ds.Put("/nodes/2/mem", "32")
	ds.Put("/pods/a", "running")
	ds.Put("/pods/b", "pending")

	checkList(t, ds, "/nodes/1/", map[string]string{
		"/nodes/1/cpu": "4",
		"/nodes/1/mem": "16",
	})
	checkList(t, ds, "/nodes/", map[string]string{
		"/nodes/1/cpu": "4",
		"/nodes/1/mem": "16",
		"/nodes/2/cpu": "8",
		"/nodes/2/mem": "32",
	})
	checkList(t, ds, "/pods/", map[string]string{
		"/pods/a": "running",
		"/pods/b": "pending",
	})
	checkList(t, ds, "/nonexistent/", map[string]string{})
	checkList(t, ds, "", map[string]string{
		"/nodes/1/cpu": "4",
		"/nodes/1/mem": "16",
		"/nodes/2/cpu": "8",
		"/nodes/2/mem": "32",
		"/pods/a":      "running",
		"/pods/b":      "pending",
	})
}

func checkCAS(t *testing.T, ds *DataStore, k string, comp string, v string, prev string, found bool) {
	t.Helper()
	gotPrev, gotFound := ds.CAS(k, comp, v)
	if found != gotFound || prev != gotPrev {
		t.Errorf("gotPrev=%s, gotFound=%v; want %s,%v", gotPrev, gotFound, prev, found)
	}
}

func TestGetPut(t *testing.T) {
	ds := NewDataStore()

	checkGet(t, ds, "foo", "", false)
	checkPutPrev(t, ds, "foo", "bar", "", false)
	checkGet(t, ds, "foo", "bar", true)
	checkPutPrev(t, ds, "foo", "baz", "bar", true)
	checkGet(t, ds, "foo", "baz", true)
	checkPutPrev(t, ds, "nix", "hard", "", false)
}

func TestCASBasic(t *testing.T) {
	ds := NewDataStore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	// CAS replace existing value
	checkCAS(t, ds, "foo", "mex", "bro", "bar", true)
	checkCAS(t, ds, "foo", "bar", "bro", "bar", true)
	checkGet(t, ds, "foo", "bro", true)

	// CAS when key not found
	checkCAS(t, ds, "goa", "mm", "vv", "", false)
	checkGet(t, ds, "goa", "", false)

	// ... and now this key assigned
	ds.Put("goa", "tva")
	checkCAS(t, ds, "goa", "mm", "vv", "tva", true)
	checkCAS(t, ds, "goa", "mm", "vv", "tva", true)
}

func TestCASConcurrent(t *testing.T) {
	// Run this with -race
	ds := NewDataStore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	go func() {
		for range 2000 {
			ds.CAS("foo", "bar", "baz")
		}
	}()
	go func() {
		for range 2000 {
			ds.CAS("foo", "baz", "bar")
		}
	}()

	v, _ := ds.Get("foo")
	if v != "bar" && v != "baz" {
		t.Errorf("got v=%s, want bar or baz", v)
	}
}
