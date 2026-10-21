package fastcache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type handlerFunc func(http.ResponseWriter, *http.Request) error

func (f handlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) error { return f(w, r) }

var _ caddyhttp.Handler = handlerFunc(nil)

// newTestCache builds a provisioned FastCache over a temp dir.
func newTestCache(t *testing.T, mutate func(*FastCache)) *FastCache {
	t.Helper()

	fc := &FastCache{
		Path:       t.TempDir(),
		MemorySize: "64MB",
		DiskSize:   "1GB",
		TTL:        "1h",
	}
	if mutate != nil {
		mutate(fc)
	}

	fc.memoryBytes, _ = parseSize(fc.MemorySize, 0)
	fc.diskBytes, _ = parseSize(fc.DiskSize, 0)
	fc.promoteBelow, _ = parseSize(fc.PromoteBelow, defaultPromoteBelow)
	fc.ttl, _ = parseDuration(fc.TTL, 0)
	fc.swr, _ = parseDuration(fc.StaleWhileRevalidate, 0)
	fc.sie, _ = parseDuration(fc.StaleIfError, 0)

	fc.idx = newIndex(fc)
	if err := fc.idx.rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	go fc.idx.sweep()

	t.Cleanup(fc.idx.stop)

	return fc
}

// waitFor polls cond until it holds — eviction is async (evictLoop), so
// tests can't assert its effects immediately after a store.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

// get issues a GET through the handler and returns the recorder.
func get(t *testing.T, fc *FastCache, target string, headers http.Header, next caddyhttp.Handler) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodGet, "http://example.com"+target, nil)
	for k, vv := range headers {
		for _, v := range vv {
			r.Header.Add(k, v)
		}
	}

	w := httptest.NewRecorder()
	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}

	return w
}

// countingUpstream returns a next handler serving body, counting calls.
func countingUpstream(counter *atomic.Int64, body []byte, headers http.Header) caddyhttp.Handler {
	return handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		counter.Add(1)
		for k, vv := range headers {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)

		return nil
	})
}

func TestMissThenHit(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	body := []byte("hello cached world")
	next := countingUpstream(&calls, body, http.Header{"Cache-Control": {"public, max-age=3600"}})

	w1 := get(t, fc, "/", nil, next)
	if w1.Code != http.StatusOK || w1.Body.String() != string(body) {
		t.Fatalf("miss: got %d %q", w1.Code, w1.Body.String())
	}

	if calls.Load() != 1 {
		t.Fatalf("expected 1 upstream call, got %d", calls.Load())
	}

	w2 := get(t, fc, "/", nil, next)
	if w2.Code != http.StatusOK || w2.Body.String() != string(body) {
		t.Fatalf("hit: got %d %q", w2.Code, w2.Body.String())
	}

	if calls.Load() != 1 {
		t.Fatalf("hit should not reach upstream, got %d calls", calls.Load())
	}

	if got := w2.Header().Get("Cache-Status"); !strings.HasPrefix(got, "fastcache; hit") {
		t.Errorf("expected Cache-Status hit, got %q", got)
	}
}

func TestUncacheableResponses(t *testing.T) {
	cases := []struct {
		name    string
		headers http.Header
		status  int
	}{
		{"no-store", http.Header{"Cache-Control": {"no-store"}}, 200},
		{"private", http.Header{"Cache-Control": {"private"}}, 200},
		{"max-age=0", http.Header{"Cache-Control": {"public, max-age=0"}}, 200},
		{"vary-star", http.Header{"Vary": {"*"}}, 200},
		{"status-500", nil, 500},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newTestCache(t, nil)

			var calls atomic.Int64

			next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
				calls.Add(1)
				for k, vv := range tc.headers {
					for _, v := range vv {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("x"))

				return nil
			})

			get(t, fc, "/", nil, next)
			get(t, fc, "/", nil, next)

			if calls.Load() != 2 {
				t.Errorf("expected 2 upstream calls for uncacheable response, got %d", calls.Load())
			}
		})
	}
}

func TestRequestBypass(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		headers http.Header
		honorCC bool
		want    int64
	}{
		{"post", http.MethodPost, nil, false, 2},
		{"no-cache honored", http.MethodGet, http.Header{"Cache-Control": {"no-cache"}}, true, 2},
		{"no-cache ignored by default", http.MethodGet, http.Header{"Cache-Control": {"no-cache"}}, false, 1},
		{"pragma ignored by default", http.MethodGet, http.Header{"Pragma": {"no-cache"}}, false, 1},
		{"authorization", http.MethodGet, http.Header{"Authorization": {"Bearer t"}}, false, 2},
		{"upgrade", http.MethodGet, http.Header{"Upgrade": {"websocket"}}, false, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newTestCache(t, func(fc *FastCache) { fc.HonorRequestCC = tc.honorCC })

			var calls atomic.Int64
			next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

			r := httptest.NewRequest(tc.method, "http://example.com/", nil)
			for k, vv := range tc.headers {
				for _, v := range vv {
					r.Header.Add(k, v)
				}
			}

			if err := fc.ServeHTTP(httptest.NewRecorder(), r, next); err != nil {
				t.Fatal(err)
			}

			if err := fc.ServeHTTP(httptest.NewRecorder(), r, next); err != nil {
				t.Fatal(err)
			}

			if calls.Load() != tc.want {
				t.Errorf("got %d upstream calls, want %d", calls.Load(), tc.want)
			}
		})
	}
}

func TestExpiry(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=1"}})

	get(t, fc, "/", nil, next)

	time.Sleep(1100 * time.Millisecond)

	get(t, fc, "/", nil, next)

	if calls.Load() != 2 {
		t.Errorf("expected re-fetch after expiry, got %d calls", calls.Load())
	}
}

func TestIfNoneMatch(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{
		"Cache-Control": {"public, max-age=3600"},
		"ETag":          {`"abc123"`},
	})

	get(t, fc, "/", nil, next)

	w := get(t, fc, "/", http.Header{"If-None-Match": {`"abc123"`}}, next)
	if w.Code != http.StatusNotModified {
		t.Errorf("expected 304, got %d", w.Code)
	}

	if calls.Load() != 1 {
		t.Errorf("expected 1 upstream call, got %d", calls.Load())
	}
}

func TestPurge(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	r := httptest.NewRequest("PURGE", "http://example.com/", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()

	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on purge, got %d", w.Code)
	}

	get(t, fc, "/", nil, next)

	if calls.Load() != 2 {
		t.Errorf("expected upstream call after purge, got %d", calls.Load())
	}
}

func TestVary(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"Accept-Language"} })

	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		_, _ = w.Write([]byte("lang:" + r.Header.Get("Accept-Language")))

		return nil
	})

	w1 := get(t, fc, "/", http.Header{"Accept-Language": {"en"}}, next)
	w2 := get(t, fc, "/", http.Header{"Accept-Language": {"fr"}}, next)

	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls for distinct vary values, got %d", calls.Load())
	}

	w3 := get(t, fc, "/", http.Header{"Accept-Language": {"en"}}, next)
	if w3.Body.String() != "lang:en" || calls.Load() != 2 {
		t.Errorf("expected en variant hit without upstream call, got body %q calls %d", w3.Body.String(), calls.Load())
	}

	_ = w1
	_ = w2
}

func TestVaryUnlistedHeader(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"Accept-Encoding"} })

	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Vary", "X-Custom-Header")
		_, _ = w.Write([]byte("x"))

		return nil
	})

	get(t, fc, "/", nil, next)
	get(t, fc, "/", nil, next)

	if calls.Load() != 2 {
		t.Errorf("response varying on unlisted header must not be cached, got %d calls", calls.Load())
	}
}

// TestVaryChange: when upstream starts sending a Vary list that differs
// from the stored mapping, the old variants are invalidated and the new
// scheme keys correctly.
func TestVaryChange(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"Accept-Language"} })

	var varyOn atomic.Bool
	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=1")
		if varyOn.Load() {
			w.Header().Set("Vary", "Accept-Language")
			_, _ = w.Write([]byte("lang:" + r.Header.Get("Accept-Language")))
		} else {
			_, _ = w.Write([]byte("plain"))
		}

		return nil
	})

	get(t, fc, "/", nil, next)
	if w := get(t, fc, "/", nil, next); w.Body.String() != "plain" || calls.Load() != 1 {
		t.Fatalf("expected plain hit, got %q calls=%d", w.Body.String(), calls.Load())
	}

	// Upstream starts varying. The next store must drop the old no-vary
	// mapping; subsequent lookups key by Accept-Language.
	varyOn.Store(true)
	time.Sleep(1100 * time.Millisecond)

	w := get(t, fc, "/", http.Header{"Accept-Language": {"en"}}, next)
	if w.Body.String() != "lang:en" {
		t.Fatalf("expected lang:en, got %q", w.Body.String())
	}

	if w = get(t, fc, "/", http.Header{"Accept-Language": {"en"}}, next); w.Body.String() != "lang:en" || calls.Load() != 2 {
		t.Fatalf("expected lang:en hit, got %q calls=%d", w.Body.String(), calls.Load())
	}

	if w = get(t, fc, "/", http.Header{"Accept-Language": {"fr"}}, next); w.Body.String() != "lang:fr" {
		t.Fatalf("expected lang:fr, got %q", w.Body.String())
	}
}

// TestShardLayout: body files land in a 2-hex-char shard directory and the
// rebuild walk finds them again.
func TestShardLayout(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	var e *entry
	fc.idx.entries.Range(func(_, v any) bool { e = v.(*entry); return false })
	if e == nil {
		t.Fatal("no entry stored")
	}

	shard := filepath.Base(filepath.Dir(e.path))
	if !regexp.MustCompile(`^[0-9a-f]{2}$`).MatchString(shard) {
		t.Fatalf("body file not under a 2-hex shard dir: %s", e.path)
	}

	fc.idx.stop()

	fc2 := &FastCache{Path: fc.Path, MemorySize: "0", DiskSize: "1GB", TTL: "1h"}
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.ttl, _ = parseDuration(fc2.TTL, 0)
	fc2.idx = newIndex(fc2)
	if err := fc2.idx.rebuild(); err != nil {
		t.Fatal(err)
	}
	defer fc2.idx.stop()

	if w := get(t, fc2, "/", nil, next); w.Body.String() != "x" {
		t.Fatalf("sharded entry not restored, got %q", w.Body.String())
	}
}

func TestHead(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("hello"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	r := httptest.NewRequest(http.MethodHead, "http://example.com/", nil)
	w := httptest.NewRecorder()

	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}

	if w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Errorf("HEAD should return headers only, got %d len %d", w.Code, w.Body.Len())
	}

	if calls.Load() != 1 {
		t.Errorf("HEAD should hit cache, got %d calls", calls.Load())
	}
}

func TestRange(t *testing.T) {
	fc := newTestCache(t, nil)

	body := []byte("0123456789abcdef")

	var calls atomic.Int64
	next := countingUpstream(&calls, body, http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	r.Header.Set("Range", "bytes=0-3")
	w := httptest.NewRecorder()

	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}

	if w.Code != http.StatusPartialContent || w.Body.String() != "0123" {
		t.Errorf("expected 206 '0123', got %d %q", w.Code, w.Body.String())
	}
}

func TestSingleflight(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64

	release := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		<-release
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("x"))

		return nil
	})

	const n = 16

	var wg sync.WaitGroup
	wg.Add(n)

	for range n {
		go func() {
			defer wg.Done()

			r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
			_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
		}()
	}

	// Let followers pile up behind the leader.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("expected exactly 1 upstream call for %d concurrent misses, got %d", n, calls.Load())
	}
}

func TestRestartRebuild(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("persistent"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)
	fc.idx.stop()

	// Simulate a restart: a fresh index over the same directory.
	fc2 := &FastCache{Path: fc.Path, MemorySize: "64MB", DiskSize: "1GB", TTL: "1h"}
	fc2.memoryBytes, _ = parseSize(fc2.MemorySize, 0)
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.ttl, _ = parseDuration(fc2.TTL, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	go fc2.idx.sweep()

	defer fc2.idx.stop()

	w := get(t, fc2, "/", nil, next)
	if w.Body.String() != "persistent" {
		t.Fatalf("expected rebuilt hit, got %q", w.Body.String())
	}

	if calls.Load() != 1 {
		t.Errorf("rebuilt index should serve without upstream, got %d calls", calls.Load())
	}
}

func TestDiskBudgetEviction(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.DiskSize = "2KB" })

	var calls atomic.Int64

	for i, key := range []string{"/a", "/b", "/c"} {
		body := []byte(fmt.Sprintf("body-%d-%s", i, string(make([]byte, 700))))
		next := countingUpstream(&calls, body, http.Header{"Cache-Control": {"public, max-age=3600"}})
		get(t, fc, key, nil, next)
	}

	// The oldest entries must have been evicted to fit the budget.
	waitFor(t, "disk-budget eviction", func() bool {
		return fc.idx.fileCount.Load() < 3
	})

	var miss atomic.Int64

	get(t, fc, "/a", nil, countingUpstream(&miss, []byte("x"), nil))

	if miss.Load() != 1 {
		t.Error("oldest entry should have been evicted")
	}
}

func TestMemoryTierDemotion(t *testing.T) {
	// memory_size smaller than the body → entry stays disk-servable.
	fc := newTestCache(t, func(fc *FastCache) { fc.MemorySize = "1KB" })

	body := make([]byte, 64*1024)
	for i := range body {
		body[i] = byte(i)
	}

	var calls atomic.Int64
	next := countingUpstream(&calls, body, http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	w := get(t, fc, "/", nil, next)
	if w.Body.String() != string(body) {
		t.Fatal("disk-tier hit returned wrong body")
	}

	if calls.Load() != 1 {
		t.Errorf("disk-tier hit should not reach upstream, got %d", calls.Load())
	}
}

func TestConcurrentHammer(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	body := []byte("hammer")
	next := countingUpstream(&calls, body, http.Header{"Cache-Control": {"public, max-age=3600"}})

	var wg sync.WaitGroup

	for i := range 32 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for range 50 {
				get(t, fc, fmt.Sprintf("/k%d", i%8), nil, next)
			}
		}(i)
	}

	wg.Wait()
}

func TestMaxItems(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.MaxItems = 2 })

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/a", nil, next)
	get(t, fc, "/b", nil, next)
	get(t, fc, "/c", nil, next)

	waitFor(t, "count-cap eviction", func() bool { return fc.idx.fileCount.Load() <= 2 })

	// /a was least recently used → evicted → upstream again.
	var miss atomic.Int64

	get(t, fc, "/a", nil, countingUpstream(&miss, []byte("x"), nil))

	if miss.Load() != 1 {
		t.Error("expected count-cap eviction of the oldest entry")
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.StaleWhileRevalidate = "5s" })

	var calls atomic.Int64
	body := "v1"
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))

		return nil
	})

	get(t, fc, "/", nil, next)

	time.Sleep(1100 * time.Millisecond)

	// Within the stale window: serve stale immediately and refresh async.
	body = "v2"
	w := get(t, fc, "/", nil, next)
	if w.Code != http.StatusOK || w.Body.String() != "v1" {
		t.Fatalf("stale serve failed: %d %q", w.Code, w.Body.String())
	}

	// Wait until the background refresh has committed — upstream-call count
	// fires at handler entry, before the commit, so poll the served body.
	waitFor(t, "background revalidation to commit v2", func() bool {
		return get(t, fc, "/", nil, next).Body.String() == "v2"
	})
}

// TestOrphanTmpFiles verifies .tmp files from crashed stores get cleaned by
// rebuild (they lack the magic header).
func TestOrphanTmpFiles(t *testing.T) {
	fc := newTestCache(t, nil)

	orphan := filepath.Join(fc.Path, "crashed.tmp")
	if err := os.WriteFile(orphan, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	fc.idx.stop()

	fc2 := &FastCache{Path: fc.Path, MemorySize: "64MB", DiskSize: "1GB", TTL: "1h"}
	fc2.memoryBytes, _ = parseSize(fc2.MemorySize, 0)
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("expected orphan tmp file removed on rebuild")
	}
}

// TestConcurrentPurgeChurn hammers hits on a few keys while PURGEs and
// RAM-budget demotions/promotions run underneath; it exists to surface
// races on the variant set and the RAM body slot under -race.
func TestConcurrentPurgeChurn(t *testing.T) {
	// 4 keys x 2KB bodies > 4KB RAM budget -> every promotion demotes another.
	fc := newTestCache(t, func(fc *FastCache) {
		fc.MemorySize = "4KB"
		fc.DiskSize = "64KB"
	})

	var calls atomic.Int64
	next := countingUpstream(&calls, make([]byte, 2*1024), http.Header{"Cache-Control": {"public, max-age=3600"}})

	stop := make(chan struct{})

	var wg sync.WaitGroup

	for i := range 8 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
					get(t, fc, fmt.Sprintf("/k%d", i%4), nil, next)
				}
			}
		}(i)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, key := range []string{"/k0", "/k1", "/k2", "/k3"} {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("PURGE", "http://example.com"+key, nil)
			r.RemoteAddr = "127.0.0.1:1234"
			_ = fc.ServeHTTP(w, r, next)
		}
	}

	close(stop)
	wg.Wait()
}

// TestDiskHitPromotesToRAM: a disk-tier hit must land the body in the RAM
// tier (the file is consumed by the serve, so the promotion read must not
// depend on the file offset).
func TestDiskHitPromotesToRAM(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, make([]byte, 4096), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	e, _, ok := fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
	if !ok {
		t.Fatal("expected entry")
	}

	// Simulate a demotion.
	if b := e.body.Swap(nil); b != nil {
		fc.idx.memSize.Add(-int64(len(*b)))
	}

	w := get(t, fc, "/", nil, next)
	if w.Body.Len() != 4096 {
		t.Fatalf("disk hit returned %d bytes", w.Body.Len())
	}

	if e.body.Load() == nil {
		t.Error("entry was not promoted back to RAM after a disk hit")
	}

	if got := fc.idx.memSize.Load(); got != 4096 {
		t.Errorf("memSize = %d, want 4096", got)
	}
}

type failAfterFirstWrite struct {
	http.ResponseWriter
	n int
}

func (f *failAfterFirstWrite) Write(b []byte) (int, error) {
	if f.n++; f.n > 1 {
		return 0, errors.New("client gone")
	}

	return f.ResponseWriter.Write(b)
}

// TestTruncatedBodyNotCached: a client that disappears mid-stream must not
// leave a partial body in the cache.
func TestTruncatedBodyNotCached(t *testing.T) {
	for _, declared := range []bool{true, false} {
		t.Run(fmt.Sprintf("content-length=%v", declared), func(t *testing.T) {
			fc := newTestCache(t, nil)

			var calls atomic.Int64

			next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
				calls.Add(1)
				w.Header().Set("Cache-Control", "public, max-age=3600")
				if declared {
					w.Header().Set("Content-Length", "8")
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("1234"))
				_, _ = w.Write([]byte("5678"))

				return nil
			})

			r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
			_ = fc.ServeHTTP(&failAfterFirstWrite{ResponseWriter: httptest.NewRecorder()}, r, next)

			w := get(t, fc, "/", nil, next)
			if w.Body.String() != "12345678" {
				t.Errorf("got body %q", w.Body.String())
			}

			if calls.Load() != 2 {
				t.Errorf("truncated response must be refetched, got %d calls", calls.Load())
			}
		})
	}
}

// TestHandlerErrorNotCached: an upstream error after the headers were sent
// (e.g. reverse_proxy copy failure) must discard the capture.
func TestHandlerErrorNotCached(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("partial"))

		return errors.New("upstream reset")
	})

	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
	_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)

	if calls.Load() != 2 {
		t.Errorf("errored response must not be cached, got %d calls", calls.Load())
	}
}

// TestRestoreAfterRestart: re-storing a key that was rebuilt from disk must
// not reuse (and then delete) the rebuilt entry's file path.
func TestRestoreAfterRestart(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("v1"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)
	fc.idx.stop()

	fc2 := &FastCache{Path: fc.Path, MemorySize: "0", DiskSize: "1GB", TTL: "1h"}
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.ttl, _ = parseDuration(fc2.TTL, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatal(err)
	}

	defer fc2.idx.stop()

	for range 3 {
		if fc2.idx.purge("example.comhttp://example.com/", "purge") != 1 {
			t.Fatal("expected one variant to purge")
		}

		next = countingUpstream(&calls, []byte("v2"), http.Header{"Cache-Control": {"public, max-age=3600"}})
		get(t, fc2, "/", nil, next)

		if w := get(t, fc2, "/", nil, next); w.Body.String() != "v2" || !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
			t.Fatalf("expected a hit for the re-stored entry, got %q %q", w.Body.String(), w.Header().Get("Cache-Status"))
		}
	}
}

// TestPurgeAllowlist: PURGE is loopback-only unless purge_allow opens it.
func TestPurgeAllowlist(t *testing.T) {
	purge := func(t *testing.T, fc *FastCache, remoteAddr string, next caddyhttp.Handler) *httptest.ResponseRecorder {
		t.Helper()

		r := httptest.NewRequest("PURGE", "http://example.com/", nil)
		r.RemoteAddr = remoteAddr
		w := httptest.NewRecorder()
		if err := fc.ServeHTTP(w, r, next); err != nil {
			t.Fatal(err)
		}

		return w
	}

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	fc := newTestCache(t, nil)
	get(t, fc, "/", nil, next)

	if w := purge(t, fc, "203.0.113.7:1234", next); w.Code != http.StatusForbidden {
		t.Errorf("default config must deny remote PURGE, got %d", w.Code)
	}

	if w := purge(t, fc, "127.0.0.1:1234", next); w.Code != http.StatusOK {
		t.Errorf("loopback PURGE should work, got %d", w.Code)
	}

	fc2 := newTestCache(t, func(fc *FastCache) { fc.PurgeAllow = []string{"203.0.113.0/24"} })
	if err := fc2.compilePurgeAllow(); err != nil {
		t.Fatal(err)
	}

	get(t, fc2, "/", nil, next)

	if w := purge(t, fc2, "203.0.113.7:1234", next); w.Code != http.StatusOK {
		t.Errorf("allowed CIDR should purge, got %d", w.Code)
	}

	if w := purge(t, fc2, "192.0.2.7:1234", next); w.Code != http.StatusForbidden {
		t.Errorf("non-matching CIDR should be denied, got %d", w.Code)
	}
}

// TestHeuristicStatusCaching: RFC 9111 heuristically-cacheable statuses
// (here 404) are stored under the default TTL.
func TestHeuristicStatusCaching(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("nope"))

		return nil
	})

	get(t, fc, "/", nil, next)

	w := get(t, fc, "/", nil, next)
	if w.Code != http.StatusNotFound || w.Body.String() != "nope" {
		t.Fatalf("expected cached 404 replay, got %d %q", w.Code, w.Body.String())
	}

	if calls.Load() != 1 {
		t.Errorf("heuristically cacheable status should hit once, got %d calls", calls.Load())
	}
}

// TestLongKey: URIs longer than NAME_MAX must still be cacheable.
func TestLongKey(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	target := "/" + string(bytes.Repeat([]byte("a"), 1000))

	get(t, fc, target, nil, next)
	get(t, fc, target, nil, next)

	if calls.Load() != 1 {
		t.Errorf("long key should be cached, got %d calls", calls.Load())
	}
}

// TestFollowerCancel: a coalesced follower whose client goes away must not
// stay parked on the leader.
func TestFollowerCancel(t *testing.T) {
	fc := newTestCache(t, nil)

	release := make(chan struct{})
	started := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		close(started)
		<-release
		_, _ = w.Write([]byte("x"))

		return nil
	})

	leaderDone := make(chan struct{})

	go func() {
		defer close(leaderDone)

		r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
	}()

	<-started

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(ctx)

	errc := make(chan error, 1)
	go func() { errc <- fc.ServeHTTP(httptest.NewRecorder(), r, next) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not return after its context was cancelled")
	}

	close(release)
	<-leaderDone // the leader's commit writes files into the TempDir — join it before teardown
}

var _ = io.EOF

// TestSMaxAge: s-maxage overrides max-age for a shared cache, both ways.
func TestSMaxAge(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=0, s-maxage=3600"}})

	get(t, fc, "/", nil, next)
	w := get(t, fc, "/", nil, next)

	if !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") || calls.Load() != 1 {
		t.Fatalf("s-maxage=3600 should be cached despite max-age=0: %q, %d calls", w.Header().Get("Cache-Status"), calls.Load())
	}

	var calls2 atomic.Int64
	next2 := countingUpstream(&calls2, []byte("y"), http.Header{"Cache-Control": {"public, max-age=3600, s-maxage=0"}})

	get(t, fc, "/b", nil, next2)
	get(t, fc, "/b", nil, next2)

	if calls2.Load() != 2 {
		t.Fatalf("s-maxage=0 should disable caching despite max-age=3600: %d calls", calls2.Load())
	}
}

// TestStaleIfError: beyond the SWR window a stale entry is only an error
// fallback — upstream 5xx or handler errors get the stale body; a healthy
// upstream replaces the entry.
func TestStaleIfError(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleIfError = "5s"
	})

	var calls atomic.Int64
	fail := 0 // 0 = ok, 1 = 5xx, 2 = handler error
	body := "v1"
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)

		switch fail {
		case 1:
			w.WriteHeader(http.StatusBadGateway)
			return nil
		case 2:
			return errors.New("upstream dead")
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))

		return nil
	})

	get(t, fc, "/", nil, next) // stores v1
	time.Sleep(80 * time.Millisecond)

	// Past expiry, inside the SIE window: 5xx → stale v1 is served.
	fail = 1
	w := get(t, fc, "/", nil, next)
	if w.Code != http.StatusOK || w.Body.String() != "v1" {
		t.Fatalf("stale_if_error should serve the stale entry on 5xx, got %d %q", w.Code, w.Body.String())
	}

	// Handler error before anything was written → same fallback.
	fail = 2
	w = get(t, fc, "/", nil, next)
	if w.Code != http.StatusOK || w.Body.String() != "v1" {
		t.Fatalf("stale_if_error should serve the stale entry on handler error, got %d %q", w.Code, w.Body.String())
	}

	// Upstream healthy again → new response replaces the stale entry.
	fail = 0
	body = "v2"
	w = get(t, fc, "/", nil, next)
	if w.Body.String() != "v2" {
		t.Fatalf("expected revalidated body v2, got %q", w.Body.String())
	}

	if calls.Load() != 4 {
		t.Fatalf("expected 4 upstream calls, got %d", calls.Load())
	}
}

// TestMatchHeader: when match_header rules are set, only responses whose
// headers match are stored.
func TestMatchHeader(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.MatchHeaders = map[string][]string{"X-Cache-Me": {"yes"}}
	})

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/no", nil, next)
	get(t, fc, "/no", nil, next)
	if calls.Load() != 2 {
		t.Fatalf("response without X-Cache-Me should not be cached: %d calls", calls.Load())
	}

	nextYes := countingUpstream(&calls, []byte("x"), http.Header{
		"Cache-Control": {"public, max-age=3600"},
		"X-Cache-Me":    {"yes"},
	})

	get(t, fc, "/yes", nil, nextYes)
	w := get(t, fc, "/yes", nil, nextYes)
	if !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatalf("matching response should be cached, got %q", w.Header().Get("Cache-Status"))
	}
}

// TestCacheKeyTemplate: cache_key resolves caddy placeholders per request.
func TestCacheKeyTemplate(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.CacheKey = "{http.request.uri.path}" })

	getPath := func(target string, next caddyhttp.Handler) *httptest.ResponseRecorder {
		t.Helper()

		r := httptest.NewRequest(http.MethodGet, "http://example.com"+target, nil)
		repl := caddy.NewReplacer()
		repl.Set("http.request.uri.path", r.URL.Path)
		r = r.WithContext(context.WithValue(r.Context(), caddy.ReplacerCtxKey, repl))

		w := httptest.NewRecorder()
		if err := fc.ServeHTTP(w, r, next); err != nil {
			t.Fatalf("ServeHTTP: %v", err)
		}

		return w
	}

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	getPath("/a?x=1", next)
	w := getPath("/a?x=2", next)

	if !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("requests differing only in query should share the {path} key")
	}

	if calls.Load() != 1 {
		t.Fatalf("expected 1 upstream call, got %d", calls.Load())
	}
}

// TestAdminAPI: the admin routes list keys and regex-purge them.
func TestAdminAPI(t *testing.T) {
	fc := newTestCache(t, nil)
	liveCaches.Store(fc, struct{}{})
	t.Cleanup(func() { liveCaches.Delete(fc) })

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/a", nil, next)
	get(t, fc, "/b", nil, next)

	// GET /fastcache/keys
	w := httptest.NewRecorder()
	if err := listCacheKeys(w, httptest.NewRequest(http.MethodGet, "/fastcache/keys", nil)); err != nil {
		t.Fatal(err)
	}

	var listing struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}

	if len(listing.Keys) != 2 {
		t.Fatalf("expected 2 keys, got %v", listing.Keys)
	}

	// DELETE /fastcache/purge {"pattern": ...}
	pw := httptest.NewRecorder()
	body := strings.NewReader(`{"pattern": "/a$"}`)
	if err := purgeCacheKeys(pw, httptest.NewRequest(http.MethodDelete, "/fastcache/purge", body)); err != nil {
		t.Fatal(err)
	}

	var res struct {
		Purged int `json:"purged"`
	}
	if err := json.Unmarshal(pw.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}

	if res.Purged != 1 {
		t.Fatalf("expected 1 purged variant, got %d", res.Purged)
	}

	// /a was evicted; /b still hits.
	var missA atomic.Int64
	get(t, fc, "/a", nil, countingUpstream(&missA, []byte("y"), nil))
	if missA.Load() != 1 {
		t.Fatal("/a should have been purged")
	}

	if w := get(t, fc, "/b", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("/b should still be cached")
	}
}
func TestRAMOnly(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.DiskSize = "0" })

	var calls atomic.Int64
	body := []byte("ram only body")
	next := countingUpstream(&calls, body, http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	w := get(t, fc, "/", nil, next)
	if w.Body.String() != string(body) || !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatalf("expected a RAM hit, got %d %q %q", w.Code, w.Body.String(), w.Header().Get("Cache-Status"))
	}

	if calls.Load() != 1 {
		t.Fatalf("hit reached upstream: %d calls", calls.Load())
	}

	files, err := os.ReadDir(fc.Path)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 0 {
		t.Fatalf("ram-only cache wrote %d files", len(files))
	}
}

// TestRAMOnlyOversize: a body larger than memory_size has nowhere to go and
// must not be cached.
func TestRAMOnlyOversize(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.DiskSize = "0"
		fc.MemorySize = "64"
	})

	var calls atomic.Int64
	next := countingUpstream(&calls, bytes.Repeat([]byte("x"), 128), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)
	get(t, fc, "/", nil, next)

	if calls.Load() != 2 {
		t.Fatalf("over-budget body should not be cached, got %d upstream calls", calls.Load())
	}
}

// TestRAMOnlyRebuild: switching to disk_size 0 wipes files left by a
// disk-backed configuration instead of serving them.
func TestRAMOnlyRebuild(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("v1"), http.Header{"Cache-Control": {"public, max-age=3600"}})
	get(t, fc, "/", nil, next)
	fc.idx.stop()

	fc2 := &FastCache{Path: fc.Path, MemorySize: "64MB", DiskSize: "0", TTL: "1h"}
	fc2.memoryBytes, _ = parseSize(fc2.MemorySize, 0)
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.ttl, _ = parseDuration(fc2.TTL, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatal(err)
	}

	defer fc2.idx.stop()

	files, err := os.ReadDir(fc.Path)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 0 {
		t.Fatalf("ram-only rebuild left %d files", len(files))
	}

	if w := get(t, fc2, "/", nil, next); strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("served a stale disk entry in ram-only mode")
	}

	if calls.Load() != 2 {
		t.Fatalf("expected a miss after switching to ram-only, got %d calls", calls.Load())
	}
}

// TestRAMOnlyEviction: RAM pressure evicts whole entries — there is no disk
// copy to demote to.
func TestRAMOnlyEviction(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.DiskSize = "0"
		fc.MemorySize = "100"
	})

	var calls atomic.Int64
	next := countingUpstream(&calls, bytes.Repeat([]byte("x"), 60), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/a", nil, next)
	get(t, fc, "/b", nil, next) // 60+60 > 100: /a must be evicted outright

	waitFor(t, "RAM-only eviction of /a", func() bool { return fc.idx.fileCount.Load() == 1 })

	if w := get(t, fc, "/a", nil, next); strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("evicted entry still served")
	}

	// Re-storing /a evicts /b; /a now hits.
	waitFor(t, "RAM-only eviction of /b", func() bool { return fc.idx.fileCount.Load() == 1 })

	if w := get(t, fc, "/a", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatalf("expected hit, got %q", w.Header().Get("Cache-Status"))
	}

	if calls.Load() != 3 {
		t.Fatalf("expected 3 upstream calls, got %d", calls.Load())
	}
}

// TestUnsafeMethodInvalidation: a non-error response to an unsafe method
// invalidates the stored response for the request URI (RFC 9111 §4.4); an
// error response does not.
func TestUnsafeMethodInvalidation(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	var fail atomic.Bool

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if r.Method == http.MethodPost {
			if fail.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return nil
			}
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("v1"))
		return nil
	})

	get(t, fc, "/x", nil, next)
	if w := get(t, fc, "/x", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("expected hit")
	}

	// Successful POST invalidates: the next GET re-fetches.
	req := httptest.NewRequest(http.MethodPost, "http://example.com/x", nil)
	rec := httptest.NewRecorder()
	if err := fc.ServeHTTP(rec, req, next); err != nil || rec.Code != 204 {
		t.Fatalf("POST: %v code=%d", err, rec.Code)
	}

	if w := get(t, fc, "/x", nil, next); strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("POST did not invalidate the entry")
	}

	// An error response to POST must NOT invalidate.
	fail.Store(true)

	rec = httptest.NewRecorder()
	if err := fc.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "http://example.com/x", nil), next); err != nil || rec.Code != 500 {
		t.Fatalf("POST 500: %v code=%d", err, rec.Code)
	}

	if w := get(t, fc, "/x", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("failed POST wrongly invalidated the entry")
	}

	if calls.Load() != 4 {
		t.Fatalf("expected 4 upstream calls, got %d", calls.Load())
	}
}

// TestMustRevalidate: a stale entry carrying must-revalidate may not be
// served — requests synchronously revalidate, and an upstream error is
// passed through rather than masked by the stale body.
func TestMustRevalidate(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StaleWhileRevalidate = "5s"
		fc.StaleIfError = "10s"
	})

	var fail atomic.Bool
	var body atomic.Value
	body.Store("v1")

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if fail.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return nil
		}
		w.Header().Set("Cache-Control", "public, max-age=1, must-revalidate")
		_, _ = w.Write([]byte(body.Load().(string)))
		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(1100 * time.Millisecond)

	// Stale + must-revalidate: synchronous revalidation, fresh body served.
	body.Store("v2")

	w := get(t, fc, "/", nil, next)
	if w.Body.String() != "v2" {
		t.Fatalf("stale body served despite must-revalidate: %q", w.Body.String())
	}
	if strings.Contains(w.Header().Get("Cache-Status"), "fwd=stale") {
		t.Fatalf("Cache-Status %q claims a stale serve", w.Header().Get("Cache-Status"))
	}

	// Expire v2 and fail upstream: the 502 must pass through, no stale.
	time.Sleep(1100 * time.Millisecond)
	fail.Store(true)

	if w = get(t, fc, "/", nil, next); w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 passthrough, got %d", w.Code)
	}
}

// TestConditionalRevalidation304: a stale entry with validators is
// revalidated conditionally; a 304 merges headers/freshness into the stored
// entry (RFC 9111 §4.3.4) without re-fetching the body — both on the SWR
// background path and the synchronous stale_if_error path.
func TestConditionalRevalidation304(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StaleWhileRevalidate = "5s"
		fc.StaleIfError = "10s"
	})

	var calls atomic.Int64
	var sawCond atomic.Bool
	var send304 atomic.Bool

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if r.Header.Get("If-None-Match") == `"v1"` {
			sawCond.Store(true)
			if send304.Load() {
				w.Header().Set("Cache-Control", "public, max-age=60")
				w.Header().Set("X-New", "yes")
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
		}
		w.Header().Set("Cache-Control", "public, max-age=1")
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("body"))
		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(1100 * time.Millisecond)

	// Inside the SWR window: stale is served, background refresh sends the
	// stored validator and merges the 304.
	send304.Store(true)

	w := get(t, fc, "/", nil, next)
	if w.Body.String() != "body" {
		t.Fatalf("stale serve failed: %q", w.Body.String())
	}

	waitFor(t, "conditional refresh to complete", func() bool { return sawCond.Load() && calls.Load() >= 2 })
	waitFor(t, "entry fresh again after 304 merge", func() bool {
		w := get(t, fc, "/", nil, next)
		cs := w.Header().Get("Cache-Status")
		return strings.HasPrefix(cs, "fastcache; hit") && !strings.Contains(cs, "fwd=stale") &&
			w.Header().Get("X-New") == "yes" && w.Body.String() == "body"
	})

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 upstream calls (1 fetch + 1 revalidation), got %d", got)
	}
}

// TestHeuristicFreshness: without explicit freshness, Last-Modified implies
// 10% of the object's age (RFC 9111 §4.2.2), capped at the default TTL.
func TestHeuristicFreshness(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if r.URL.Path == "/old" {
			w.Header().Set("Last-Modified", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
		} else {
			w.Header().Set("Last-Modified", time.Now().Add(-3*time.Second).UTC().Format(http.TimeFormat))
		}
		_, _ = w.Write([]byte("x"))
		return nil
	})

	get(t, fc, "/old", nil, next)
	if w := get(t, fc, "/old", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("heuristically cacheable response was not stored")
	}

	// Modified 3s ago → heuristic TTL ~300ms; after 600ms it must re-fetch.
	get(t, fc, "/new", nil, next)
	time.Sleep(600 * time.Millisecond)

	if w := get(t, fc, "/new", nil, next); strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("heuristic TTL not applied: stale entry still served as fresh")
	}
}

// TestTargetedCacheControl: RFC 9213 targeted fields override Cache-Control
// for this cache — Fastcache-Cache-Control beats CDN-Cache-Control beats
// Cache-Control.
func TestTargetedCacheControl(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		switch r.URL.Path {
		case "/cdn-ttl":
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("CDN-Cache-Control", "max-age=300")
		case "/fast-override":
			w.Header().Set("Cache-Control", "max-age=300")
			w.Header().Set("CDN-Cache-Control", "max-age=300")
			w.Header().Set("Fastcache-Cache-Control", "no-store")
		}
		_, _ = w.Write([]byte("x"))
		return nil
	})

	// CDN-Cache-Control overrides Cache-Control: no-store ignored, cached.
	get(t, fc, "/cdn-ttl", nil, next)
	if w := get(t, fc, "/cdn-ttl", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("CDN-Cache-Control did not override Cache-Control: no-store")
	}

	// Fastcache-Cache-Control overrides both: no-store wins, not cached.
	get(t, fc, "/fast-override", nil, next)
	if w := get(t, fc, "/fast-override", nil, next); strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("Fastcache-Cache-Control: no-store was ignored")
	}
}

// TestCacheStatusParams exercises the RFC 9211 parameters on each path.
func TestCacheStatusParams(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	// Miss + stored.
	w := get(t, fc, "/", nil, next)
	cs := w.Header().Get("Cache-Status")
	if !strings.Contains(cs, "fwd=uri-miss") || !strings.Contains(cs, "stored") || !strings.Contains(cs, "key=") {
		t.Fatalf("miss Cache-Status %q lacks fwd=uri-miss; stored; key=", cs)
	}

	// Hit with remaining ttl.
	w = get(t, fc, "/", nil, next)
	cs = w.Header().Get("Cache-Status")
	if !strings.Contains(cs, "hit") || !strings.Contains(cs, "ttl=") {
		t.Fatalf("hit Cache-Status %q lacks hit; ttl=", cs)
	}

	// Unsafe method bypass.
	rec := httptest.NewRecorder()
	_ = fc.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "http://example.com/", nil), next)
	if cs = rec.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=request") {
		t.Fatalf("bypass Cache-Status %q lacks fwd=request", cs)
	}
}

// TestProvisionAndCleanup exercises the module lifecycle: Provision parses
// config, builds the index, registers the instance; Cleanup unregisters it
// and stops the background goroutines.
func TestProvisionAndCleanup(t *testing.T) {
	fc := &FastCache{
		Path:                 t.TempDir(),
		MemorySize:           "16MB",
		DiskSize:             "128MB",
		TTL:                  "1h",
		StaleWhileRevalidate: "30s",
		StaleIfError:         "1m",
		PurgeAllow:           []string{"10.0.0.0/8"},
	}

	if err := fc.Provision(caddy.Context{}); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if fc.idx == nil || fc.memoryBytes != 16_000_000 || fc.swr != 30*time.Second || fc.sie != time.Minute {
		t.Fatal("Provision did not wire config into the index")
	}

	if _, ok := liveCaches.Load(fc); !ok {
		t.Fatal("Provision did not register the instance")
	}

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	if w := get(t, fc, "/", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("provisioned cache did not serve a hit")
	}

	if err := fc.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	if _, ok := liveCaches.Load(fc); ok {
		t.Fatal("Cleanup did not unregister the instance")
	}
}

// TestProvisionBadConfig: malformed option values must fail Provision.
func TestProvisionBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*FastCache)
	}{
		{"bad memory_size", func(fc *FastCache) { fc.MemorySize = "gibbons" }},
		{"bad disk_size", func(fc *FastCache) { fc.DiskSize = "-1x" }},
		{"bad ttl", func(fc *FastCache) { fc.TTL = "tomorrow" }},
		{"bad swr", func(fc *FastCache) { fc.StaleWhileRevalidate = "soon" }},
		{"bad sie", func(fc *FastCache) { fc.StaleIfError = "eventually" }},
		{"bad purge_allow", func(fc *FastCache) { fc.PurgeAllow = []string{"not-an-ip"} }},
		{"bad promote_below", func(fc *FastCache) { fc.PromoteBelow = "lots" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &FastCache{Path: t.TempDir(), TTL: "1h"}
			tc.mutate(fc)

			if err := fc.Provision(caddy.Context{}); err == nil {
				t.Fatal("expected Provision to fail")
			}
		})
	}
}

// TestUnmarshalCaddyfile parses a full directive block and rejects unknown
// options.
func TestUnmarshalCaddyfile(t *testing.T) {
	fc := new(FastCache)

	d := caddyfile.NewTestDispenser(`fastcache {
    path                     /var/cache/x
    memory_size              32MB
    disk_size                512MB
    max_items                100
    ttl                      5m
    stale_while_revalidate   30s
    stale_if_error           1m
    vary                     Accept-Encoding Accept-Language
    disable_touch_on_hit
    purge_allow              10.0.0.0/8 192.168.1.5
    honor_request_cache_control
    cache_key                {http.request.host}{http.request.uri.path}
    match_header             Content-Type image/png image/jpeg
    promote_below            32KB
}`)

	if err := fc.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("UnmarshalCaddyfile: %v", err)
	}

	if fc.Path != "/var/cache/x" || fc.MemorySize != "32MB" || fc.DiskSize != "512MB" ||
		fc.MaxItems != 100 || fc.TTL != "5m" || fc.StaleWhileRevalidate != "30s" ||
		fc.StaleIfError != "1m" || fc.CacheKey != "{http.request.host}{http.request.uri.path}" ||
		!fc.DisableTouchOnHit || !fc.HonorRequestCC || fc.PromoteBelow != "32KB" {
		t.Fatalf("misparsed config: %+v", fc)
	}

	if len(fc.Vary) != 2 || len(fc.PurgeAllow) != 2 {
		t.Fatalf("vary/purge_allow: %+v", fc)
	}

	if got := fc.MatchHeaders["Content-Type"]; len(got) != 2 || got[0] != "image/png" {
		t.Fatalf("match_header: %+v", fc.MatchHeaders)
	}

	// Unknown option → error.
	bad := new(FastCache)
	if err := bad.UnmarshalCaddyfile(caddyfile.NewTestDispenser("fastcache {\n    bogus_opt x\n}")); err == nil {
		t.Fatal("expected error for unknown option")
	}
}

// TestCaptureWriterInterfaces: Flush passes through and Hijack reports
// ErrNotSupported when the downstream writer can't hijack.
func TestCaptureWriterInterfaces(t *testing.T) {
	fc := newTestCache(t, nil)

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		f, ok := w.(http.Flusher)
		if !ok {
			return errors.New("writer lost http.Flusher")
		}
		f.Flush()

		h, ok := w.(http.Hijacker)
		if !ok {
			return errors.New("writer lost http.Hijacker")
		}
		if _, _, err := h.Hijack(); !errors.Is(err, http.ErrNotSupported) {
			return fmt.Errorf("expected ErrNotSupported, got %v", err)
		}

		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	w := get(t, fc, "/", nil, next)
	if w.Code != http.StatusOK || !w.Flushed {
		t.Fatalf("expected 200 + flushed recorder, got %d flushed=%v", w.Code, w.Flushed)
	}
}

// TestMetrics: request/eviction counters move, and registration into a
// prometheus registry exposes the families.
func TestMetrics(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next) // miss + store
	get(t, fc, "/", nil, next) // hit

	// PURGE evicts the entry.
	pr := httptest.NewRequest("PURGE", "http://example.com/", nil)
	pr.RemoteAddr = "127.0.0.1:1234"
	prec := httptest.NewRecorder()
	if err := fc.ServeHTTP(prec, pr, next); err != nil || prec.Code != http.StatusOK {
		t.Fatalf("PURGE: %v code=%d", err, prec.Code)
	}

	// Unsafe method bypasses and invalidates.
	get(t, fc, "/", nil, next) // re-store
	rec := httptest.NewRecorder()
	_ = fc.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "http://example.com/", nil), next)

	m := fc.idx.metrics
	for result, want := range map[string]float64{"miss": 2, "hit": 1, "bypass": 1, "purged": 1} {
		if got := testutil.ToFloat64(m.requests.WithLabelValues(result)); got != want {
			t.Fatalf("requests_total{result=%q} = %v, want %v", result, got, want)
		}
	}
	for reason, want := range map[string]float64{"purge": 1, "invalidate": 1} {
		if got := testutil.ToFloat64(m.evictions.WithLabelValues(reason)); got != want {
			t.Fatalf("evictions_total{reason=%q} = %v, want %v", reason, got, want)
		}
	}

	// Registration into a fresh registry: all families scrape.
	reg := prometheus.NewRegistry()
	m.register(reg)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	fams := map[string]bool{}
	for _, mf := range mfs {
		fams[mf.GetName()] = true
	}
	for _, name := range []string{"fastcache_requests_total", "fastcache_revalidations_total", "fastcache_evictions_total", "fastcache_entries", "fastcache_bytes"} {
		if !fams[name] {
			t.Fatalf("metric %s missing from scrape", name)
		}
	}
}

// TestMetricsRevalidation: the revalidations counter reflects 304 merges
// and error fallbacks.
func TestMetricsRevalidation(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StaleIfError = "10s"
	})

	var send304 atomic.Bool
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if send304.Load() && r.Header.Get("If-None-Match") == `"v"` {
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		w.Header().Set("Cache-Control", "public, max-age=1")
		w.Header().Set("ETag", `"v"`)
		_, _ = w.Write([]byte("b"))
		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(1100 * time.Millisecond)

	// Past freshness, inside the SIE window: synchronous revalidation gets a
	// 304 and merges.
	send304.Store(true)
	if w := get(t, fc, "/", nil, next); w.Body.String() != "b" {
		t.Fatalf("expected merged entry body, got %q", w.Body.String())
	}

	if got := testutil.ToFloat64(fc.idx.metrics.revalidations.WithLabelValues("not_modified")); got != 1 {
		t.Fatalf("revalidations{not_modified} = %v, want 1", got)
	}
}

// TestPromoteBelow: bodies above promote_below stay disk-servable via
// sendfile and never land in the RAM tier; smaller ones promote as usual.
func TestPromoteBelow(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.PromoteBelow = "4KB" })

	small, large := make([]byte, 2*1024), make([]byte, 8*1024)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		if r.URL.Path == "/large" {
			_, _ = w.Write(large)
		} else {
			_, _ = w.Write(small)
		}
		return nil
	})

	get(t, fc, "/large", nil, next)
	get(t, fc, "/large", nil, next)

	e, _, ok := fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com/large", nil))
	if !ok {
		t.Fatal("expected entry for /large")
	}
	if e.body.Load() != nil {
		t.Error("8KB body should not promote with promote_below 4KB")
	}
	if calls.Load() != 1 {
		t.Errorf("disk hit should not reach upstream, got %d", calls.Load())
	}

	get(t, fc, "/small", nil, next)
	get(t, fc, "/small", nil, next)

	e, _, ok = fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com/small", nil))
	if !ok {
		t.Fatal("expected entry for /small")
	}
	if e.body.Load() == nil {
		t.Error("2KB body should promote with promote_below 4KB")
	}
}

// TestPromoteBelowZero disables RAM promotion entirely — every hit sendfiles.
func TestPromoteBelowZero(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.PromoteBelow = "0" })

	var calls atomic.Int64
	next := countingUpstream(&calls, make([]byte, 512), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)
	get(t, fc, "/", nil, next)

	e, _, ok := fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
	if !ok {
		t.Fatal("expected entry")
	}
	if e.body.Load() != nil {
		t.Error("nothing should promote with promote_below 0")
	}
	if calls.Load() != 1 {
		t.Errorf("disk hit should not reach upstream, got %d", calls.Load())
	}
}
