package fastcache

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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
	fc.statusTTL, _ = parseStatusTTLs(fc.StatusTTL)
	fc.revalidateTimeout, _ = parseDuration(fc.RevalidateTimeout, 30*time.Second)
	fc.lockTimeout, _ = parseDuration(fc.LockTimeout, 30*time.Second)

	fc.idx = newIndex(fc)
	if err := fc.idx.rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	fc.idx.start()

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
	return handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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
		{"set-cookie", http.Header{
			"Cache-Control": {"public, max-age=3600"},
			"Set-Cookie":    {"sid=abc; HttpOnly"},
		}, 200},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newTestCache(t, nil)

			var calls atomic.Int64

			next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

func TestStoreSetCookie(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.StoreSetCookie = true })

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{
		"Cache-Control": {"public, max-age=3600"},
		"Set-Cookie":    {"sid=abc; HttpOnly"},
	})

	w1 := get(t, fc, "/", nil, next)
	w2 := get(t, fc, "/", nil, next)

	if calls.Load() != 1 {
		t.Fatalf("store_set_cookie should store the response, got %d calls", calls.Load())
	}
	if got := w2.Header().Get("Set-Cookie"); got != "sid=abc; HttpOnly" {
		t.Fatalf("hit should replay the stored Set-Cookie, got %q", got)
	}
	_ = w1
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

func TestPurgeMissStatus(t *testing.T) {
	fc := newTestCache(t, nil)

	r := httptest.NewRequest("PURGE", "http://example.com/never-stored", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()

	if err := fc.ServeHTTP(w, r, nil); err != nil {
		t.Fatal(err)
	}

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for a missing key, got %d", w.Code)
	}
	if cs := w.Header().Get("Cache-Status"); cs == "fastcache; purged" {
		t.Fatalf("a 404 PURGE must not claim 'purged', got %q", cs)
	}
}

// TestPurgeAbortsInflightCommit: a PURGE issued while an upstream response
// is still being captured must prevent that capture's commit — otherwise
// the just-purged content is resurrected.
func TestPurgeAbortsInflightCommit(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("part1"))
		close(started)
		<-release
		_, _ = w.Write([]byte("part2"))

		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
	}()

	<-started

	// PURGE (of a not-yet-committed key) lands mid-capture.
	pr := httptest.NewRequest("PURGE", "http://example.com/", nil)
	pr.RemoteAddr = "127.0.0.1:1234"
	_ = fc.ServeHTTP(httptest.NewRecorder(), pr, next)

	close(release)
	<-done

	// The in-flight commit must have been dropped: next GET re-fetches.
	get(t, fc, "/", nil, countingUpstream(&calls, []byte("v2"), http.Header{"Cache-Control": {"public, max-age=3600"}}))

	if calls.Load() != 2 {
		t.Fatalf("purged content was committed by the racing capture, calls=%d", calls.Load())
	}
}

// TestMultiValuedHeaders: repeated Cache-Control and Vary field lines fold
// per RFC 9110 §5.3 and must be parsed as one comma-joined value.
func TestMultiValuedHeaders(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"X-A", "X-B"} })

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header()["Cache-Control"] = []string{"public", "max-age=3600"}
		w.Header()["Vary"] = []string{"X-A", "X-B"}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	mkReq := func(a, b string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		r.Header.Set("X-A", a)
		r.Header.Set("X-B", b)

		return r
	}

	_ = fc.ServeHTTP(httptest.NewRecorder(), mkReq("1", "2"), next)
	_ = fc.ServeHTTP(httptest.NewRecorder(), mkReq("1", "2"), next)

	if calls.Load() != 1 {
		t.Fatalf("multi-line Cache-Control/Vary should still store and hit, got %d calls", calls.Load())
	}
}

// TestClientValidatorsDontMerge: when the client supplied its own
// If-None-Match differing from the stored ETag, a 304 proves the client's
// copy fresh — not ours. The 304 forwards to the client; the entry is not
// refreshed.
func TestClientValidatorsDontMerge(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleWhileRevalidate = "0"
		fc.StaleIfError = "10s"
	})

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if inm := r.Header.Get("If-None-Match"); inm != "" {
			w.WriteHeader(http.StatusNotModified)

			return nil
		}
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(80 * time.Millisecond)

	// Stale entry; client presents a *different* validator.
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	r.Header.Set("If-None-Match", `"other"`)
	w := httptest.NewRecorder()
	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}

	if w.Code != http.StatusNotModified {
		t.Fatalf("client's 304 should pass through, got %d", w.Code)
	}

	// The entry must still be stale — not merged — so another request
	// revalidates upstream again instead of hitting.
	var recheck atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		get(t, fc, "/", nil, countingUpstream(&recheck, []byte("x"), nil))
	}()
	<-done

	if recheck.Load() != 1 {
		t.Fatal("a client-validator 304 must not refresh the stored entry")
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

// TestVaryKeyCollision: vary components are length-prefixed, so header
// values containing ';'/'=' cannot make two different header sets share a
// variant key.
func TestVaryKeyCollision(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"X-A", "X-B"} })

	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Vary", "X-A, X-B")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Header.Get("X-A") + "|" + r.Header.Get("X-B")))

		return nil
	})

	mkReq := func(a, b string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		r.Header.Set("X-A", a)
		r.Header.Set("X-B", b)

		return r
	}

	w1 := httptest.NewRecorder()
	_ = fc.ServeHTTP(w1, mkReq("1", "z;X-B=q"), next)
	w2 := httptest.NewRecorder()
	_ = fc.ServeHTTP(w2, mkReq("1;X-B=z", "q"), next)
	w3 := httptest.NewRecorder()
	_ = fc.ServeHTTP(w3, mkReq("1", "z;X-B=q"), next)

	if calls.Load() != 2 {
		t.Fatalf("colliding vary keys merged two variants: %d upstream calls", calls.Load())
	}
	if got := w3.Body.String(); got != "1|z;X-B=q" {
		t.Fatalf("hit served the wrong variant: %q", got)
	}
}

// TestVaryMultiValue: repeated header lines and one comma-folded line are
// equivalent field values and must key the same variant.
func TestVaryMultiValue(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"X-Lang"} })

	var calls atomic.Int64

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Vary", "X-Lang")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	r1 := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	r1.Header.Add("X-Lang", "en")
	r1.Header.Add("X-Lang", "fr")

	_ = fc.ServeHTTP(httptest.NewRecorder(), r1, next)

	r2 := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	r2.Header.Set("X-Lang", "en,fr")

	_ = fc.ServeHTTP(httptest.NewRecorder(), r2, next)

	if calls.Load() != 1 {
		t.Fatalf("equivalent multi-valued header should hit one variant, got %d calls", calls.Load())
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

	fc2.idx.start()

	defer fc2.idx.stop()

	w := get(t, fc2, "/", nil, next)
	if w.Body.String() != "persistent" {
		t.Fatalf("expected rebuilt hit, got %q", w.Body.String())
	}

	if calls.Load() != 1 {
		t.Errorf("rebuilt index should serve without upstream, got %d calls", calls.Load())
	}
}

// TestRebuildRejectsOldMagic: metas written under a previous format version
// (FCH1) carry stale varyKey encodings — they must be rejected and their
// files reaped rather than loaded as unreachable entries.
func TestRebuildRejectsOldMagic(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("persistent"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)
	fc.idx.stop()

	// Downgrade the meta file to the previous format version.
	var metaPath string
	for _, mp := range findMeta(t, fc.Path) {
		metaPath = mp
	}
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	copy(data[:4], "FCH1")
	if err := os.WriteFile(metaPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	fc2 := &FastCache{Path: fc.Path, MemorySize: "64MB", DiskSize: "1GB", TTL: "1h"}
	fc2.memoryBytes, _ = parseSize(fc2.MemorySize, 0)
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.ttl, _ = parseDuration(fc2.TTL, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	fc2.idx.start()

	defer fc2.idx.stop()

	if fc2.idx.fileCount.Load() != 0 {
		t.Fatalf("FCH1 meta should not be indexed, got %d entries", fc2.idx.fileCount.Load())
	}
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("FCH1 meta should have been reaped at rebuild")
	}
}

// findMeta returns the .meta sidecar paths under dir.
func findMeta(t *testing.T, dir string) []string {
	t.Helper()

	var out []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".meta") {
			out = append(out, p)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func TestDiskBudgetEviction(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.DiskSize = "2KB" })

	var calls atomic.Int64

	for i, key := range []string{"/a", "/b", "/c"} {
		body := fmt.Appendf(nil, "body-%d-%s", i, string(make([]byte, 700)))
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
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

// TestRefreshVarsIsolation: the request-scoped vars map must not be
// shared with the background SWR fetch. Handlers downstream write to it
// (reverse_proxy calls SetVar on every proxied request) while the
// caller's request is still being read — access logging calls GetVar on
// every request — which in production is a fatal "concurrent map read
// and map write". The final assertion fails deterministically if the
// background write leaks into the caller's map; under -race, the read
// loop reports the overlap.
func TestRefreshVarsIsolation(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.StaleWhileRevalidate = "5s" })

	varsReq := func(t *testing.T, next caddyhttp.Handler) (*http.Request, *httptest.ResponseRecorder) {
		t.Helper()

		r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		ctx := context.WithValue(r.Context(), caddyhttp.VarsCtxKey, map[string]any{
			caddyhttp.ClientIPVarKey: "1.2.3.4",
		})
		r = r.WithContext(ctx)

		w := httptest.NewRecorder()
		if err := fc.ServeHTTP(w, r, next); err != nil {
			t.Fatalf("ServeHTTP: %v", err)
		}

		return r, w
	}

	ok := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("v1"))

		return nil
	})
	varsReq(t, ok)

	time.Sleep(1100 * time.Millisecond)

	// Stale and inside the SWR window: the caller gets the old body while
	// refresh() re-fetches in a goroutine. The write mirrors what
	// reverse_proxy does to every request it proxies.
	release := make(chan struct{})
	writing := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		caddyhttp.SetVar(r.Context(), "bg_write", "1")
		<-release

		w.Header().Set("Cache-Control", "public, max-age=1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("v2"))

		return nil
	})
	r2, w2 := varsReq(t, writing)
	if w2.Body.String() != "v1" {
		t.Fatalf("expected stale body, got %q", w2.Body.String())
	}

	// While the background write is in flight, hammer reads on the
	// caller's map — what access logging does at request end.
	for range 100000 {
		_ = caddyhttp.GetVar(r2.Context(), caddyhttp.ClientIPVarKey)
	}
	close(release)

	waitFor(t, "background revalidation to commit v2", func() bool {
		return get(t, fc, "/", nil, ok).Body.String() == "v2"
	})

	if v := caddyhttp.GetVar(r2.Context(), "bg_write"); v != nil {
		t.Fatalf("background SetVar leaked into caller's vars map: %v", v)
	}
}

// TestOrphanTmpFiles verifies .tmp files from crashed stores get cleaned by
// rebuild (they lack the magic header).
func TestOrphanTmpFiles(t *testing.T) {
	fc := newTestCache(t, nil)

	orphan := filepath.Join(fc.Path, "ab", "0123456789abcdef01234567-1.tmp")
	if err := os.MkdirAll(filepath.Dir(orphan), 0o755); err != nil {
		t.Fatal(err)
	}
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

// TestRebuildPreservesForeignFiles: files that don't look like fastcache
// data (non-matching names, foreign directories) must survive a rebuild —
// path may be misconfigured to a shared directory.
func TestRebuildPreservesForeignFiles(t *testing.T) {
	fc := newTestCache(t, nil)

	foreign := filepath.Join(fc.Path, "notes.txt")
	foreignDir := filepath.Join(fc.Path, "otherapp", "data.bin")

	if err := os.MkdirAll(filepath.Dir(foreignDir), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{foreign, foreignDir} {
		if err := os.WriteFile(p, []byte("keep me"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	fc.idx.stop()

	fc2 := &FastCache{Path: fc.Path, MemorySize: "64MB", DiskSize: "1GB", TTL: "1h"}
	fc2.memoryBytes, _ = parseSize(fc2.MemorySize, 0)
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatal(err)
	}

	defer fc2.idx.stop()

	for _, p := range []string{foreign, foreignDir} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("foreign file removed on rebuild: %s", p)
		}
	}
	if _, err := os.Stat(filepath.Dir(foreignDir)); err != nil {
		t.Error("foreign directory removed on rebuild")
	}
}

// TestRebuildOverBudgetEvicts: a restored index that already exceeds
// disk_size must start evicting without waiting for a new store.
func TestRebuildOverBudgetEvicts(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, make([]byte, 700), http.Header{"Cache-Control": {"public, max-age=3600"}})

	for _, key := range []string{"/a", "/b", "/c"} {
		get(t, fc, key, nil, next)
	}
	fc.idx.stop()

	fc2 := &FastCache{Path: fc.Path, MemorySize: "0", DiskSize: "1KB", TTL: "1h"}
	fc2.diskBytes, _ = parseSize(fc2.DiskSize, 0)
	fc2.ttl, _ = parseDuration(fc2.TTL, 0)
	fc2.idx = newIndex(fc2)

	if err := fc2.idx.rebuild(); err != nil {
		t.Fatal(err)
	}
	fc2.idx.start()

	defer fc2.idx.stop()

	waitFor(t, "over-budget rebuild to evict", func() bool {
		return fc2.idx.diskSize.Load() <= 1024
	})
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

// TestPromoteAfterEvictionNoLeak: a hit that promotes a body must not
// resurrect RAM accounting on an entry evicted mid-serve — memSize would
// leak forever since freeRAM only scans indexed entries.
func TestPromoteAfterEvictionNoLeak(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, make([]byte, 4096), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	e, _, ok := fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
	if !ok {
		t.Fatal("expected entry")
	}

	// Demote to disk so the next hit takes the promotion path.
	if b := e.body.Swap(nil); b != nil {
		fc.idx.memSize.Add(-int64(len(*b)))
	}

	_, done, err := fc.idx.openBody(e)
	if err != nil {
		t.Fatalf("openBody: %v", err)
	}

	// Evict while the serve holds the body file open, then let the
	// promotion run — it must roll its accounting back.
	fc.idx.deleteEntry(e)
	done()

	if e.body.Load() != nil {
		t.Error("body promoted into an evicted entry")
	}
	if got := fc.idx.memSize.Load(); got != 0 {
		t.Errorf("memSize leaked: got %d, want 0", got)
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

			next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

// assertNoFlights fails the test if any flight records leaked.
func assertNoFlights(t *testing.T, fc *FastCache) {
	t.Helper()
	fc.flights.Range(func(k, _ any) bool {
		t.Errorf("leaked flight %v", k)
		return true
	})
}

// TestLeaderPanicReleasesFlight — the production wedge: reverse_proxy
// panics http.ErrAbortHandler when a client disconnects mid-stream. The
// panic unwinds through ServeHTTP; the flight must still be released or
// every later request for the key parks on a channel that never closes.
func TestLeaderPanicReleasesFlight(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		if n == 1 {
			_, _ = w.Write([]byte("partial"))
			cancel()                    // client disconnected mid-stream
			panic(http.ErrAbortHandler) // reverse_proxy's response to the copy error
		}
		_, _ = w.Write([]byte("full"))

		return nil
	})

	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(ctx)
	func() {
		defer func() {
			if p := recover(); p != http.ErrAbortHandler {
				t.Fatalf("expected ErrAbortHandler to propagate, got %v", p)
			}
		}()
		_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
	}()
	assertNoFlights(t, fc)

	// The panicked fill committed nothing: the same key re-fetches
	// immediately instead of hanging.
	w := get(t, fc, "/", nil, next)
	if w.Body.String() != "full" {
		t.Fatalf("expected re-fetched body, got %q", w.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls, got %d", calls.Load())
	}

	// The re-fetch stored: the key is a normal entry again.
	if w = get(t, fc, "/", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatalf("expected stored entry after panic recovery, got %q", w.Header().Get("Cache-Status"))
	}
	assertNoFlights(t, fc)
}

// TestClientGoneMidFillCompletesStore: a fill is detached from its client —
// canceling mid-body no longer kills it. The fill completes and stores so
// coalesced followers hit the finished entry instead of re-filling.
func TestClientGoneMidFillCompletesStore(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	midFill := make(chan struct{})
	gate := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("first-"))
		if n == 1 {
			close(midFill)
			<-gate
		}
		_, _ = w.Write([]byte("second"))

		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(ctx)

	done := make(chan error, 1)
	go func() { done <- fc.ServeHTTP(httptest.NewRecorder(), r, next) }()

	<-midFill
	cancel()
	time.Sleep(30 * time.Millisecond) // let the detach watcher swap the writer
	close(gate)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("detached fill should complete cleanly, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not finish the detached fill")
	}
	assertNoFlights(t, fc)

	// The fill completed without its client and committed: the next
	// request is a hit, not a re-fill.
	w := get(t, fc, "/", nil, next)
	if w.Body.String() != "first-second" {
		t.Fatalf("expected cached body, got %q", w.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 upstream call (fill completed detached), got %d", calls.Load())
	}
}

// TestCancelMidFillDoesNotWedgeKey — the production repro: a fill whose
// client cancels mid-response must not permanently wedge the cache key.
// When the abandoned fill dies (revalidate_timeout bounds the clientless
// fetch), the next request must fetch and complete normally.
func TestCancelMidFillDoesNotWedgeKey(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.RevalidateTimeout = "60ms" })

	var calls atomic.Int64
	midFill := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		if n == 1 {
			_, _ = w.Write([]byte("partial"))
			close(midFill)
			<-r.Context().Done() // abandoned once revalidate_timeout fires
			return r.Context().Err()
		}
		_, _ = w.Write([]byte("full"))

		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(ctx)

	errc := make(chan error, 1)
	go func() { errc <- fc.ServeHTTP(httptest.NewRecorder(), r, next) }()

	<-midFill
	cancel()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected the abandoned fill to fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("abandoned fill did not end within revalidate_timeout")
	}
	assertNoFlights(t, fc)

	// The canceled fill poisoned nothing: the same key fetches fresh and
	// completes — before the fix this request hung forever.
	w := get(t, fc, "/", nil, next)
	if w.Body.String() != "full" {
		t.Fatalf("expected re-fetched body, got %q", w.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls, got %d", calls.Load())
	}
}

// TestLockTimeoutFallback: a follower parked behind a stuck leader escapes
// after lock_timeout and fetches upstream itself.
func TestLockTimeoutFallback(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.LockTimeout = "50ms" })

	var calls atomic.Int64
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	defer func() { close(release); <-leaderDone; assertNoFlights(t, fc) }()

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		if n == 1 {
			<-release // the leader's fill stays stuck
		}
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("body"))

		return nil
	})

	go func() {
		defer close(leaderDone)
		r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
	}()
	waitFor(t, "leader in flight", func() bool { return calls.Load() == 1 })

	// An unrelated key is unaffected by the stuck leader.
	if w := get(t, fc, "/other", nil, next); w.Body.String() != "body" {
		t.Fatalf("unrelated key blocked behind stuck leader: %q", w.Body.String())
	}

	// Parked behind the stuck leader, the follower must escape on
	// lock_timeout and fetch upstream itself.
	start := time.Now()
	w := get(t, fc, "/", nil, next)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("follower waited %s on a stuck leader", elapsed)
	}
	if w.Body.String() != "body" {
		t.Fatalf("expected direct fetch body, got %q", w.Body.String())
	}
	if calls.Load() != 3 {
		t.Fatalf("expected follower to fetch upstream itself, got %d calls", calls.Load())
	}
}

// TestSIELockTimeout: a must-revalidate follower parked behind a stuck
// revalidation escapes after lock_timeout and revalidates upstream itself.
func TestSIELockTimeout(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.LockTimeout = "50ms" })

	var calls atomic.Int64
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	defer func() { close(release); <-leaderDone; assertNoFlights(t, fc) }()

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=1, must-revalidate")
		if n == 2 {
			<-release // the coalesced revalidation stays stuck
		}
		_, _ = w.Write([]byte("v"))

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(1100 * time.Millisecond)

	go func() {
		defer close(leaderDone)
		get(t, fc, "/", nil, next)
	}()
	waitFor(t, "revalidation in flight", func() bool { return calls.Load() == 2 })

	// must-revalidate forbids serving stale — the follower escapes the
	// parked flight on lock_timeout and revalidates upstream itself.
	w := get(t, fc, "/", nil, next)
	if w.Body.String() != "v" {
		t.Fatalf("expected revalidated body, got %q", w.Body.String())
	}
	if calls.Load() != 3 {
		t.Fatalf("expected follower to revalidate directly, got %d calls", calls.Load())
	}
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
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

// TestStaleFallbackHeaderIsolation: headers the failing upstream wrote must
// not leak into the served stale entry — captureWriter buffers upstream
// headers privately and only commits them to the client on a real forward.
func TestStaleFallbackHeaderIsolation(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleIfError = "5s"
	})

	var calls atomic.Int64
	fail := 0 // 0 = ok, 1 = 5xx, 2 = handler error
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)

		if fail != 0 {
			w.Header().Set("X-Leak", "1")
			w.Header().Set("Set-Cookie", "sid=bad")
			if fail == 2 {
				return errors.New("upstream dead")
			}
			w.WriteHeader(http.StatusBadGateway)

			return nil
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("v1"))

		return nil
	})

	get(t, fc, "/", nil, next) // stores v1 (no CC → ttl=50ms)
	time.Sleep(80 * time.Millisecond)

	for _, fail = range []int{1, 2} {
		w := get(t, fc, "/", nil, next)
		if w.Code != http.StatusOK || w.Body.String() != "v1" {
			t.Fatalf("fail=%d: expected stale fallback, got %d %q", fail, w.Code, w.Body.String())
		}
		if w.Header().Get("X-Leak") != "" || w.Header().Get("Set-Cookie") != "" {
			t.Errorf("fail=%d: upstream error headers leaked into the stale serve", fail)
		}
	}
}

// TestHopByHopNotStored: fields nominated by the response's Connection
// header and trailer-value keys are hop-by-hop — neither stored nor
// forwarded on a miss.
func TestHopByHopNotStored(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Connection", "X-Internal")
		w.Header().Set("X-Internal", "secret")
		w.Header().Add(http.TrailerPrefix+"X-Trailer-Val", "tl")
		w.Header().Set("X-Normal", "kept")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	w1 := get(t, fc, "/", nil, next)
	w2 := get(t, fc, "/", nil, next)

	if calls.Load() != 1 {
		t.Fatalf("expected stored response, got %d calls", calls.Load())
	}

	for _, w := range []*httptest.ResponseRecorder{w1, w2} {
		if w.Header().Get("X-Internal") != "" {
			t.Error("Connection-nominated header reached the client")
		}
		if w.Header().Get(http.TrailerPrefix+"X-Trailer-Val") != "" || w.Header().Get("X-Trailer-Val") != "" {
			t.Error("trailer value key reached the client")
		}
		if w.Header().Get("X-Normal") != "kept" {
			t.Error("normal upstream header was dropped")
		}
	}
}

// rw1xx wraps a recorder with real-server 1xx semantics: informational
// WriteHeader codes are recorded but don't end the response (httptest's
// ResponseRecorder treats the first WriteHeader as final).
type rw1xx struct {
	http.ResponseWriter
	codes []int
}

func (w *rw1xx) WriteHeader(code int) {
	w.codes = append(w.codes, code)
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		return
	}

	w.ResponseWriter.WriteHeader(code)
}

// TestInformationalPassthrough: a 1xx (e.g. 103 Early Hints) must forward
// to the client without triggering the cacheability decision — the final
// WriteHeader is what decides.
func TestInformationalPassthrough(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rec := httptest.NewRecorder()
	w := &rw1xx{ResponseWriter: rec}

	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}

	if len(w.codes) != 2 || w.codes[0] != http.StatusEarlyHints || w.codes[1] != http.StatusOK {
		t.Fatalf("expected codes [103 200], got %v", w.codes)
	}

	w2 := get(t, fc, "/", nil, next)
	if calls.Load() != 1 {
		t.Fatalf("response after 103 was not cached: %d calls", calls.Load())
	}
	if w2.Header().Get("Cache-Control") == "" || w2.Header().Get("Link") == "" {
		t.Error("headers set around the 1xx were dropped")
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

	// An empty pattern would match every key — rejected as a guard.
	ew := httptest.NewRecorder()
	err := purgeCacheKeys(ew, httptest.NewRequest(http.MethodDelete, "/fastcache/purge", strings.NewReader(`{"pattern": ""}`)))
	var apiErr caddy.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty pattern, got %v", err)
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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
		{"bad revalidate_timeout", func(fc *FastCache) { fc.RevalidateTimeout = "later" }},
		{"bad purge_allow", func(fc *FastCache) { fc.PurgeAllow = []string{"not-an-ip"} }},
		{"bad promote_below", func(fc *FastCache) { fc.PromoteBelow = "lots" }},
		{"negative memory_size", func(fc *FastCache) { fc.MemorySize = "-1" }},
		{"negative disk_size", func(fc *FastCache) { fc.DiskSize = "-1" }},
		{"negative ttl", func(fc *FastCache) { fc.TTL = "-1h" }},
		{"negative swr", func(fc *FastCache) { fc.StaleWhileRevalidate = "-5s" }},
		{"negative sie", func(fc *FastCache) { fc.StaleIfError = "-5s" }},
		{"negative revalidate_timeout", func(fc *FastCache) { fc.RevalidateTimeout = "-5s" }},
		{"negative max_items", func(fc *FastCache) { fc.MaxItems = -5 }},
		{"negative promote_below", func(fc *FastCache) { fc.PromoteBelow = "-1" }},
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
    name                     edge
    memory_size              32MB
    disk_size                512MB
    max_items                100
    ttl                      5m
    stale_while_revalidate   30s
    stale_if_error           1m
    status_ttl               404 30s
    status_ttl               500 502 503 10s
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

	if fc.Path != "/var/cache/x" || fc.Name != "edge" || fc.MemorySize != "32MB" || fc.DiskSize != "512MB" ||
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

	if fc.StatusTTL[404] != "30s" || fc.StatusTTL[500] != "10s" || fc.StatusTTL[502] != "10s" || fc.StatusTTL[503] != "10s" || len(fc.StatusTTL) != 4 {
		t.Fatalf("status_ttl: %+v", fc.StatusTTL)
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

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
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

	// Unwrap exposes the wrapped writer to http.ResponseController probes.
	rec := httptest.NewRecorder()
	cw := newCaptureWriter(fc, rec, httptest.NewRequest(http.MethodGet, "/", nil), "k")
	if cw.Unwrap() != http.ResponseWriter(rec) {
		t.Error("captureWriter.Unwrap does not return the wrapped writer")
	}

	sw := &statusRW{ResponseWriter: rec}
	if sw.Unwrap() != http.ResponseWriter(rec) {
		t.Error("statusRW.Unwrap does not return the wrapped writer")
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
		if got := testutil.ToFloat64(m.requests.WithLabelValues("", result)); got != want {
			t.Fatalf("requests_total{result=%q} = %v, want %v", result, got, want)
		}
	}
	for reason, want := range map[string]float64{"purge": 1, "invalidate": 1} {
		if got := testutil.ToFloat64(m.evictions.WithLabelValues("", reason)); got != want {
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

	if got := testutil.ToFloat64(fc.idx.metrics.revalidations.WithLabelValues("", "not_modified")); got != 1 {
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

// cacheFiles counts files directly inside shard directories under dir.
func cacheFiles(t *testing.T, dir string) int {
	t.Helper()

	n := 0
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && shardDirRe.MatchString(filepath.Base(filepath.Dir(p))) {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return n
}

// TestCommitReplaceNoLeak: when a commit replaces a live variant (a 200
// answer to a conditional revalidation is the common trigger), the
// replaced entry's files must be removed and its accounting dropped —
// removeEntry's CAS cannot fire because entries already holds the new one.
func TestCommitReplaceNoLeak(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleIfError = "5s"
	})

	var calls atomic.Int64
	body := "v1"
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public") // freshness = configured TTL (50ms)
		_, _ = w.Write([]byte(body))

		return nil
	})

	get(t, fc, "/", nil, next) // stores v1

	before := fc.idx.diskSize.Load()
	if files := cacheFiles(t, fc.Path); files != 2 {
		t.Fatalf("expected 2 files (body+meta) after store, got %d", files)
	}

	time.Sleep(80 * time.Millisecond)

	body = "v2"
	w := get(t, fc, "/", nil, next) // SIE revalidation, 200 replaces entry
	if w.Body.String() != "v2" {
		t.Fatalf("expected v2, got %q", w.Body.String())
	}

	if got := fc.idx.fileCount.Load(); got != 1 {
		t.Errorf("fileCount = %d, want 1 (replaced variant leaked)", got)
	}
	if got := fc.idx.diskSize.Load(); got != before {
		t.Errorf("diskSize = %d, want %d (replaced variant still counted)", got, before)
	}
	if files := cacheFiles(t, fc.Path); files != 2 {
		t.Errorf("expected 2 files on disk after replace, got %d", files)
	}
}

// TestNoCacheNotStored: Cache-Control: no-cache forbids reuse without
// successful revalidation — the cache stores nothing, so every request
// reaches upstream.
func TestNoCacheNotStored(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, no-cache"}})

	get(t, fc, "/", nil, next)
	w := get(t, fc, "/", nil, next)

	if strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("no-cache response was served from cache")
	}
	if calls.Load() != 2 {
		t.Fatalf("no-cache response should revalidate upstream every time, got %d calls", calls.Load())
	}
}

// TestSMaxAgeNoStale: s-maxage implies proxy-revalidate for a shared cache
// (RFC 9111 §5.2.2.10) — an s-maxage entry past its freshness must
// revalidate synchronously, never serve stale even inside an SWR window.
func TestSMaxAgeNoStale(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StaleWhileRevalidate = "5s"
		fc.StaleIfError = "10s"
	})

	var calls atomic.Int64
	body := "v1"
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, s-maxage=1")
		_, _ = w.Write([]byte(body))

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(1100 * time.Millisecond)

	body = "v2"
	w := get(t, fc, "/", nil, next)

	if w.Body.String() != "v2" {
		t.Fatalf("s-maxage entry served stale %q inside SWR window", w.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("expected synchronous revalidation, got %d calls", calls.Load())
	}
}

// TestHopByHopNotForwarded: every hop-by-hop field — the static list, not
// just Connection-nominated ones — must be stripped before the upstream
// response reaches the client.
func TestHopByHopNotForwarded(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("Upgrade", "h2c")
		w.Header().Set("Proxy-Authenticate", "Basic realm=up")
		w.Header().Set("Te", "trailers")
		w.Header().Set("Trailer", "X-T")
		w.Header().Set("Connection", "X-Internal")
		w.Header().Set("X-Internal", "secret")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	w1 := get(t, fc, "/", nil, next)
	w2 := get(t, fc, "/", nil, next)
	if calls.Load() != 1 {
		t.Fatalf("expected stored response, got %d calls", calls.Load())
	}

	for _, w := range []*httptest.ResponseRecorder{w1, w2} {
		for _, h := range []string{"Keep-Alive", "Upgrade", "Proxy-Authenticate", "Te", "Trailer", "Connection", "X-Internal"} {
			if w.Header().Get(h) != "" {
				t.Errorf("hop-by-hop header %q reached the client", h)
			}
		}
	}
}

// hijackRW makes a ResponseRecorder hijackable over a net.Pipe.
type hijackRW struct {
	http.ResponseWriter
	c net.Conn
}

func (h *hijackRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return h.c, bufio.NewReadWriter(bufio.NewReader(h.c), bufio.NewWriter(h.c)), nil
}

// TestHijackDiscards: a handler that hijacks the connection mid-capture
// must never commit — an undeclared-length body would otherwise be stored
// truncated.
func TestHijackDiscards(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		if n == 1 {
			c, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return err
			}

			return c.Close()
		}

		return nil
	})

	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	server, _ := net.Pipe()
	w := &hijackRW{ResponseWriter: httptest.NewRecorder(), c: server}
	defer func() { _ = server.Close() }()

	if err := fc.ServeHTTP(w, r, next); err != nil {
		t.Fatal(err)
	}

	// The hijacked capture must be gone — nothing may reach the index.
	if _, _, ok := fc.idx.lookup(r); ok {
		t.Fatal("hijacked capture was committed")
	}

	// A normal fetch stores and then serves hits.
	get(t, fc, "/", nil, next)
	if w := get(t, fc, "/", nil, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatalf("expected hit, got %q", w.Header().Get("Cache-Status"))
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls, got %d", calls.Load())
	}
}

// TestRefreshTimeout: a background revalidation is bounded by
// revalidate_timeout — a hung upstream can't pin the flight forever.
func TestRefreshTimeout(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleWhileRevalidate = "5s"
		fc.RevalidateTimeout = "60ms"
	})

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		n := calls.Add(1)
		if n == 1 {
			w.Header().Set("Cache-Control", "public, max-age=1")
			_, _ = w.Write([]byte("v1"))

			return nil
		}

		// Refresh fetches block until the detached context times out.
		<-r.Context().Done()

		return r.Context().Err()
	})

	get(t, fc, "/", nil, next)
	time.Sleep(80 * time.Millisecond)

	// Each stale hit re-arms the refresh; while refresh #1 still blocks, the
	// "reval:" flight suppresses new fetches. Once the timeout kills it, the
	// next stale hit launches refresh #2.
	waitFor(t, "a second refresh after timeout", func() bool {
		get(t, fc, "/", nil, next)

		return calls.Load() >= 3
	})
}

// TestMissFollowerNoHerd: when the leader's response is not cacheable,
// followers must serialize through subsequent flights instead of all
// fetching upstream concurrently.
func TestMissFollowerNoHerd(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls, inflight, maxInflight atomic.Int64
	release := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		cur := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if cur <= m || maxInflight.CompareAndSwap(m, cur) {
				break
			}
		}
		<-release
		inflight.Add(-1)

		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte("x"))

		return nil
	})

	const n = 3 // < maxFlightRounds so all requests stay flight-coalesced

	start := make(chan struct{})

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			<-start
			r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
			_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
		})
	}
	close(start)

	// Wait for followers to pile up, then release fetches one at a time.
	time.Sleep(50 * time.Millisecond)
	for range n {
		release <- struct{}{}
	}
	wg.Wait()

	if m := maxInflight.Load(); m > 1 {
		t.Fatalf("followers fetched concurrently: max in-flight upstream = %d", m)
	}
}

// TestSIEFollowerCoalesce: concurrent hits on a stale must-revalidate
// entry coalesce into a single revalidation.
func TestSIEFollowerCoalesce(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	release := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=1, must-revalidate")
		if n > 1 {
			<-release
		}
		_, _ = w.Write([]byte("v"))

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(1100 * time.Millisecond)

	const n = 8

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			get(t, fc, "/", nil, next)
		})
	}

	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls (1 store + 1 coalesced revalidation), got %d", calls.Load())
	}
}

// TestRebuildForeignShardNames: a file named like a fastcache file but
// inside a foreign subdirectory must survive rebuild.
func TestRebuildForeignShardNames(t *testing.T) {
	fc := newTestCache(t, nil)

	foreign := filepath.Join(fc.Path, "otherapp", "0123456789abcdef01234567-1")
	if err := os.MkdirAll(filepath.Dir(foreign), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreign, []byte("keep me"), 0o644); err != nil {
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
	defer fc2.idx.stop()

	if _, err := os.Stat(foreign); err != nil {
		t.Error("cachefile-named foreign file was reaped on rebuild")
	}
}

// TestRebuildVaryConflict: two metas for the same base key disagreeing on
// VaryHeaders — the first-loaded scheme wins, the conflicting entry is
// dropped instead of indexed unreachable.
func TestRebuildVaryConflict(t *testing.T) {
	dir := t.TempDir()
	shard := filepath.Join(dir, "ab")
	if err := os.MkdirAll(shard, 0o755); err != nil {
		t.Fatal(err)
	}

	write := func(name string, m *metaHeader) {
		t.Helper()
		body := filepath.Join(shard, name)
		if err := os.WriteFile(body, []byte("x"), 0o640); err != nil {
			t.Fatal(err)
		}
		mf, err := os.OpenFile(body+".meta", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writeMeta(mf, m); err != nil {
			t.Fatal(err)
		}
		_ = mf.Close()
	}

	now := time.Now().UnixNano()
	write("aabbccddeeff001122334455-1", &metaHeader{
		Key: "example.com/x", VaryHeaders: []string{"A"}, VaryKey: "1:A1:v1;",
		Status: 200, Headers: map[string][]string{"A": {"v1"}},
		StoredAt: now, ExpiresAt: now + int64(time.Hour), StaleAt: now + int64(time.Hour),
	})
	write("bbccddeeff00112233445566-2", &metaHeader{
		Key: "example.com/x", VaryHeaders: []string{"B"}, VaryKey: "1:B1:v2;",
		Status: 200, Headers: map[string][]string{"B": {"v2"}},
		StoredAt: now, ExpiresAt: now + int64(time.Hour), StaleAt: now + int64(time.Hour),
	})

	fc := &FastCache{Path: dir, MemorySize: "64MB", DiskSize: "1GB", TTL: "1h"}
	fc.memoryBytes, _ = parseSize(fc.MemorySize, 0)
	fc.diskBytes, _ = parseSize(fc.DiskSize, 0)
	ix := newIndex(fc)
	if err := ix.rebuild(); err != nil {
		t.Fatal(err)
	}
	defer ix.stop()

	if got := ix.fileCount.Load(); got != 1 {
		t.Fatalf("conflicting metas should index one entry, got %d", got)
	}
	if files := cacheFiles(t, dir); files != 2 {
		t.Errorf("conflicting entry's files should be dropped, got %d files", files)
	}
}

// TestVaryOrderStable: the Vary list's order is canonicalized at capture —
// an upstream reordering must not drop existing variants.
func TestVaryOrderStable(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"X-A", "X-B"} })

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		n := calls.Add(1)
		if n%2 == 0 {
			w.Header().Set("Vary", "X-B, X-A") // reversed order
		} else {
			w.Header().Set("Vary", "X-A, X-B")
		}
		_, _ = w.Write([]byte("v" + r.Header.Get("X-A") + r.Header.Get("X-B")))

		return nil
	})

	h1 := http.Header{"X-A": {"1"}, "X-B": {"1"}}
	h2 := http.Header{"X-A": {"2"}, "X-B": {"2"}}

	get(t, fc, "/", h1, next) // stores under [X-A X-B]
	get(t, fc, "/", h2, next) // stores under reordered Vary — same scheme

	if w := get(t, fc, "/", h1, next); !strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatalf("Vary reorder dropped the first variant, got %q", w.Header().Get("Cache-Status"))
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls, got %d", calls.Load())
	}
}

// TestIfMatch: If-Match uses strong comparison; a mismatch is a 412, "*"
// matches any stored entity, and a weak stored ETag matches nothing.
func TestIfMatch(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}, "ETag": {`"v1"`}})

	get(t, fc, "/", nil, next)

	req := func(h http.Header) *httptest.ResponseRecorder {
		return get(t, fc, "/", h, next)
	}

	if w := req(http.Header{"If-Match": {`"nope"`}}); w.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-Match mismatch: got %d, want 412", w.Code)
	}
	if w := req(http.Header{"If-Match": {`"v1"`}}); w.Code != http.StatusOK {
		t.Fatalf("If-Match match: got %d, want 200", w.Code)
	}
	if w := req(http.Header{"If-Match": {"*"}}); w.Code != http.StatusOK {
		t.Fatalf("If-Match *: got %d, want 200", w.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("conditionals should hit the cache, got %d calls", calls.Load())
	}

	// * matches any current representation — even one stored without an
	// ETag.
	fc2 := newTestCache(t, nil)
	var calls2 atomic.Int64
	next2 := countingUpstream(&calls2, []byte("y"), http.Header{"Cache-Control": {"public, max-age=3600"}})
	get(t, fc2, "/", nil, next2)
	if w := get(t, fc2, "/", http.Header{"If-Match": {"*"}}, next2); w.Code != http.StatusOK {
		t.Fatalf("If-Match * on etag-less entry: got %d, want 200", w.Code)
	}
}

// TestIfNoneMatchStarNoMerge: a client's If-None-Match: * only proves a
// representation exists upstream — a 304 for it must not merge freshness
// into our stored entry.
func TestIfNoneMatchStarNoMerge(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleIfError = "10s"
	})

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)

			return nil
		}
		w.Header().Set("Cache-Control", "public") // freshness = configured TTL (50ms)
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("body"))

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(80 * time.Millisecond)

	// Past expiry: client's * is forwarded upstream, 304 comes back and is
	// forwarded — but must not be merged into the entry.
	w := get(t, fc, "/", http.Header{"If-None-Match": {"*"}}, next)
	if w.Code != http.StatusNotModified {
		t.Fatalf("expected client's 304 forwarded, got %d", w.Code)
	}

	_, stale, ok := fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
	if !ok || !stale {
		t.Fatal("*-only 304 must not refresh the stored entry")
	}
}

// TestRefresh304Deleted: a 304 merge landing after the entry was purged
// must not resurrect the meta sidecar.
func TestRefresh304Deleted(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleWhileRevalidate = "5s"
	})

	var calls atomic.Int64
	release := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public") // freshness = configured TTL (50ms)
		w.Header().Set("ETag", `"v1"`)
		if n == 1 {
			_, _ = w.Write([]byte("body"))

			return nil
		}

		<-release
		w.WriteHeader(http.StatusNotModified)

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(80 * time.Millisecond)

	get(t, fc, "/", nil, next) // stale serve; refresh blocks on release
	waitFor(t, "refresh to reach upstream", func() bool { return calls.Load() == 2 })

	// Purge while the revalidation is in flight, then let the 304 land.
	fc.idx.purge(fc.idx.baseKey(httptest.NewRequest(http.MethodGet, "http://example.com/", nil)), "purge")
	close(release)

	waitFor(t, "refresh goroutine to finish", func() bool {
		done := true
		fc.flights.Range(func(_, _ any) bool { done = false; return false })

		return done
	})

	if files := cacheFiles(t, fc.Path); files != 0 {
		t.Errorf("purge raced 304 merge left %d files behind", files)
	}
}

// TestProxyAuthorizationBypass: proxy credentials mark the request as
// connection-specific — its response must not be stored or served.
func TestProxyAuthorizationBypass(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	h := http.Header{"Proxy-Authorization": {"Basic Zm9v"}}
	get(t, fc, "/", h, next)
	w := get(t, fc, "/", h, next)

	if strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
		t.Fatal("response to Proxy-Authorization request was served from cache")
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 upstream calls, got %d", calls.Load())
	}
}

// TestPragmaMultiToken: Pragma: no-cache is honored as a bypass even
// inside a multi-token value when honor_request_cache_control is set.
func TestPragmaMultiToken(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.HonorRequestCC = true })

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	h := http.Header{"Pragma": {"foo=1, no-cache"}}
	get(t, fc, "/", h, next)
	get(t, fc, "/", h, next)

	if calls.Load() != 2 {
		t.Fatalf("multi-token Pragma: no-cache did not bypass, got %d calls", calls.Load())
	}
}

// TestMatchHeaderRequiresValues: match_header with no values is a config
// error, not a silent "cache nothing" rule.
func TestMatchHeaderRequiresValues(t *testing.T) {
	d := caddyfile.NewTestDispenser("fastcache {\n\tmatch_header X-Only\n}")
	var fc FastCache
	if err := fc.UnmarshalCaddyfile(d); err == nil {
		t.Fatal("match_header with no values should be an error")
	}
}

// TestPurgeLocationInvalidation: a non-error unsafe-method response
// invalidates not just the request URI but also its Location and
// Content-Location targets (RFC 9111 §4.4).
func TestPurgeLocationInvalidation(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/created")
			w.Header().Set("Content-Location", "/item/1")
			w.WriteHeader(http.StatusCreated)

			return nil
		}
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("x"))

		return nil
	})

	get(t, fc, "/created", nil, next)
	get(t, fc, "/item/1", nil, next)
	get(t, fc, "/u", nil, next)
	if calls.Load() != 3 {
		t.Fatalf("setup stores failed: %d calls", calls.Load())
	}

	r := httptest.NewRequest(http.MethodPost, "http://example.com/u", nil)
	if err := fc.ServeHTTP(httptest.NewRecorder(), r, next); err != nil {
		t.Fatal(err)
	}

	// The request URI plus both pointed-at URIs must re-fetch.
	for _, p := range []string{"/u", "/created", "/item/1"} {
		if w := get(t, fc, p, nil, next); strings.HasPrefix(w.Header().Get("Cache-Status"), "fastcache; hit") {
			t.Errorf("%s still served after invalidation", p)
		}
	}

	if calls.Load() != 7 {
		t.Fatalf("expected 7 upstream calls, got %d", calls.Load())
	}
}

// TestReadMetaOversize: a meta sidecar beyond the gob length cap is
// rejected by stat size alone — never read into memory — and reaped.
func TestReadMetaOversize(t *testing.T) {
	dir := t.TempDir()
	shard := filepath.Join(dir, "ab")
	if err := os.MkdirAll(shard, 0o755); err != nil {
		t.Fatal(err)
	}

	big := filepath.Join(shard, "aabbccddeeff001122334455-1.meta")
	if err := os.WriteFile(big, bytes.Repeat([]byte("x"), 2<<20), 0o640); err != nil {
		t.Fatal(err)
	}

	ix := newIndex(&FastCache{Path: dir})
	if err := ix.rebuild(); err != nil {
		t.Fatal(err)
	}
	defer ix.stop()

	if ix.fileCount.Load() != 0 {
		t.Fatalf("oversized meta was indexed: %d entries", ix.fileCount.Load())
	}
	if _, err := os.Stat(big); !os.IsNotExist(err) {
		t.Error("oversized meta was not reaped")
	}
}

// TestRevalidateTimeoutOption parses the new knob.
func TestRevalidateTimeoutOption(t *testing.T) {
	d := caddyfile.NewTestDispenser("fastcache {\n\trevalidate_timeout 45s\n}")
	var fc FastCache
	if err := fc.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	if fc.RevalidateTimeout != "45s" {
		t.Fatalf("revalidate_timeout = %q", fc.RevalidateTimeout)
	}
}

// TestStatusRW: the unsafe-method wrapper records the first status only and
// forwards the full ResponseWriter interface (Flush, ReadFrom, Hijack,
// Unwrap) so bypassed upstream handlers behave normally.
func TestStatusRW(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusRW{ResponseWriter: rec}

	sw.WriteHeader(http.StatusCreated)
	sw.WriteHeader(http.StatusTeapot) // second call: status must not flip
	if sw.status != http.StatusCreated {
		t.Fatalf("status flipped to %d", sw.status)
	}

	// Write without an explicit WriteHeader commits 200.
	sw2 := &statusRW{ResponseWriter: httptest.NewRecorder()}
	_, _ = sw2.Write([]byte("x"))
	if sw2.status != http.StatusOK || !sw2.wrote {
		t.Fatal("Write did not commit an implicit 200")
	}

	// Flush commits 200 and reaches the wrapped flusher.
	sw3 := &statusRW{ResponseWriter: httptest.NewRecorder()}
	sw3.Flush()
	if sw3.status != http.StatusOK {
		t.Fatal("Flush did not commit 200")
	}

	// ReadFrom: io.Copy picks it up (a LimitReader hides WriterTo so the
	// destination's ReadFrom is what runs); it must delegate to a wrapped
	// ReaderFrom (sendfile) when there is one.
	sw4 := &statusRW{ResponseWriter: httptest.NewRecorder()}
	n, err := io.Copy(sw4, io.LimitReader(strings.NewReader("body"), 4))
	if err != nil || n != 4 {
		t.Fatalf("ReadFrom fallback: %d %v", n, err)
	}

	rf := &readerFromRW{ResponseWriter: httptest.NewRecorder()}
	sw5 := &statusRW{ResponseWriter: rf}
	_, _ = io.Copy(sw5, io.LimitReader(strings.NewReader("body"), 4))
	if !rf.used {
		t.Fatal("ReadFrom did not reach the wrapped ReaderFrom")
	}

	// Hijack and Unwrap.
	if _, _, err := sw.Hijack(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}

	server, _ := net.Pipe()
	defer func() { _ = server.Close() }()
	sw6 := &statusRW{ResponseWriter: &hijackRW{ResponseWriter: httptest.NewRecorder(), c: server}}
	if _, _, err := sw6.Hijack(); err != nil {
		t.Fatalf("hijack passthrough: %v", err)
	}

	if sw.Unwrap() != rec {
		t.Fatal("Unwrap did not return the embedded writer")
	}
}

// readerFromRW implements io.ReaderFrom so statusRW.ReadFrom delegation can
// be observed.
type readerFromRW struct {
	http.ResponseWriter
	used bool
}

func (w *readerFromRW) ReadFrom(src io.Reader) (int64, error) {
	w.used = true
	return io.Copy(w.ResponseWriter, src)
}

// TestDiscardRW: the refresh-time discard writer swallows everything —
// its Header map is the only reachable surface.
func TestDiscardRW(t *testing.T) {
	d := &discardRW{h: http.Header{}}
	d.Header().Set("X", "y")
	d.WriteHeader(http.StatusTeapot)

	if n, err := d.Write([]byte("abc")); err != nil || n != 3 {
		t.Fatalf("discard Write: %d %v", n, err)
	}
	if d.Header().Get("X") != "y" {
		t.Fatal("discard Header lost a value")
	}
}

// TestParseFastCache: the httpcaddyfile adapter wraps the same Unmarshal.
func TestParseFastCache(t *testing.T) {
	d := caddyfile.NewTestDispenser("fastcache {\n\tmemory_size 8MB\n}")

	h, err := parseFastCache(httpcaddyfile.Helper{Dispenser: d})
	if err != nil {
		t.Fatal(err)
	}

	if fc, ok := h.(*FastCache); !ok || fc.MemorySize != "8MB" {
		t.Fatalf("parseFastCache returned %+v", h)
	}

	// Adapter errors surface, not panic.
	bad := caddyfile.NewTestDispenser("fastcache {\n\tbogus_opt x\n}")
	if _, err := parseFastCache(httpcaddyfile.Helper{Dispenser: bad}); err == nil {
		t.Fatal("expected parse error for bad option")
	}
}

// TestAdminRoutes: the admin module exposes the keys and purge routes.
func TestAdminRoutes(t *testing.T) {
	routes := adminAPI{}.Routes()
	if len(routes) != 2 || routes[0].Pattern != "/fastcache/keys" || routes[1].Pattern != "/fastcache/purge" {
		t.Fatalf("unexpected routes: %+v", routes)
	}
}

// TestAdminPurgeBodyLimit: the purge endpoint refuses bodies beyond the
// 1 MiB cap instead of buffering them into memory.
func TestAdminPurgeBodyLimit(t *testing.T) {
	body := strings.NewReader(`{"pattern":"` + strings.Repeat("a", 1<<20) + `"}`)
	w := httptest.NewRecorder()

	var apiErr caddy.APIError
	if err := purgeCacheKeys(w, httptest.NewRequest(http.MethodDelete, "/fastcache/purge", body)); !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized body, got %v", err)
	}
}

// TestAdminKeysSorted: the keys listing is deterministic.
func TestAdminKeysSorted(t *testing.T) {
	fc := newTestCache(t, nil)
	liveCaches.Store(fc, struct{}{})
	t.Cleanup(func() { liveCaches.Delete(fc) })

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/z", nil, next)
	get(t, fc, "/a", nil, next)
	get(t, fc, "/m", nil, next)

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

	if !slices.IsSorted(listing.Keys) || len(listing.Keys) != 3 {
		t.Fatalf("keys not sorted: %v", listing.Keys)
	}
}

// TestVarySchemeChange: when upstream starts answering with a different
// whitelisted Vary list, the old variant set is unreachable — commit drops
// the whole mapping and rebuilds it under the new scheme.
func TestVarySchemeChange(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) { fc.Vary = []string{"X-A", "X-B"} })

	var calls atomic.Int64
	varyBy := "X-A"
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)
		w.Header().Set("Vary", varyBy)
		_, _ = w.Write([]byte("x"))

		return nil
	})

	h1 := http.Header{"X-A": {"1"}, "X-B": {"1"}}
	h2 := http.Header{"X-A": {"2"}, "X-B": {"2"}}

	get(t, fc, "/", h1, next) // scheme [X-A]
	varyBy = "X-B"
	get(t, fc, "/", h2, next) // scheme change → mapping dropped, rebuilt [X-B]

	if got := fc.idx.fileCount.Load(); got != 1 {
		t.Fatalf("old variant should be dropped, fileCount=%d", got)
	}

	// A request under the old scheme now misses — it computes a different
	// varyKey under [X-B].
	get(t, fc, "/", h1, next)
	if calls.Load() != 3 {
		t.Fatalf("expected 3 upstream calls, got %d", calls.Load())
	}
}

// TestReadMetaErrors: every malformed sidecar shape is rejected before it
// can reach the gob decoder.
func TestReadMetaErrors(t *testing.T) {
	dir := t.TempDir()
	put := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o640); err != nil {
			t.Fatal(err)
		}

		return p
	}

	magic := make([]byte, 4)
	copy(magic, fileMagic)

	mk := func(metaLen uint32, payload []byte) []byte {
		b := append(append([]byte{}, magic...), byte(metaLen>>24), byte(metaLen>>16), byte(metaLen>>8), byte(metaLen))
		return append(b, payload...)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{"missing", filepath.Join(dir, "nope")},
		{"tiny", put("tiny", []byte("x"))},
		{"bad magic", put("badmagic", append([]byte("ZZZZ"), 0, 0, 0, 1, 0))},
		{"declared over cap", put("cap", mk(2<<20, nil))},
		{"short payload", put("short", mk(64, []byte("ab")))},
		{"empty gob", put("empty", mk(0, nil))},
		{"bad gob", put("badgob", mk(10, []byte("not-a-gob!")))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := readMeta(tc.path); err == nil {
				t.Fatal("expected readMeta error")
			}
		})
	}

	// Roundtrip: writeMeta's output parses back to the same record.
	var buf bytes.Buffer
	want := &metaHeader{Key: "example.com/x", Status: 200, StoredAt: 7, Headers: map[string][]string{"A": {"1"}}}
	if _, err := writeMeta(&buf, want); err != nil {
		t.Fatal(err)
	}

	p := put("ok", buf.Bytes())
	got, _, err := readMeta(p)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != want.Key || got.Status != want.Status || got.Headers["A"][0] != "1" {
		t.Fatalf("meta roundtrip mismatch: %+v", got)
	}
}

// TestPersistMetaSkips: persistMeta bails out on RAM-only entries,
// meta-less entries, and entries that left the index — never resurrecting
// a sidecar for deleted content.
func TestPersistMetaSkips(t *testing.T) {
	dir := t.TempDir()

	ram := &FastCache{Path: dir, MemorySize: "64MB", DiskSize: "0", TTL: "1h"}
	ram.memoryBytes, _ = parseSize(ram.MemorySize, 0)
	ram.diskBytes, _ = parseSize(ram.DiskSize, 0)
	ramIx := newIndex(ram)
	ramIx.persistMeta(&entry{key: "k", metaPath: filepath.Join(dir, "ab", "gone.meta")})

	disk := &FastCache{Path: dir, MemorySize: "64MB", DiskSize: "1GB", TTL: "1h"}
	disk.memoryBytes, _ = parseSize(disk.MemorySize, 0)
	disk.diskBytes, _ = parseSize(disk.DiskSize, 0)
	ix := newIndex(disk)

	// No metaPath — nothing to write.
	ix.persistMeta(&entry{key: "k"})

	// Not in the index — the write would resurrect a dead entry.
	ghost := &entry{key: "k", metaPath: filepath.Join(dir, "ab", "ghost.meta")}
	ix.persistMeta(ghost)
	if _, err := os.Stat(ghost.metaPath); !os.IsNotExist(err) {
		t.Fatal("persistMeta wrote a meta for an unindexed entry")
	}

	// Live entry — the meta lands.
	shard := filepath.Join(dir, "ab")
	if err := os.MkdirAll(shard, 0o750); err != nil {
		t.Fatal(err)
	}
	live := &entry{key: "k", baseKey: "k", metaPath: filepath.Join(shard, "live.meta"), status: 200}
	hdr := http.Header{"A": {"1"}}
	live.headers.Store(&hdr)
	live.storedAt.Store(time.Now().UnixNano())
	live.expiresAt.Store(time.Now().Add(time.Hour).UnixNano())
	ix.entries.Store("k", live)
	ix.persistMeta(live)

	if _, err := os.Stat(live.metaPath); err != nil {
		t.Fatalf("persistMeta did not write live meta: %v", err)
	}
}

// TestPurgeDuringCapture: a PURGE landing while the upstream response is
// still streaming must abort the commit — purged content can never be
// resurrected by a racing store.
func TestPurgeDuringCapture(t *testing.T) {
	fc := newTestCache(t, nil)

	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})

	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		n := calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			close(started)
			<-release
		}
		_, _ = w.Write([]byte("x"))

		return nil
	})

	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = fc.ServeHTTP(httptest.NewRecorder(), r, next)
	}()

	<-started
	fc.idx.purge(fc.idx.baseKey(r), "purge")
	close(release)
	<-done

	if _, _, ok := fc.idx.lookup(r); ok {
		t.Fatal("capture committed over a purge")
	}

	get(t, fc, "/", nil, next)
	if calls.Load() != 2 {
		t.Fatalf("expected a fresh upstream call after aborted commit, got %d", calls.Load())
	}
}

// TestLastModified: lastModified parses the stored field or yields the
// zero time (which ServeContent treats as "no timestamp").
func TestLastModified(t *testing.T) {
	e := &entry{}

	hdr := http.Header{}
	e.headers.Store(&hdr)
	if got := lastModified(e); !got.IsZero() {
		t.Fatalf("no Last-Modified: got %v", got)
	}

	hdr["Last-Modified"] = []string{"not a date"}
	if got := lastModified(e); !got.IsZero() {
		t.Fatalf("bad Last-Modified: got %v", got)
	}

	hdr["Last-Modified"] = []string{"Mon, 02 Jan 2006 15:04:05 GMT"}
	if got := lastModified(e); got.IsZero() || got.Year() != 2006 {
		t.Fatalf("valid Last-Modified: got %v", got)
	}
}

// TestMetricsRegisterReuse: a second cacheMetrics into one registry reuses
// the first's collectors — counters aggregate across instances.
func TestMetricsRegisterReuse(t *testing.T) {
	reg := prometheus.NewRegistry()

	m1 := newCacheMetrics("")
	m1.register(reg)

	m2 := newCacheMetrics("")
	m2.register(reg)

	if m1.requests != m2.requests || m1.evictions != m2.evictions {
		t.Fatal("second instance did not reuse the registered collectors")
	}

	m1.req("hit")
	m2.req("hit")

	got, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	var found float64
	for _, mf := range got {
		if mf.GetName() != "fastcache_requests_total" {
			continue
		}
		for _, c := range mf.GetMetric() {
			for _, l := range c.GetLabel() {
				if l.GetName() == "result" && l.GetValue() == "hit" {
					found = c.GetCounter().GetValue()
				}
			}
		}
	}

	if found != 2 {
		t.Fatalf("counters should aggregate across instances, got %v", found)
	}

	// A named instance reports its own series, separate from the unnamed one.
	m3 := newCacheMetrics("ram")
	m3.register(reg)
	m3.req("hit")

	got, err = reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	series := map[string]float64{}
	for _, mf := range got {
		if mf.GetName() != "fastcache_requests_total" {
			continue
		}
		for _, c := range mf.GetMetric() {
			var name, result string
			for _, l := range c.GetLabel() {
				switch l.GetName() {
				case "name":
					name = l.GetValue()
				case "result":
					result = l.GetValue()
				}
			}
			if result == "hit" {
				series[name] = c.GetCounter().GetValue()
			}
		}
	}
	if series[""] != 2 || series["ram"] != 1 {
		t.Fatalf("name label should split series, got %v", series)
	}

	// sumIndexes sees live instances and skips ones with a nil index.
	fc := newTestCache(t, nil)
	liveCaches.Store(fc, struct{}{})
	liveCaches.Store(&FastCache{}, struct{}{}) // never provisioned
	defer liveCaches.Delete(fc)

	if total := sumIndexes("", func(ix *index) int64 { return ix.fileCount.Load() }); total != 0 {
		t.Fatalf("sumIndexes = %d, want 0", total)
	}
}

// TestFreshnessTable: every freshness source, in precedence order.
func TestFreshnessTable(t *testing.T) {
	fc := &FastCache{Path: t.TempDir()}
	fc.ttl = time.Hour
	ix := newIndex(fc)

	past := time.Now().Add(-100 * time.Second).UTC().Format(http.TimeFormat)
	future := time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)

	for _, tc := range []struct {
		name string
		h    http.Header
		want time.Duration
	}{
		{"default", http.Header{}, time.Hour},
		{"max-age", http.Header{"Cache-Control": {"public, max-age=60"}}, 60 * time.Second},
		{"s-maxage wins", http.Header{"Cache-Control": {"public, s-maxage=30, max-age=60"}}, 30 * time.Second},
		{"no-cache", http.Header{"Cache-Control": {"public, no-cache"}}, 0},
		{"max-age zero", http.Header{"Cache-Control": {"public, max-age=0"}}, 0},
		{"expires future", http.Header{"Expires": {future}}, 10 * time.Second},
		{"expires past", http.Header{"Expires": {past}}, 0},
		{"expires garbage", http.Header{"Expires": {"junk"}}, time.Hour},
		{"last-modified heuristic", http.Header{"Last-Modified": {past}}, -1},
		{"last-modified future", http.Header{"Last-Modified": {future}}, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := freshness(ix, tc.h)
			if tc.want < 0 {
				// ~age/10 with real elapsed time — assert the neighborhood.
				if got < 9*time.Second || got > 11*time.Second {
					t.Fatalf("freshness = %v, want ~10s", got)
				}
				return
			}
			if tc.name == "expires future" {
				if got <= 0 || got > 10*time.Second {
					t.Fatalf("freshness = %v", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("freshness = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUnmarshalCaddyfileMissingArgs: single-value options must reject a
// bare keyword instead of silently keeping defaults.
func TestUnmarshalCaddyfileMissingArgs(t *testing.T) {
	for _, opt := range []string{
		"path", "memory_size", "disk_size", "max_items", "ttl",
		"stale_while_revalidate", "stale_if_error", "cache_key",
		"promote_below", "revalidate_timeout",
	} {
		t.Run(opt, func(t *testing.T) {
			var fc FastCache
			err := fc.UnmarshalCaddyfile(caddyfile.NewTestDispenser("fastcache {\n\t" + opt + "\n}"))
			if err == nil {
				t.Fatalf("%s with no argument should be an error", opt)
			}
		})
	}
}

// statusUpstream serves a fixed status/body, counting calls.
func statusUpstream(counter *atomic.Int64, status int, body string, headers http.Header) caddyhttp.Handler {
	return handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		counter.Add(1)
		for k, vv := range headers {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))

		return nil
	})
}

// hitTTL extracts the ttl= parameter of a hit's Cache-Status header.
func hitTTL(t *testing.T, w *httptest.ResponseRecorder) int64 {
	t.Helper()

	cs := w.Header().Get("Cache-Status")
	for p := range strings.SplitSeq(cs, ";") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(p), "ttl="); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatalf("bad ttl param in Cache-Status %q", cs)
			}

			return n
		}
	}

	t.Fatalf("no ttl param in Cache-Status %q", cs)

	return 0
}

// TestStatusTTL: the per-status cap bounds computed freshness — the
// default ttl AND explicit upstream max-age — while leaving unlisted
// statuses alone.
func TestStatusTTL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		statusTTL map[int]string
		status    int
		headers   http.Header
		minTTL    int64
		maxTTL    int64 // hit ttl must land inside [minTTL, maxTTL]
	}{
		{"caps default ttl", map[int]string{404: "30s"}, http.StatusNotFound, nil, 0, 30},
		{"caps upstream max-age", map[int]string{200: "30s"}, http.StatusOK,
			http.Header{"Cache-Control": {"public, max-age=3600"}}, 0, 30},
		{"smaller upstream max-age wins", map[int]string{200: "30s"}, http.StatusOK,
			http.Header{"Cache-Control": {"public, max-age=5"}}, 0, 5},
		{"unlisted status unaffected", map[int]string{404: "30s"}, http.StatusOK,
			http.Header{"Cache-Control": {"public, max-age=3600"}}, 3500, 3600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := newTestCache(t, func(fc *FastCache) { fc.StatusTTL = tc.statusTTL })

			var calls atomic.Int64
			next := statusUpstream(&calls, tc.status, "x", tc.headers)

			get(t, fc, "/", nil, next)
			w := get(t, fc, "/", nil, next)

			if calls.Load() != 1 {
				t.Fatalf("expected a stored hit, got %d calls", calls.Load())
			}
			if w.Code != tc.status {
				t.Fatalf("expected status %d, got %d", tc.status, w.Code)
			}
			if ttl := hitTTL(t, w); ttl < tc.minTTL || ttl > tc.maxTTL {
				t.Errorf("hit ttl = %ds, want [%d, %d]", ttl, tc.minTTL, tc.maxTTL)
			}
		})
	}
}

// TestStatusTTLZero: a zero cap makes the status unstorable — every
// request re-fetches, matching nginx's "not listed in proxy_cache_valid =
// never cached".
func TestStatusTTLZero(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StatusTTL = map[int]string{404: "0"}
	})

	var calls atomic.Int64
	next := statusUpstream(&calls, http.StatusNotFound, "missing", nil)

	for i := range 2 {
		if w := get(t, fc, "/", nil, next); w.Code != http.StatusNotFound {
			t.Fatalf("request %d: got %d", i, w.Code)
		}
	}

	if calls.Load() != 2 {
		t.Errorf("status_ttl 0 must never store, got %d calls", calls.Load())
	}
}

// TestStatusTTLNotAWhitelist: status_ttl tunes freshness only — a status
// outside the heuristically cacheable set is still never stored.
func TestStatusTTLNotAWhitelist(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StatusTTL = map[int]string{302: "30s"}
	})

	var calls atomic.Int64
	next := statusUpstream(&calls, http.StatusFound, "x", nil)

	get(t, fc, "/", nil, next)
	get(t, fc, "/", nil, next)

	if calls.Load() != 2 {
		t.Errorf("302 must not be stored even with a status_ttl entry, got %d calls", calls.Load())
	}
}

// TestStatusTTLExpiry: a capped entry expires at the cap, not at ttl.
func TestStatusTTLExpiry(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StatusTTL = map[int]string{404: "50ms"} // default ttl is 1h
	})

	var calls atomic.Int64
	next := statusUpstream(&calls, http.StatusNotFound, "missing", nil)

	get(t, fc, "/", nil, next)
	get(t, fc, "/", nil, next)

	if calls.Load() != 1 {
		t.Fatalf("expected a stored hit inside the cap, got %d calls", calls.Load())
	}

	time.Sleep(80 * time.Millisecond)

	get(t, fc, "/", nil, next)

	if calls.Load() != 2 {
		t.Errorf("expected re-fetch after status_ttl expiry, got %d calls", calls.Load())
	}
}

// TestStatusTTLStaleWindow: the cap shortens freshness, not retention — a
// short-capped entry is still a stale_if_error fallback until staleAt.
func TestStatusTTLStaleWindow(t *testing.T) {
	fc := newTestCache(t, func(fc *FastCache) {
		fc.StatusTTL = map[int]string{404: "50ms"}
		fc.StaleIfError = "5s"
	})

	var calls atomic.Int64
	fail := false
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		calls.Add(1)

		if fail {
			w.WriteHeader(http.StatusBadGateway)

			return nil
		}

		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing"))

		return nil
	})

	get(t, fc, "/", nil, next) // stores 404: fresh 50ms, staleAt = +5.05s
	time.Sleep(80 * time.Millisecond)

	// Past the capped freshness, inside the SIE window: the 5xx answer
	// falls back to the stale 404.
	fail = true
	w := get(t, fc, "/", nil, next)
	if w.Code != http.StatusNotFound || w.Body.String() != "missing" {
		t.Fatalf("expected stale 404 fallback, got %d %q", w.Code, w.Body.String())
	}
}

// TestParseStatusTTLs: duration validation — unparseable and negative
// durations are rejected, zero is allowed (never store).
func TestParseStatusTTLs(t *testing.T) {
	if m, err := parseStatusTTLs(nil); m != nil || err != nil {
		t.Fatalf("nil map: got %v, %v", m, err)
	}

	for _, s := range []string{"bogus", "-5s"} {
		if _, err := parseStatusTTLs(map[int]string{404: s}); err == nil {
			t.Errorf("status_ttl duration %q should be an error", s)
		}
	}

	m, err := parseStatusTTLs(map[int]string{404: "30s", 500: "0"})
	if err != nil || m[404] != 30*time.Second || m[500] != 0 {
		t.Fatalf("parseStatusTTLs = %v, %v", m, err)
	}
}

// TestStatusTTLCaddyfileErrors: status_ttl needs at least one status code
// plus a duration, and every code must be a valid HTTP status.
func TestStatusTTLCaddyfileErrors(t *testing.T) {
	for _, line := range []string{
		"status_ttl",
		"status_ttl 404",
		"status_ttl x 30s",
		"status_ttl 99 30s",
		"status_ttl 600 30s",
	} {
		t.Run(line, func(t *testing.T) {
			var fc FastCache
			if err := fc.UnmarshalCaddyfile(caddyfile.NewTestDispenser("fastcache {\n\t" + line + "\n}")); err == nil {
				t.Errorf("%q should be an error", line)
			}
		})
	}
}

// obsTestCache builds a test cache whose logger records every level into
// an observer, so tests can assert the warnings/errors an operator sees.
func obsTestCache(t *testing.T, mutate func(*FastCache)) (*FastCache, *observer.ObservedLogs) {
	t.Helper()
	core, observed := observer.New(zapcore.DebugLevel)

	return newTestCache(t, func(fc *FastCache) {
		fc.logger = zap.New(core)
		if mutate != nil {
			mutate(fc)
		}
	}), observed
}

// entryPath returns the on-disk body path of the cached entry for target.
func entryPath(t *testing.T, fc *FastCache, target string) string {
	t.Helper()

	e, _, hit := fc.idx.lookup(httptest.NewRequest(http.MethodGet, "http://example.com"+target, nil))
	if !hit {
		t.Fatalf("no cached entry for %s", target)
	}

	return e.path
}

// TestStaleFallbackLogsWarn: upstream failures masked by a stale fallback
// must surface as warnings — the served response looks like a plain hit.
func TestStaleFallbackLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleIfError = "5s"
	})

	fail := 0 // 0 = ok, 1 = 5xx, 2 = handler error
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		switch fail {
		case 1:
			w.WriteHeader(http.StatusBadGateway)

			return nil
		case 2:
			return errors.New("upstream dead")
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("v1"))

		return nil
	})

	get(t, fc, "/", nil, next) // stores v1
	time.Sleep(80 * time.Millisecond)

	fail = 1
	if w := get(t, fc, "/", nil, next); w.Body.String() != "v1" {
		t.Fatalf("expected stale fallback on 5xx, got %q", w.Body.String())
	}
	if logs.FilterMessage("upstream error; serving stale entry").Len() != 1 {
		t.Fatalf("5xx masked by stale should warn, got %v", logs.All())
	}

	fail = 2
	if w := get(t, fc, "/", nil, next); w.Body.String() != "v1" {
		t.Fatalf("expected stale fallback on handler error, got %q", w.Body.String())
	}
	if logs.FilterMessage("revalidation failed; serving stale entry").Len() != 1 {
		t.Fatalf("handler error masked by stale should warn, got %v", logs.All())
	}
}

// TestBackgroundRevalidationLogsWarn: a failed detached refresh has no
// caller to report to — the warn is the only trace of the upstream error.
func TestBackgroundRevalidationLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleWhileRevalidate = "5s"
	})

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		if calls.Add(1) > 1 {
			return errors.New("upstream dead")
		}

		w.Header().Set("Cache-Control", "public")
		_, _ = w.Write([]byte("v1"))

		return nil
	})

	get(t, fc, "/", nil, next)
	time.Sleep(80 * time.Millisecond)

	// Inside the SWR window: stale served, background refresh errors out.
	if w := get(t, fc, "/", nil, next); w.Body.String() != "v1" {
		t.Fatalf("expected stale serve, got %q", w.Body.String())
	}
	waitFor(t, "background revalidation warn", func() bool {
		return logs.FilterMessage("background revalidation failed").Len() == 1
	})
}

// TestPurgeDeniedLogsWarn: a rejected PURGE is an audit event — silent
// 403s would hide cache-invalidation probes.
func TestPurgeDeniedLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, nil)

	r := httptest.NewRequest("PURGE", "http://example.com/", nil)
	r.RemoteAddr = "203.0.113.7:1234"
	w := httptest.NewRecorder()

	if err := fc.ServeHTTP(w, r, nil); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
	if logs.FilterMessage("PURGE denied").Len() != 1 {
		t.Fatalf("denied PURGE should warn, got %v", logs.All())
	}
}

// TestRebuildLogsDroppedFiles: rebuild reaping corrupt or tampered files
// without a trace hides cache corruption — malformed metas and missing
// bodies warn, and the summary reports everything it removed.
func TestRebuildLogsDroppedFiles(t *testing.T) {
	fc, logs := obsTestCache(t, nil)

	shard := filepath.Join(fc.Path, "ab")
	if err := os.MkdirAll(shard, 0o750); err != nil {
		t.Fatal(err)
	}

	putMeta := func(name string, m *metaHeader) {
		t.Helper()
		mf, err := os.OpenFile(filepath.Join(shard, name+".meta"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writeMeta(mf, m); err != nil {
			t.Fatal(err)
		}
		if err := mf.Close(); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now().UnixNano()

	// Garbage .meta → unreadable.
	if err := os.WriteFile(filepath.Join(shard, "00112233445566778899aabb-1.meta"), []byte("junk"), 0o640); err != nil {
		t.Fatal(err)
	}

	// Well-formed meta whose body is gone → tampered entry.
	putMeta("112233445566778899aabbcc-2", &metaHeader{
		Key: "example.com/gone", Status: 200,
		StoredAt: now, ExpiresAt: now + int64(time.Hour), StaleAt: now + int64(time.Hour),
	})

	// Expired pair → counted, not warned.
	body := filepath.Join(shard, "2233445566778899aabbccdd-3")
	if err := os.WriteFile(body, []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	putMeta("2233445566778899aabbccdd-3", &metaHeader{
		Key: "example.com/old", Status: 200,
		StoredAt: now - int64(2*time.Hour), ExpiresAt: now - int64(time.Hour), StaleAt: now - 1,
	})

	// Orphan in-flight body → counted.
	if err := os.WriteFile(filepath.Join(shard, "33445566778899aabbccddee-4.tmp"), []byte("partial"), 0o640); err != nil {
		t.Fatal(err)
	}

	// Unreadable shard dir → subtree skipped, warned.
	bad := filepath.Join(fc.Path, "cd")
	if err := os.MkdirAll(bad, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(bad, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(bad, 0o750) })

	if err := fc.idx.rebuild(); err != nil {
		t.Fatal(err)
	}

	if logs.FilterMessage("dropping unreadable cache meta file").Len() != 1 {
		t.Errorf("unreadable meta should warn, got %v", logs.All())
	}
	if logs.FilterMessage("cache body file missing; dropping meta sidecar").Len() != 1 {
		t.Errorf("missing body should warn, got %v", logs.All())
	}
	if logs.FilterMessage("error scanning cache directory; subtree skipped").Len() != 1 {
		t.Errorf("unreadable shard dir should warn, got %v", logs.All())
	}

	// The rebuild summary reports the reaped counts (this is the second
	// rebuild — the first ran inside newTestCache).
	infos := logs.FilterMessage("cache index rebuilt").All()
	last := infos[len(infos)-1].ContextMap()
	if last["expired"] != int64(1) || last["invalid"] != int64(2) || last["orphans"] != int64(1) {
		t.Fatalf("rebuild summary counts wrong: %v", last)
	}
}

// TestBodyFileErrorsLog: a vanished body is the benign eviction race
// (debug); an unreadable one is an anomaly (warn). Either way the entry
// is dropped and refetched.
func TestBodyFileErrorsLog(t *testing.T) {
	fc, logs := obsTestCache(t, nil)

	var calls atomic.Int64
	next := countingUpstream(&calls, make([]byte, 32<<10), http.Header{"Cache-Control": {"public, max-age=3600"}})

	get(t, fc, "/", nil, next)

	bodyPath := entryPath(t, fc, "/")
	if err := os.Chmod(bodyPath, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(bodyPath, 0o640) })

	get(t, fc, "/", nil, next) // EACCES → warn + refetch
	if logs.FilterMessage("cannot open cache body file; refetching upstream").Len() != 1 {
		t.Fatalf("unreadable body should warn, got %v", logs.All())
	}
	if calls.Load() != 2 {
		t.Fatalf("expected refetch, got %d calls", calls.Load())
	}

	// A file that vanished entirely is the benign eviction race — debug.
	get(t, fc, "/two", nil, next)
	if err := os.Remove(entryPath(t, fc, "/two")); err != nil {
		t.Fatal(err)
	}
	get(t, fc, "/two", nil, next)
	if logs.FilterMessage("cannot open cache body file; refetching upstream").Len() != 1 {
		t.Fatalf("missing body should stay debug, got %v", logs.All())
	}
	if calls.Load() != 4 {
		t.Fatalf("expected 4 upstream calls, got %d", calls.Load())
	}
}

// TestRemoveFileLogsWarn: removal failures are reported — a file that
// cannot be deleted leaks space until the next rebuild. "Already gone" is
// not an error.
func TestRemoveFileLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, nil)

	nonempty := t.TempDir()
	if err := os.WriteFile(filepath.Join(nonempty, "f"), []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	fc.idx.removeFile(nonempty) // ENOTEMPTY
	if logs.FilterMessage("cannot remove cache file").Len() != 1 {
		t.Fatalf("failed remove should warn, got %v", logs.All())
	}

	fc.idx.removeFile(filepath.Join(nonempty, "does-not-exist"))
	if logs.FilterMessage("cannot remove cache file").Len() != 1 {
		t.Fatalf("missing file should not warn, got %v", logs.All())
	}
}

// TestStoreDirUnwritableLogsWarn: an uncreatable shard dir degrades to a
// passthrough — the response still serves, the warn explains why nothing
// is stored.
func TestStoreDirUnwritableLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, nil)

	if err := os.Chmod(fc.Path, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(fc.Path, 0o750) })

	var calls atomic.Int64
	next := countingUpstream(&calls, []byte("x"), http.Header{"Cache-Control": {"public, max-age=3600"}})
	if w := get(t, fc, "/", nil, next); w.Code != http.StatusOK {
		t.Fatalf("response should still be served, got %d", w.Code)
	}
	if logs.FilterMessage("cannot create cache directory; response not stored").Len() != 1 {
		t.Fatalf("unwritable cache dir should warn, got %v", logs.All())
	}
}

// TestAbandonedFillLogsWarn: a detached fill that outlives
// revalidate_timeout is dropped — the warn records the discarded work.
func TestAbandonedFillLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, func(fc *FastCache) { fc.RevalidateTimeout = "60ms" })

	midFill := make(chan struct{})
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("partial"))
		close(midFill)
		<-r.Context().Done() // abandoned once revalidate_timeout fires

		return r.Context().Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(ctx)
	done := make(chan error, 1)
	go func() { done <- fc.ServeHTTP(httptest.NewRecorder(), r, next) }()

	<-midFill
	cancel()

	if err := <-done; err == nil {
		t.Fatal("expected the abandoned fill to fail")
	}
	waitFor(t, "abandoned fill warn", func() bool {
		return logs.FilterMessage("abandoning detached fill; revalidate_timeout exceeded").Len() == 1
	})
	assertNoFlights(t, fc)
}

// TestPersistMetaLogsWarn: a meta sidecar that cannot be rewritten after a
// 304 merge warns — the entry stays servable but the update is lost.
func TestPersistMetaLogsWarn(t *testing.T) {
	fc, logs := obsTestCache(t, func(fc *FastCache) {
		fc.TTL = "50ms"
		fc.StaleIfError = "5s"
	})

	var calls atomic.Int64
	next := handlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls.Add(1)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusNotModified)

			return nil
		}

		w.Header().Set("Cache-Control", "public")
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("v1"))

		return nil
	})

	get(t, fc, "/", nil, next) // stores with ETag, freshness = 50ms ttl

	// Read-only shard dir: the body still opens, the meta rewrite fails.
	shard := filepath.Dir(entryPath(t, fc, "/"))
	if err := os.Chmod(shard, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o750) })

	time.Sleep(80 * time.Millisecond)
	if w := get(t, fc, "/", nil, next); w.Body.String() != "v1" {
		t.Fatalf("304 merge should serve the stored body, got %q", w.Body.String())
	}
	if logs.FilterMessage("cannot write meta sidecar").Len() != 1 {
		t.Fatalf("failed meta rewrite should warn, got %v", logs.All())
	}
}
