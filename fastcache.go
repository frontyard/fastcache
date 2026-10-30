// Package fastcache implements a Caddy HTTP handler that caches responses in
// a two-tier store: an in-memory body tier under memory_size, and a disk
// capacity tier under disk_size. Hits are served either from RAM or via
// kernel sendfile, with no locks on the read path.
package fastcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(new(FastCache))
	httpcaddyfile.RegisterHandlerDirective("fastcache", parseFastCache)
}

// FastCache is the http.handlers.fastcache module.
type FastCache struct {
	Path                 string              `json:"path,omitempty"`
	MemorySize           string              `json:"memory_size,omitempty"`
	DiskSize             string              `json:"disk_size,omitempty"`
	MaxItems             int                 `json:"max_items,omitempty"`
	TTL                  string              `json:"ttl,omitempty"`
	StaleWhileRevalidate string              `json:"stale_while_revalidate,omitempty"`
	StaleIfError         string              `json:"stale_if_error,omitempty"`
	CacheKey             string              `json:"cache_key,omitempty"`
	MatchHeaders         map[string][]string `json:"match_headers,omitempty"`
	Vary                 []string            `json:"vary,omitempty"`
	DisableTouchOnHit    bool                `json:"disable_touch_on_hit,omitempty"`
	PurgeAllow           []string            `json:"purge_allow,omitempty"`
	HonorRequestCC       bool                `json:"honor_request_cache_control,omitempty"`
	PromoteBelow         string              `json:"promote_below,omitempty"`
	StoreSetCookie       bool                `json:"store_set_cookie,omitempty"`
	RevalidateTimeout    string              `json:"revalidate_timeout,omitempty"`

	memoryBytes       int64
	diskBytes         int64
	promoteBelow      int64
	ttl               time.Duration
	swr               time.Duration
	sie               time.Duration
	revalidateTimeout time.Duration
	purgeNets         []netip.Prefix

	logger  *zap.Logger
	idx     *index
	flights sync.Map // flight key -> chan struct{}
}

// CaddyModule implements caddy.Module.
func (*FastCache) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.fastcache",
		New: func() caddy.Module { return new(FastCache) },
	}
}

// Provision implements caddy.Provisioner.
func (fc *FastCache) Provision(ctx caddy.Context) error {
	fc.logger = ctx.Logger()

	if fc.Path == "" {
		fc.Path = "/var/cache/fastcache"
	}

	var err error

	if fc.memoryBytes, err = parseSize(fc.MemorySize, 64<<20); err != nil {
		return fmt.Errorf("memory_size: %w", err)
	}

	if fc.diskBytes, err = parseSize(fc.DiskSize, 1<<30); err != nil {
		return fmt.Errorf("disk_size: %w", err)
	}

	if fc.promoteBelow, err = parseSize(fc.PromoteBelow, defaultPromoteBelow); err != nil {
		return fmt.Errorf("promote_below: %w", err)
	}

	if fc.ttl, err = parseDuration(fc.TTL, time.Hour); err != nil {
		return fmt.Errorf("ttl: %w", err)
	}

	if fc.swr, err = parseDuration(fc.StaleWhileRevalidate, 0); err != nil {
		return fmt.Errorf("stale_while_revalidate: %w", err)
	}

	if fc.sie, err = parseDuration(fc.StaleIfError, 0); err != nil {
		return fmt.Errorf("stale_if_error: %w", err)
	}

	if fc.revalidateTimeout, err = parseDuration(fc.RevalidateTimeout, 30*time.Second); err != nil {
		return fmt.Errorf("revalidate_timeout: %w", err)
	}

	for name, v := range map[string]int64{
		"memory_size": fc.memoryBytes, "disk_size": fc.diskBytes, "promote_below": fc.promoteBelow,
	} {
		if v < 0 {
			return fmt.Errorf("%s must be >= 0, got %d", name, v)
		}
	}
	for name, v := range map[string]time.Duration{
		"ttl": fc.ttl, "stale_while_revalidate": fc.swr, "stale_if_error": fc.sie,
		"revalidate_timeout": fc.revalidateTimeout,
	} {
		if v < 0 {
			return fmt.Errorf("%s must be >= 0, got %s", name, v)
		}
	}
	if fc.MaxItems < 0 {
		return fmt.Errorf("max_items must be >= 0, got %d", fc.MaxItems)
	}

	if err = fc.compilePurgeAllow(); err != nil {
		return err
	}

	if err = os.MkdirAll(fc.Path, 0o750); err != nil {
		return err
	}

	fc.idx = newIndex(fc)

	if err = fc.idx.rebuild(); err != nil {
		return fmt.Errorf("rebuilding the index: %w", err)
	}

	// Instances sharing a directory index each other's files at rebuild
	// and purge/evict across one another — almost always a mistake.
	warned := false
	liveCaches.Range(func(k, _ any) bool {
		if other := k.(*FastCache); other != fc && filepath.Clean(other.Path) == filepath.Clean(fc.Path) {
			warned = true
			return false
		}

		return true
	})
	if warned {
		fc.logger.Warn("fastcache instances share a cache directory; they will index and evict each other's entries",
			zap.String("path", fc.Path))
	}

	fc.idx.start()
	liveCaches.Store(fc, struct{}{})

	if reg := ctx.GetMetricsRegistry(); reg != nil {
		fc.idx.metrics.register(reg)
	}

	return nil
}

// defaultPromoteBelow bounds which bodies are worth keeping in the RAM
// tier. sendfile from the page cache beats a heap memcpy once the body is
// large; below the crossover the open/sendfile syscall overhead dominates
// and a RAM copy wins. Measured on this box (GOGC=800, keepalive):
// RAM +5% at 4 KiB, +2% at 8 KiB; sendfile +3% at 16 KiB, +13% at 32 KiB,
// +25% at 64 KiB — crossover ≈ 12-16 KiB.
const defaultPromoteBelow = 16 << 10

func parseSize(v string, def int64) (int64, error) {
	if v == "" {
		return def, nil
	}

	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, nil
	}

	s, err := humanize.ParseBytes(v)
	if err != nil {
		return 0, err
	}

	return int64(s), nil
}

func parseDuration(v string, def time.Duration) (time.Duration, error) {
	if v == "" {
		return def, nil
	}

	return time.ParseDuration(v)
}

// ServeHTTP implements caddyhttp.MiddlewareHandler.
func (fc *FastCache) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if r.Method == "PURGE" {
		return fc.handlePurge(w, r)
	}

	// Unsafe methods bypass the cache and, per RFC 9111 §4.4, invalidate the
	// stored response for the request URI on a non-error upstream answer.
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
	default:
		w.Header().Set("Cache-Status", "fastcache; fwd=request; detail=method")
		if ce := fc.logger.Check(zap.DebugLevel, "request bypassed cache"); ce != nil {
			ce.Write(zap.String("key", fc.idx.baseKey(r)), zap.String("reason", "unsafe method"))
		}

		sw := &statusRW{ResponseWriter: w, status: http.StatusOK}
		err := next.ServeHTTP(sw, r)
		fc.idx.metrics.req("bypass")

		if err != nil || sw.status >= 400 {
			return err
		}

		fc.idx.purge(fc.idx.baseKey(r), "invalidate")

		// §4.4 also invalidates the URIs in the response's Location and
		// Content-Location fields — only computable under the default
		// host+requestURI key scheme.
		if fc.idx.keyTmpl == "" {
			for _, hdr := range []string{"Location", "Content-Location"} {
				if loc := sw.Header().Get(hdr); loc != "" {
					if ref, perr := r.URL.Parse(loc); perr == nil {
						uri := ref.RequestURI()
						if uri == "" {
							continue // opaque URI — no origin-form to key on
						}
						host := ref.Host
						if host == "" {
							host = r.Host
						}
						fc.idx.purge(host+uri, "invalidate")
						// Absolute-form requests (RequestURI carries the
						// whole URI) store under host+full URI — purge that
						// spelling too.
						if full := ref.String(); full != uri {
							fc.idx.purge(host+full, "invalidate")
						}
					}
				}
			}
		}

		return nil
	}

	if requestBypass(fc.idx, r) {
		w.Header().Set("Cache-Status", "fastcache; fwd=request; detail=bypass")
		fc.idx.metrics.req("bypass")
		if ce := fc.logger.Check(zap.DebugLevel, "request bypassed cache"); ce != nil {
			ce.Write(zap.String("key", fc.idx.baseKey(r)), zap.String("reason", "request headers"))
		}

		return next.ServeHTTP(w, r)
	}

	// Followers loop instead of fetching directly: when the leader's
	// outcome doesn't cover the request (uncacheable response, different
	// variant, failed revalidation), rejoining — or leading — the next
	// flight keeps concurrent upstream fetches bounded. Follower waits are
	// capped, then the request fetches itself.
	for tries := 0; ; tries++ {
		e, stale, hit := fc.idx.lookup(r)
		if hit {
			if !stale {
				fc.idx.metrics.req("hit")
				if ce := fc.logger.Check(zap.DebugLevel, "cache hit"); ce != nil {
					ce.Write(zap.String("key", e.key), zap.Int("status", e.status), zap.Int64("size", e.size))
				}

				cs := []string{"hit", ttlParam(e)}
				if tries > 0 {
					cs = append(cs, "collapsed")
				}

				return fc.serveEntry(w, r, e, next, cs...)
			}

			mr := e.mustRevalidate.Load()
			if !mr && fc.idx.withinSWR(e, time.Now().UnixNano()) {
				fc.refresh(e, r, next)
				fc.idx.metrics.req("stale")
				if ce := fc.logger.Check(zap.DebugLevel, "serving stale; revalidating in background"); ce != nil {
					ce.Write(zap.String("key", e.key))
				}

				return fc.serveEntry(w, r, e, next, "hit", "fwd=stale")
			}

			// Beyond the SWR window — or any stale entry forbidding stale
			// serving — revalidate synchronously, coalesced under the same
			// flight key refresh() uses, so at most one revalidation per
			// variant is ever in flight. Without must-revalidate the stale
			// entry is the error fallback (stale_if_error) and is served to
			// followers immediately.
			done, leader := fc.startFlight("reval:" + e.key)
			if leader {
				fc.idx.metrics.req("revalidated")
				if ce := fc.logger.Check(zap.DebugLevel, "revalidating stale entry"); ce != nil {
					ce.Write(zap.String("key", e.key), zap.Bool("must_revalidate", mr))
				}
				err := fc.revalidateOrStale(w, r, e, next)
				fc.finishFlight("reval:"+e.key, done)

				return err
			}

			if !mr {
				fc.idx.metrics.req("stale")
				if ce := fc.logger.Check(zap.DebugLevel, "serving stale; revalidation in flight"); ce != nil {
					ce.Write(zap.String("key", e.key))
				}

				return fc.serveEntry(w, r, e, next, "hit", "fwd=stale", "collapsed")
			}

			if ce := fc.logger.Check(zap.DebugLevel, "awaiting in-flight revalidation"); ce != nil {
				ce.Write(zap.String("key", e.key))
			}
			select {
			case <-done:
			case <-r.Context().Done():
				return r.Context().Err()
			}

			continue
		}

		// Miss coalescing: the leader fetches upstream and streams the
		// response to its client while storing it; followers wait, then
		// loop — re-serving the new entry or leading the next flight.
		base := fc.idx.baseKey(r)
		flightKey := base + "|" + varyKey(r, fc.varyHeadersOf(base))

		done, leader := fc.startFlight(flightKey)
		if leader {
			fc.idx.metrics.req("miss")
			if ce := fc.logger.Check(zap.DebugLevel, "cache miss"); ce != nil {
				ce.Write(zap.String("key", flightKey))
			}
			err := fc.fetchAndStore(w, r, base, next)
			fc.finishFlight(flightKey, done)

			return err
		}

		if tries >= maxFlightRounds {
			// Leaders keep finishing without covering this request (e.g.
			// uncacheable responses under churn): fetch directly rather
			// than queue on yet another flight.
			fc.idx.metrics.req("miss")
			if ce := fc.logger.Check(zap.DebugLevel, "flight wait limit reached; fetching upstream directly"); ce != nil {
				ce.Write(zap.String("key", base))
			}

			return fc.fetchAndStore(w, r, base, next)
		}

		if ce := fc.logger.Check(zap.DebugLevel, "coalescing with in-flight request"); ce != nil {
			ce.Write(zap.String("key", flightKey))
		}
		select {
		case <-done:
		case <-r.Context().Done():
			return r.Context().Err()
		}
	}
}

// maxFlightRounds caps how often a request may trail another request's
// flight before fetching upstream itself.
const maxFlightRounds = 3

// fetchAndStore proxies to next through a captureWriter; a handler error
// discards the capture.
func (fc *FastCache) fetchAndStore(w http.ResponseWriter, r *http.Request, base string, next caddyhttp.Handler) (err error) {
	cw := newCaptureWriter(fc, w, r, base)
	defer func() { cw.Close(err) }()

	return next.ServeHTTP(cw, r)
}

// revalidateOrStale re-fetches synchronously while holding a stale entry
// (past its SWR window, retained by stale_if_error, or stale with
// must-revalidate). Stored validators make the fetch conditional: a 304
// merges into the entry and serves it. If upstream fails — handler error
// before any bytes reached the client, or a 5xx status — the stale entry is
// served instead, unless it forbids stale serving; captureWriter intercepts
// the 5xx case inside decide() before the status hits the wire.
func (fc *FastCache) revalidateOrStale(w http.ResponseWriter, r *http.Request, e *entry, next caddyhttp.Handler) (err error) {
	r2 := revalRequest(r, e)

	cw := newCaptureWriter(fc, w, r2, e.baseKey)
	cw.fallback = e
	cw.reval = e
	cw.origReq = r
	cw.next = next
	cw.ownVal = canMerge304(r, e)
	defer func() { cw.Close(err) }()

	if err = next.ServeHTTP(cw, r2); err != nil && !cw.wrote && !cw.swallow && !e.mustRevalidate.Load() {
		// The handler failed before anything reached the client.
		if ce := fc.logger.Check(zap.DebugLevel, "revalidation failed; serving stale entry"); ce != nil {
			ce.Write(zap.String("key", e.key), zap.Error(err))
		}

		return fc.serveEntry(w, r, e, next, "hit", "fwd=stale")
	}

	return err
}

// canMerge304 reports whether a 304 proves the stored entry fresh: either
// the revalidation carried our stored validators (the client sent no
// If-None-Match, so revalRequest attached them), or the client's own
// If-None-Match already names the stored ETag. A 304 for other client
// validators only proves the client's copy fresh — it must not merge, and
// a bare "*" is no better: it proves a representation exists, not that
// ours is the current one.
func canMerge304(r *http.Request, e *entry) bool {
	inm := r.Header.Get("If-None-Match")
	if inm == "" {
		return true
	}

	etag := strings.TrimPrefix((*e.headers.Load()).Get("ETag"), "W/")
	if etag == "" {
		return false
	}

	for t := range strings.SplitSeq(inm, ",") {
		if strings.TrimPrefix(strings.TrimSpace(t), "W/") == etag {
			return true
		}
	}

	return false
}

// revalRequest clones r for upstream revalidation: client Range headers are
// dropped and the stored validators are attached when the client didn't
// send its own, so upstream can answer 304 without re-sending the body.
func revalRequest(r *http.Request, e *entry) *http.Request {
	r2 := r.Clone(r.Context())
	r2.Header = r.Header.Clone()
	r2.Header.Del("Range")

	hdrs := *e.headers.Load()
	if r2.Header.Get("If-None-Match") == "" {
		if et := hdrs.Get("ETag"); et != "" {
			r2.Header.Set("If-None-Match", et)
		}
	}
	if r2.Header.Get("If-Modified-Since") == "" {
		if lm := hdrs.Get("Last-Modified"); lm != "" {
			r2.Header.Set("If-Modified-Since", lm)
		}
	}

	return r2
}

// Cleanup implements caddy.CleanerUpper: stops background goroutines and
// unregisters the instance on reload.
func (fc *FastCache) Cleanup() error {
	liveCaches.Delete(fc)

	if fc.idx != nil {
		fc.idx.stop()
	}

	return nil
}

func (fc *FastCache) startFlight(key string) (chan struct{}, bool) {
	ch := make(chan struct{})
	if v, loaded := fc.flights.LoadOrStore(key, ch); loaded {
		return v.(chan struct{}), false
	}

	return ch, true
}

func (fc *FastCache) finishFlight(key string, ch chan struct{}) {
	fc.flights.Delete(key)
	close(ch)
}

// varyHeadersOf returns the vary header list stored for a base key, if any.
func (fc *FastCache) varyHeadersOf(base string) []string {
	if v, ok := fc.idx.mappings.Load(base); ok {
		return v.(*mapping).varyHeaders
	}

	return nil
}

// refresh re-fetches a stale entry in the background, conditionally when
// the entry has validators (a 304 merges in place instead of re-storing the
// body). Only one refresh per key runs at a time — it shares the "reval:"
// flight key with synchronous revalidations. The request context is
// detached so it survives the caller's return, but bounded by
// revalidate_timeout so a hung upstream can't leak the goroutine forever.
func (fc *FastCache) refresh(e *entry, r *http.Request, next caddyhttp.Handler) {
	key := "reval:" + e.key

	done, leader := fc.startFlight(key)
	if !leader {
		return
	}

	// Everything derived from r happens before the goroutine: the request
	// is not safe to read once ServeHTTP returns.
	req := revalRequest(r, e)
	ownVal := canMerge304(r, e)

	// Detached from the request so it survives the caller's return; a
	// revalidate_timeout of 0 means unbounded.
	ctx := context.WithoutCancel(r.Context())
	var cancel context.CancelFunc = func() {}
	if fc.revalidateTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, fc.revalidateTimeout)
	}
	req = req.WithContext(ctx)

	go func() {
		defer cancel()
		defer fc.finishFlight(key, done)

		cw := newCaptureWriter(fc, &discardRW{h: http.Header{}}, req, e.baseKey)
		cw.reval = e
		cw.next = next
		cw.ownVal = ownVal

		err := next.ServeHTTP(cw, req)
		if err != nil {
			if ce := fc.logger.Check(zap.DebugLevel, "background revalidation failed"); ce != nil {
				ce.Write(zap.String("key", e.key), zap.Error(err))
			}
		}
		cw.Close(err)
	}()
}

// discardRW is a ResponseWriter that swallows the response (the body is still
// teed to disk by the captureWriter wrapping it).
type discardRW struct{ h http.Header }

func (d *discardRW) Header() http.Header         { return d.h }
func (d *discardRW) WriteHeader(int)             {}
func (d *discardRW) Write(b []byte) (int, error) { return len(b), nil }

// statusRW captures the response status of a bypassed request, so unsafe
// methods can invalidate the cached URI on upstream success (§4.4).
type statusRW struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRW) WriteHeader(code int) {
	if !s.wrote {
		s.status = code
		s.wrote = true
	}

	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRW) Write(b []byte) (int, error) {
	if !s.wrote {
		s.WriteHeader(http.StatusOK)
	}

	return s.ResponseWriter.Write(b)
}

func (s *statusRW) Flush() {
	if !s.wrote {
		s.WriteHeader(http.StatusOK)
	}

	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ReadFrom keeps the underlying writer's sendfile path available to
// io.Copy through the wrapper on bypassed unsafe-method responses.
func (s *statusRW) ReadFrom(src io.Reader) (int64, error) {
	if !s.wrote {
		s.WriteHeader(http.StatusOK)
	}

	if rf, ok := s.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}

	return io.Copy(s.ResponseWriter, src)
}

// Unwrap lets http.ResponseController reach the real writer (full-duplex
// and deadline controls probe through it).
func (s *statusRW) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}

	return h.Hijack()
}

// serveEntry writes a cached variant to the client. cs holds RFC 9211
// Cache-Status parameters describing how the hit was reached (e.g. "hit",
// "fwd=stale", "collapsed").
func (fc *FastCache) serveEntry(w http.ResponseWriter, r *http.Request, e *entry, next caddyhttp.Handler, cs ...string) error {
	// RAM tier first; otherwise open the body file for sendfile.
	var (
		body *[]byte
		f    *os.File
	)

	if body = e.body.Load(); body == nil {
		var done func()
		var err error

		if f, done, err = fc.idx.openBody(e); err != nil {
			// File vanished under us — drop the index entry and go upstream.
			if ce := fc.logger.Check(zap.DebugLevel, "body file missing; dropping entry and refetching"); ce != nil {
				ce.Write(zap.String("key", e.key), zap.String("path", e.path), zap.Error(err))
			}
			fc.idx.deleteEntry(e)

			return fc.fetchAndStore(w, r, e.baseKey, next)
		}
		defer done()
	}

	h := w.Header()

	hdrs := *e.headers.Load()
	maps.Copy(h, hdrs)

	h["Cache-Status"] = []string{"fastcache; " + strings.Join(cs, "; ")}

	// storedAt already embeds the upstream Age (see captureWriter.decide).
	// Set it before the conditional early-returns so 304s carry it too.
	if age := (time.Now().UnixNano() - e.storedAt.Load()) / int64(time.Second); age > 0 {
		h["Age"] = []string{strconv.FormatInt(age, 10)}
	}

	// If-Match is evaluated before If-None-Match: a failed precondition
	// (412) takes precedence over a not-modified answer (RFC 9110 §13.2.2).
	// It takes strong comparison — a weak stored ETag can never satisfy it.
	if im := r.Header.Get("If-Match"); im != "" && !etagStrongMatch(im, hdrs.Get("ETag")) {
		w.WriteHeader(http.StatusPreconditionFailed)

		return nil
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etag := hdrs.Get("ETag"); etag != "" && etagMatches(inm, etag) {
			w.WriteHeader(http.StatusNotModified)

			return nil
		}
	}

	// Fast path: a plain GET/HEAD without Range or conditional headers does
	// not need ServeContent's parsing machinery — a direct body write (or a
	// sendfile-capable io.Copy for disk entries) is enough.
	if r.Header.Get("Range") == "" && r.Header.Get("If-Modified-Since") == "" && r.Header.Get("If-Unmodified-Since") == "" && r.Header.Get("If-Range") == "" {
		h["Content-Length"] = []string{strconv.FormatInt(e.size, 10)}
		w.WriteHeader(e.status)

		if r.Method == http.MethodHead {
			return nil
		}

		if body != nil {
			_, _ = w.Write(*body)
		} else {
			// Copy picks up the ResponseWriter's ReadFrom → sendfile.
			_, _ = io.Copy(w, f)
		}

		return nil
	}

	// Range/conditional requests get the full ServeContent treatment.
	var rs io.ReadSeeker = f
	if body != nil {
		rs = bytes.NewReader(*body)
	}

	http.ServeContent(w, r, "", lastModified(e), rs)

	return nil
}

// ttlParam is the Cache-Status ttl parameter: remaining freshness, seconds.
func ttlParam(e *entry) string {
	rem := max(e.expiresAt.Load()-time.Now().UnixNano(), 0)

	return "ttl=" + strconv.FormatInt(rem/int64(time.Second), 10)
}

func lastModified(e *entry) time.Time {
	if lm := (*e.headers.Load()).Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			return t
		}
	}

	return time.Time{}
}

func etagMatches(inm, etag string) bool {
	// If-None-Match on GET/HEAD uses weak comparison (RFC 9110 §13.1.2):
	// W/"x" and "x" are equivalent.
	etag = strings.TrimPrefix(etag, "W/")

	for t := range strings.SplitSeq(inm, ",") {
		if v := strings.TrimPrefix(strings.TrimSpace(t), "W/"); v == etag || v == "*" {
			return true
		}
	}

	return false
}

// etagStrongMatch evaluates If-Match (RFC 9110 §13.1.1): strong comparison
// only — a weak stored ETag (W/"x") satisfies nothing, and "*" matches any
// current representation (a cache hit means one exists, ETag or not).
func etagStrongMatch(im, etag string) bool {
	for t := range strings.SplitSeq(im, ",") {
		v := strings.TrimSpace(t)
		if v == "*" || (etag != "" && v == etag) {
			return true
		}
	}

	return false
}

// compilePurgeAllow turns purge_allow strings (IPs or CIDRs) into prefixes.
func (fc *FastCache) compilePurgeAllow() error {
	for _, s := range fc.PurgeAllow {
		if strings.Contains(s, "/") {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				return fmt.Errorf("purge_allow %q: %w", s, err)
			}

			fc.purgeNets = append(fc.purgeNets, p.Masked())
		} else {
			a, err := netip.ParseAddr(s)
			if err != nil {
				return fmt.Errorf("purge_allow %q: %w", s, err)
			}

			fc.purgeNets = append(fc.purgeNets, netip.PrefixFrom(a, a.BitLen()))
		}
	}

	return nil
}

func (fc *FastCache) purgeAllowed(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}

	// Unconfigured: PURGE is loopback-only by default.
	if len(fc.purgeNets) == 0 {
		return ip.IsLoopback()
	}

	for _, p := range fc.purgeNets {
		if p.Contains(ip) {
			return true
		}
	}

	return false
}

func (fc *FastCache) handlePurge(w http.ResponseWriter, r *http.Request) error {
	if !fc.purgeAllowed(r) {
		if ce := fc.logger.Check(zap.DebugLevel, "PURGE denied"); ce != nil {
			ce.Write(zap.String("remote", r.RemoteAddr), zap.String("key", fc.idx.baseKey(r)))
		}
		w.Header().Set("Cache-Status", "fastcache; fwd=request; detail=purge-denied")
		w.WriteHeader(http.StatusForbidden)

		return nil
	}

	if fc.idx.purge(fc.idx.baseKey(r), "purge") == 0 {
		w.Header().Set("Cache-Status", "fastcache; fwd=purge-miss")
		w.WriteHeader(http.StatusNotFound)

		return nil
	}

	w.Header().Set("Cache-Status", "fastcache; purged")
	fc.idx.metrics.req("purged")
	w.WriteHeader(http.StatusOK)

	return nil
}

// parseFastCache adapts the Caddyfile directive to the module.
func parseFastCache(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var fc FastCache
	if err := fc.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}

	return &fc, nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
func (fc *FastCache) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "path":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.Path = d.Val()
			case "memory_size":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.MemorySize = d.Val()
			case "disk_size":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.DiskSize = d.Val()
			case "max_items":
				if !d.NextArg() {
					return d.ArgErr()
				}

				n, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.Errf("invalid max_items %q", d.Val())
				}

				fc.MaxItems = n
			case "ttl":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.TTL = d.Val()
			case "stale_while_revalidate":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.StaleWhileRevalidate = d.Val()
			case "stale_if_error":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.StaleIfError = d.Val()
			case "cache_key":
				if !d.NextArg() {
					return d.ArgErr()
				}

				fc.CacheKey = d.Val()
			case "match_header":
				if !d.NextArg() {
					return d.ArgErr()
				}

				name := d.Val()
				vals := d.RemainingArgs()
				if len(vals) == 0 {
					return d.Errf("match_header %q requires at least one value", name)
				}

				if fc.MatchHeaders == nil {
					fc.MatchHeaders = map[string][]string{}
				}

				fc.MatchHeaders[name] = append(fc.MatchHeaders[name], vals...)
			case "vary":
				fc.Vary = append(fc.Vary, d.RemainingArgs()...)
			case "disable_touch_on_hit":
				fc.DisableTouchOnHit = true
			case "purge_allow":
				fc.PurgeAllow = append(fc.PurgeAllow, d.RemainingArgs()...)
			case "honor_request_cache_control":
				fc.HonorRequestCC = true
			case "store_set_cookie":
				fc.StoreSetCookie = true
			case "promote_below":
				if !d.NextArg() {
					return d.ArgErr()
				}
				fc.PromoteBelow = d.Val()
			case "revalidate_timeout":
				if !d.NextArg() {
					return d.ArgErr()
				}
				fc.RevalidateTimeout = d.Val()
			default:
				return d.Errf("unknown fastcache option %q", d.Val())
			}
		}
	}

	return nil
}

var (
	_ caddy.Provisioner           = (*FastCache)(nil)
	_ caddy.CleanerUpper          = (*FastCache)(nil)
	_ caddyfile.Unmarshaler       = (*FastCache)(nil)
	_ caddyhttp.MiddlewareHandler = (*FastCache)(nil)
)
