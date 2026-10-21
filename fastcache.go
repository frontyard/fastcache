// Package fastcache implements a Caddy HTTP handler that caches responses in
// a two-tier store: an in-memory body tier under memory_size, and a disk
// capacity tier under disk_size. Hits are served either from RAM or via
// kernel sendfile, with no locks on the read path.
package fastcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dustin/go-humanize"
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

	memoryBytes  int64
	diskBytes    int64
	promoteBelow int64
	ttl          time.Duration
	swr          time.Duration
	sie          time.Duration
	purgeNets    []netip.Prefix

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

	go fc.idx.sweep()
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

		sw := &statusRW{ResponseWriter: w, status: http.StatusOK}
		err := next.ServeHTTP(sw, r)
		fc.idx.metrics.req("bypass")

		if err != nil || sw.status >= 400 {
			return err
		}

		fc.idx.purge(fc.idx.baseKey(r), "invalidate")

		return nil
	}

	if requestBypass(fc.idx, r) {
		w.Header().Set("Cache-Status", "fastcache; fwd=request; detail=bypass")
		fc.idx.metrics.req("bypass")

		return next.ServeHTTP(w, r)
	}

	if e, stale, hit := fc.idx.lookup(r); hit {
		if stale {
			mr := e.mustRevalidate.Load()

			if !mr && fc.idx.withinSWR(e, time.Now().UnixNano()) {
				fc.refresh(e, r, next)
				fc.idx.metrics.req("stale")

				return fc.serveEntry(w, r, e, next, "hit", "fwd=stale")
			}

			// Beyond the SWR window — or any stale entry forbidding stale
			// serving — revalidate synchronously, coalesced. Without
			// must-revalidate the stale entry is the error fallback
			// (stale_if_error) and is served to followers immediately.
			done, leader := fc.startFlight("sie:" + e.key)
			if !leader {
				if !mr {
					fc.idx.metrics.req("stale")

					return fc.serveEntry(w, r, e, next, "hit", "fwd=stale", "collapsed")
				}

				select {
				case <-done:
				case <-r.Context().Done():
					return r.Context().Err()
				}

				if e2, stale2, hit2 := fc.idx.lookup(r); hit2 && !stale2 {
					fc.idx.metrics.req("hit")

					return fc.serveEntry(w, r, e2, next, "hit", "collapsed")
				}

				fc.idx.metrics.req("miss")

				return fc.fetchAndStore(w, r, fc.idx.baseKey(r), next)
			}

			defer fc.finishFlight("sie:"+e.key, done)
			fc.idx.metrics.req("revalidated")

			return fc.revalidateOrStale(w, r, e, next)
		}

		fc.idx.metrics.req("hit")

		return fc.serveEntry(w, r, e, next, "hit", ttlParam(e))
	}

	// Miss coalescing: the leader fetches upstream and streams the response
	// to its client while storing it; followers wait, then serve the entry.
	base := fc.idx.baseKey(r)
	flightKey := base + "|" + varyKey(r, fc.varyHeadersOf(base))

	done, leader := fc.startFlight(flightKey)
	if !leader {
		select {
		case <-done:
		case <-r.Context().Done():
			return r.Context().Err()
		}

		if e, stale, hit := fc.idx.lookup(r); hit {
			if stale {
				if e.mustRevalidate.Load() {
					fc.idx.metrics.req("miss")

					return fc.fetchAndStore(w, r, base, next)
				}

				if fc.idx.withinSWR(e, time.Now().UnixNano()) {
					fc.refresh(e, r, next)
					fc.idx.metrics.req("stale")

					return fc.serveEntry(w, r, e, next, "hit", "fwd=stale", "collapsed")
				}
			}

			fc.idx.metrics.req("hit")

			return fc.serveEntry(w, r, e, next, "hit", "collapsed")
		}

		// The leader's response was not cacheable, or it was a different
		// variant: fetch ourselves (and store if this one is cacheable).
		fc.idx.metrics.req("miss")

		return fc.fetchAndStore(w, r, base, next)
	}

	defer fc.finishFlight(flightKey, done)
	fc.idx.metrics.req("miss")

	return fc.fetchAndStore(w, r, base, next)
}

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
	defer func() { cw.Close(err) }()

	if err = next.ServeHTTP(cw, r2); err != nil && !cw.wrote && !cw.swallow && !e.mustRevalidate.Load() {
		// The handler failed before anything reached the client.
		return fc.serveEntry(w, r, e, next, "hit", "fwd=stale")
	}

	return err
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
// body). Only one refresh per key runs at a time; the request context is
// detached so it survives the caller's return.
func (fc *FastCache) refresh(e *entry, r *http.Request, next caddyhttp.Handler) {
	key := "reval:" + e.key

	done, leader := fc.startFlight(key)
	if !leader {
		return
	}

	go func() {
		defer fc.finishFlight(key, done)

		req := revalRequest(r, e)
		req = req.WithContext(context.WithoutCancel(r.Context()))

		cw := newCaptureWriter(fc, &discardRW{h: http.Header{}}, req, e.baseKey)
		cw.reval = e
		cw.next = next

		err := next.ServeHTTP(cw, req)
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
			fc.idx.deleteEntry(e)

			return fc.fetchAndStore(w, r, e.baseKey, next)
		}
		defer done()
	}

	h := w.Header()

	hdrs := *e.headers.Load()
	for k, vv := range hdrs {
		h[k] = vv
	}

	h["Cache-Status"] = []string{"fastcache; " + strings.Join(cs, "; ")}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etag := hdrs.Get("ETag"); etag != "" && etagMatches(inm, etag) {
			w.WriteHeader(http.StatusNotModified)

			return nil
		}
	}

	// storedAt already embeds the upstream Age (see captureWriter.decide).
	if age := (time.Now().UnixNano() - e.storedAt.Load()) / int64(time.Second); age > 0 {
		h["Age"] = []string{strconv.FormatInt(age, 10)}
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
	rem := e.expiresAt.Load() - time.Now().UnixNano()
	if rem < 0 {
		rem = 0
	}

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

	for _, t := range strings.Split(inm, ",") {
		if v := strings.TrimPrefix(strings.TrimSpace(t), "W/"); v == etag || v == "*" {
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
		w.WriteHeader(http.StatusForbidden)

		return nil
	}

	w.Header().Set("Cache-Status", "fastcache; purged")
	fc.idx.metrics.req("purged")

	if fc.idx.purge(fc.idx.baseKey(r), "purge") == 0 {
		w.WriteHeader(http.StatusNotFound)

		return nil
	}

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

				if fc.MatchHeaders == nil {
					fc.MatchHeaders = map[string][]string{}
				}

				fc.MatchHeaders[d.Val()] = append(fc.MatchHeaders[d.Val()], d.RemainingArgs()...)
			case "vary":
				fc.Vary = append(fc.Vary, d.RemainingArgs()...)
			case "disable_touch_on_hit":
				fc.DisableTouchOnHit = true
			case "purge_allow":
				fc.PurgeAllow = append(fc.PurgeAllow, d.RemainingArgs()...)
			case "honor_request_cache_control":
				fc.HonorRequestCC = true
			case "promote_below":
				if !d.NextArg() {
					return d.ArgErr()
				}
				fc.PromoteBelow = d.Val()
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
