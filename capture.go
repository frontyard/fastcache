package fastcache

import (
	"bufio"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

// captureWriter wraps the downstream ResponseWriter on a miss. It streams
// every byte to the client while teeing a cacheable body into a temp file;
// on Close it commits the entry (header block + body, atomic rename).
//
// rwP is an atomic pointer because a fill may outlive its client: once the
// request context is done, detach() swaps the downstream writer for a
// discardRW — writes stop failing, so a handler streaming to a dead client
// (reverse_proxy's panic(http.ErrAbortHandler) path) can't abort a fill
// that coalesced followers are still waiting on.
type captureWriter struct {
	rwP  atomic.Pointer[http.ResponseWriter]
	ix   *index
	fc   *FastCache
	req  *http.Request
	base string
	next caddyhttp.Handler

	status    int
	hdr       http.Header // upstream's headers, buffered — see Header()
	cacheable bool
	decided   bool
	merged304 bool // revalidation answered 304: headers merged into reval
	varyH     []string
	fresh     time.Duration

	tmp      *os.File
	tmpPath  string
	final    string
	metaPath string
	meta     *metaHeader
	bodySize int64
	declared int64 // upstream Content-Length, -1 if absent
	failed   bool
	wrote    bool   // headers/body already committed to the client
	swallow  bool   // a stale fallback was served; drop upstream output
	hijacked bool   // downstream was hijacked mid-capture; never commit
	purgeAt  uint64 // purgeSeq snapshot at capture start; a bump aborts the commit

	// fallback is a stale entry to serve when upstream fails (stale_if_error).
	fallback *entry

	// reval is the stored entry being conditionally revalidated: a 304
	// merges into it instead of committing a new body.
	reval *entry

	// ownVal is set when the revalidation carried the stored validators —
	// or the client's If-None-Match already names the stored ETag — so a
	// 304 proves OUR entry fresh and may merge. When the client sent other
	// validators, a 304 only proves their copy fresh: it forwards unmerged.
	ownVal bool

	// origReq is the client's original request when req was rewritten for
	// revalidation — stale/304 responses are served against the client's
	// own conditional headers, not ours.
	origReq *http.Request

	// buf mirrors the body in memory while it fits the RAM budget, so the
	// entry can be committed to the RAM tier without re-reading the file.
	// bufLimit caps it: memBudget in RAM-only mode, or the lower of
	// memBudget/promoteBelow when a disk tier exists (oversized bodies
	// serve faster via sendfile than from a heap copy).
	buf      []byte
	keepBuf  bool
	bufLimit int64
}

func newCaptureWriter(fc *FastCache, rw http.ResponseWriter, r *http.Request, base string) *captureWriter {
	cw := &captureWriter{ix: fc.idx, fc: fc, req: r, base: base, status: http.StatusOK, declared: -1,
		hdr: http.Header{}, purgeAt: fc.idx.purgeSeq.Load()}
	cw.rwP.Store(&rw)

	return cw
}

// writer returns the current downstream writer: the client's, or a discard
// sink once detach() ran.
func (cw *captureWriter) writer() http.ResponseWriter { return *cw.rwP.Load() }

// detach swaps the downstream writer for a discard sink. The client is gone,
// so bytes no longer need to reach it — but failing writes would abort the
// upstream stream (caddy's reverse_proxy panics ErrAbortHandler on a copy
// error) and kill a fill that waiting followers still need. A write already
// in flight to the dead client may still fail and poison the capture, which
// is safe: it is discarded on Close and followers retry as the next leader.
func (cw *captureWriter) detach() {
	var d http.ResponseWriter = &discardRW{h: http.Header{}}
	cw.rwP.Store(&d)
}

// Header returns a private buffer, not the client's live header map: a
// swallowed response (304 merge, stale fallback) or a failed handler must
// never leak its headers into what gets served. decide() copies them into
// the real writer only when the upstream response is committed to the
// client.
func (cw *captureWriter) Header() http.Header { return cw.hdr }

// decide runs on the first WriteHeader (or implicit 200): it evaluates
// cacheability and, when positive, opens the temp file and writes the meta
// block so body bytes can stream straight to disk.
func (cw *captureWriter) decide(status int) {
	cw.decided = true
	cw.status = status

	serveReq := cw.req
	if cw.origReq != nil {
		serveReq = cw.origReq
	}

	// Conditional revalidation (RFC 9111 §4.3.4): upstream kept the body —
	// merge the new headers and freshness into the stored entry and serve
	// it, swallowing the upstream output.
	if status == http.StatusNotModified && cw.reval != nil && cw.ownVal {
		cw.swallow = true
		cw.merged304 = true
		cw.ix.refresh304(cw.reval, cw.hdr)
		if ce := cw.fc.logger.Check(zap.DebugLevel, "revalidated entry via 304 merge"); ce != nil {
			ce.Write(zap.String("key", cw.reval.key))
		}
		_ = cw.fc.serveEntry(cw.writer(), serveReq, cw.reval, cw.next, "hit")

		return
	}

	// stale_if_error: upstream answered with a server error and a stale
	// entry is held — serve it and swallow the upstream output unless the
	// entry forbids stale serving (must-revalidate). Nothing is stored
	// (5xx is uncacheable anyway).
	if cw.fallback != nil && status >= 500 && !cw.fallback.mustRevalidate.Load() {
		cw.swallow = true
		if ce := cw.fc.logger.Check(zap.DebugLevel, "upstream error; serving stale entry"); ce != nil {
			ce.Write(zap.String("key", cw.fallback.key), zap.Int("status", status))
		}
		_ = cw.fc.serveEntry(cw.writer(), serveReq, cw.fallback, cw.next,
			"hit", "fwd=stale", "fwd-status="+strconv.Itoa(status))

		return
	}

	// The upstream response is going to the client: move the buffered
	// headers into the real writer, dropping hop-by-hop fields.
	cw.forwardHeaders()

	fresh, swr, varyH, mr, ok := responseCacheable(cw.ix, cw.req, status, cw.hdr)
	if !ok || fresh <= 0 {
		cw.missStatus()
		if ce := cw.fc.logger.Check(zap.DebugLevel, "response not stored"); ce != nil {
			ce.Write(zap.String("key", cw.base), zap.Int("status", status),
				zap.Bool("cacheable", ok), zap.Duration("fresh", fresh))
		}
		return
	}

	cw.cacheable = true
	cw.varyH = varyH
	cw.fresh = fresh

	if cl := cw.hdr.Get("Content-Length"); cl != "" {
		if n, err := strconv.ParseInt(cl, 10, 64); err == nil && n >= 0 {
			cw.declared = n
		}
	}

	if cw.ix.ramOnly {
		// No disk tier: the body must fit the memory budget. Undersized
		// declared bodies are rejected here; undeclared ones are buffered
		// and aborted by Write if they grow past the budget.
		if cw.ix.memBudget <= 0 || cw.declared > cw.ix.memBudget {
			cw.cacheable = false
			if ce := cw.fc.logger.Check(zap.DebugLevel, "response not stored"); ce != nil {
				ce.Write(zap.String("key", cw.base), zap.String("reason", "memory budget"),
					zap.Int64("declared_size", cw.declared))
			}

			return
		}

		cw.bufLimit = cw.ix.memBudget
		cw.keepBuf = true
		if cw.declared > 0 {
			cw.buf = make([]byte, 0, cw.declared)
		}
	} else {
		cw.bufLimit = min(cw.ix.memBudget, cw.ix.promoteBelow)

		cw.final = cw.ix.filePath(cw.key())
		cw.tmpPath = cw.final + ".tmp"
		cw.metaPath = metaPathFor(cw.final)

		if err := os.MkdirAll(filepath.Dir(cw.final), 0o750); err != nil {
			cw.cacheable = false
			cw.fc.logger.Warn("cannot create cache directory; response not stored",
				zap.String("path", filepath.Dir(cw.final)), zap.Error(err))

			return
		}

		f, err := os.OpenFile(cw.tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
		if err != nil {
			cw.cacheable = false
			cw.fc.logger.Warn("cannot create cache file; response not stored",
				zap.String("path", cw.tmpPath), zap.Error(err))

			return
		}

		cw.tmp = f

		// Undeclared length: buffer optimistically, Write drops buf if the
		// body outgrows bufLimit. Declared oversize: skip the mirror entirely.
		cw.keepBuf = cw.bufLimit > 0 && (cw.declared <= 0 || cw.declared <= cw.bufLimit)
		if cw.keepBuf && cw.declared > 0 {
			cw.buf = make([]byte, 0, cw.declared)
		}
	}

	now := time.Now()

	// Account for the age the response already accumulated in an upstream
	// cache (RFC 9111 §4.2.3): shifting the clock base back makes StoredAt,
	// ExpiresAt and the served Age header all include it.
	if a, err := strconv.Atoi(strings.TrimSpace(cw.hdr.Get("Age"))); err == nil && a > 0 {
		now = now.Add(-time.Duration(a) * time.Second)
	}

	cw.meta = &metaHeader{
		Key:            cw.base,
		VaryHeaders:    varyH,
		VaryKey:        varyKey(cw.req, varyH),
		Status:         status,
		Headers:        map[string][]string(cacheableHeaders(cw.hdr)),
		StoredAt:       now.UnixNano(),
		ExpiresAt:      now.Add(fresh).UnixNano(),
		StaleAt:        now.Add(fresh + max(swr, cw.ix.sie)).UnixNano(),
		MustRevalidate: mr,
	}

	cw.missStatus()
}

// forwardHeaders copies the buffered upstream headers into the client
// writer, dropping hop-by-hop fields: the static list, fields the response
// nominates via Connection, and trailer-value keys (RFC 9110 §7.6.1).
// Unlike cacheableHeaders it keeps Content-Length — the live response
// forwards it intact; stored entries recompute it on serve.
func (cw *captureWriter) forwardHeaders() {
	connToks := connHeaderTokens(cw.hdr)
	for k, vv := range cw.hdr {
		ck := http.CanonicalHeaderKey(k)
		if hopHeaders[ck] || connToks[ck] || strings.HasPrefix(k, http.TrailerPrefix) {
			continue
		}

		cw.writer().Header()[k] = vv
	}
}

// missStatus marks the response in Cache-Status (RFC 9211): uri-miss plus
// "stored" and the cache key when the response will be committed.
func (cw *captureWriter) missStatus() {
	cs := "fastcache; fwd=uri-miss"
	if cw.cacheable {
		cs += "; stored; key=" + strconv.Quote(cw.base)
	}

	cw.writer().Header().Set("Cache-Status", cs)
}

func (cw *captureWriter) key() string {
	vk := varyKey(cw.req, cw.varyH)
	if vk == "" {
		return cw.base
	}

	return cw.base + "|v|" + vk
}

func (cw *captureWriter) WriteHeader(status int) {
	if !cw.decided && status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		// Informational response — flush its headers downstream and keep
		// buffering for the final WriteHeader.
		cw.forwardHeaders()
		cw.writer().WriteHeader(status)

		return
	}

	if !cw.decided {
		cw.decide(status)
	}

	if cw.swallow {
		return
	}

	cw.writer().WriteHeader(status)
	cw.wrote = true
}

func (cw *captureWriter) Write(b []byte) (int, error) {
	if !cw.decided {
		cw.decide(http.StatusOK)
	}

	if cw.swallow {
		return len(b), nil
	}

	n, err := cw.writer().Write(b)
	cw.wrote = true

	if !cw.cacheable || cw.failed {
		return n, err
	}

	// A short or failed client write means the upstream stream is about to
	// be cut off (client gone): never commit a partial body.
	if err != nil || n != len(b) {
		cw.failed = true

		return n, err
	}

	if cw.tmp != nil {
		if _, werr := cw.tmp.Write(b); werr != nil {
			cw.failed = true
			cw.fc.logger.Warn("cache file write failed; entry will not be stored",
				zap.String("path", cw.tmpPath), zap.Error(werr))

			return n, err
		}
	}

	cw.bodySize += int64(n)

	if cw.keepBuf {
		if cw.bodySize <= cw.bufLimit {
			cw.buf = append(cw.buf, b...)
		} else {
			cw.keepBuf, cw.buf = false, nil
			if cw.ix.ramOnly {
				// Over the memory budget with no disk fallback: uncacheable.
				cw.failed = true
			}
		}
	}

	return n, err
}

// Close commits (or discards) the captured response: the body file is
// renamed into place, then the meta sidecar, then the index entry. A
// non-nil handler error discards the capture.
func (cw *captureWriter) Close(handlerErr error) {
	// Revalidation accounting: a 304 merge, a swallowed upstream error, a
	// failed/aborted capture, or an upstream 5xx are all outcomes to count.
	if cw.reval != nil {
		switch {
		case cw.merged304:
			cw.ix.metrics.reval("not_modified")
		case cw.swallow || handlerErr != nil || cw.failed || cw.status >= 500:
			cw.ix.metrics.reval("error")
		default:
			cw.ix.metrics.reval("ok")
		}
	}

	if !cw.cacheable {
		return
	}

	if cw.tmp != nil {
		_ = cw.tmp.Close()
	}

	if handlerErr != nil || cw.failed || cw.hijacked ||
		(cw.declared >= 0 && cw.declared != cw.bodySize) ||
		cw.req.Context().Err() != nil ||
		cw.ix.purgeSeq.Load() != cw.purgeAt {
		// A purge or unsafe-method invalidation raced this capture —
		// committing now would resurrect just-purged content.
		if ce := cw.fc.logger.Check(zap.DebugLevel, "discarding captured response"); ce != nil {
			reason := "purge raced the capture"
			switch {
			case handlerErr != nil:
				reason = "handler error"
			case cw.failed:
				reason = "write failed"
			case cw.hijacked:
				reason = "hijacked"
			case cw.declared >= 0 && cw.declared != cw.bodySize:
				reason = "size mismatch"
			case cw.req.Context().Err() != nil:
				reason = "request cancelled"
			}
			ce.Write(zap.String("key", cw.base), zap.String("reason", reason),
				zap.Error(handlerErr), zap.Error(cw.req.Context().Err()))
		}
		if cw.tmpPath != "" {
			_ = os.Remove(cw.tmpPath)
		}

		return
	}

	var metaSize int64

	if !cw.ix.ramOnly {
		if err := os.Rename(cw.tmpPath, cw.final); err != nil {
			_ = os.Remove(cw.tmpPath)
			cw.fc.logger.Warn("cannot commit cache file; entry not stored",
				zap.String("path", cw.final), zap.Error(err))

			return
		}

		// Meta sidecar goes in last — its presence marks a complete entry.
		mf, err := os.OpenFile(cw.metaPath+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
		if err != nil {
			_ = os.Remove(cw.final)
			cw.fc.logger.Warn("cannot write meta sidecar; entry not stored",
				zap.String("path", cw.metaPath), zap.Error(err))

			return
		}

		metaSize, err = writeMeta(mf, cw.meta)
		_ = mf.Close()

		if err != nil {
			_ = os.Remove(cw.final)
			_ = os.Remove(cw.metaPath + ".tmp")
			cw.fc.logger.Warn("cannot write meta sidecar; entry not stored",
				zap.String("path", cw.metaPath), zap.Error(err))

			return
		}

		if err := os.Rename(cw.metaPath+".tmp", cw.metaPath); err != nil {
			_ = os.Remove(cw.final)
			_ = os.Remove(cw.metaPath + ".tmp")
			cw.fc.logger.Warn("cannot commit meta sidecar; entry not stored",
				zap.String("path", cw.metaPath), zap.Error(err))

			return
		}
	}

	var fileSize int64
	if !cw.ix.ramOnly {
		fileSize = cw.bodySize + metaSize
	}

	hdr := http.Header(cw.meta.Headers)
	e := &entry{
		key:      cw.key(),
		baseKey:  cw.base,
		varyH:    cw.varyH,
		varyK:    varyKey(cw.req, cw.varyH),
		path:     cw.final,
		metaPath: cw.metaPath,
		size:     cw.bodySize,
		fileSize: fileSize,
		purgeAt:  cw.purgeAt,
		status:   cw.status,
	}
	e.headers.Store(&hdr)
	e.storedAt.Store(cw.meta.StoredAt)
	e.expiresAt.Store(cw.meta.ExpiresAt)
	e.staleAt.Store(cw.meta.StaleAt)
	e.mustRevalidate.Store(cw.meta.MustRevalidate)
	e.seq.Store(uint64(cw.meta.StoredAt))

	// Populate the RAM tier directly for in-budget bodies. The add is
	// optimistic: commit signals the eviction worker if it overflowed.
	if cw.keepBuf {
		data := cw.buf[:len(cw.buf):len(cw.buf)]
		e.body.Store(&data)
		cw.ix.memSize.Add(cw.bodySize)
	}

	cw.ix.commit(e)
}

// Implement optional ResponseWriter interfaces so streaming semantics pass
// through to the client unchanged.

func (cw *captureWriter) Flush() {
	if !cw.decided {
		cw.decide(http.StatusOK)
	}

	if cw.swallow {
		return
	}

	if f, ok := cw.writer().(http.Flusher); ok {
		f.Flush()
	}
}

func (cw *captureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := cw.writer().(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}

	c, rw, err := h.Hijack()
	if err == nil {
		// The handler took over the connection: whatever it writes now is
		// invisible to us — an undeclared-length body could otherwise be
		// committed truncated.
		cw.hijacked = true
	}

	return c, rw, err
}

// Unwrap lets http.ResponseController reach the real writer (full-duplex
// and deadline controls probe through it).
func (cw *captureWriter) Unwrap() http.ResponseWriter { return cw.writer() }
