package fastcache

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
)

// fileMagic identifies cache entry files written by this plugin. FCH2
// changed the varyKey encoding (length-prefixed, all header values); older
// metas carry keys a lookup would never recompute, so they are rejected and
// reaped at rebuild.
var fileMagic = []byte{'F', 'C', 'H', '2'}

// metaHeader is the serialized sidecar for one entry (written to
// <bodyfile>.meta). Keeping meta and body in separate files lets the body be
// served with kernel sendfile (a standalone *os.File under ServeContent).
type metaHeader struct {
	Key            string // base key (host + requestURI)
	VaryHeaders    []string
	VaryKey        string
	Status         int
	Headers        map[string][]string
	StoredAt       int64
	ExpiresAt      int64
	StaleAt        int64
	MustRevalidate bool // response forbids stale serving (must-revalidate)
}

// entry is one cached variant tracked by the index. Headers and the three
// timestamps are atomic because a 304 revalidation merge (refresh304)
// updates them in place while concurrent hits keep reading.
type entry struct {
	key            string // full index key (base + vary suffix)
	baseKey        string
	varyH          []string
	varyK          string
	path           string // body file
	metaPath       string // sidecar meta file
	size           int64  // body size
	fileSize       int64  // body + meta, for disk accounting
	body           atomic.Pointer[[]byte]
	headers        atomic.Pointer[http.Header]
	storedAt       atomic.Int64
	expiresAt      atomic.Int64
	staleAt        atomic.Int64
	mustRevalidate atomic.Bool
	status         int
	seq            atomic.Uint64 // last-use UnixNano, coarse (see touchGranularity)
	promoting      atomic.Bool   // a disk hit is currently loading the body into RAM
}

// meta serializes the entry's current state into a sidecar header.
func (e *entry) meta() *metaHeader {
	return &metaHeader{
		Key:            e.baseKey,
		VaryHeaders:    e.varyH,
		VaryKey:        e.varyK,
		Status:         e.status,
		Headers:        map[string][]string(*e.headers.Load()),
		StoredAt:       e.storedAt.Load(),
		ExpiresAt:      e.expiresAt.Load(),
		StaleAt:        e.staleAt.Load(),
		MustRevalidate: e.mustRevalidate.Load(),
	}
}

// touchGranularity bounds how often a hit rewrites entry.seq. Reads of seq
// are cheap shared-cache-line loads; only a changed value is stored, so a
// hot entry costs one cache-line write per second instead of one per hit
// (and there is no global counter contended across cores).
const touchGranularity = uint64(time.Second)

func (e *entry) touch(now int64) {
	if n := uint64(now); e.seq.Load()+touchGranularity < n {
		e.seq.Store(n)
	}
}

func (e *entry) expired(now int64) bool {
	return now > e.expiresAt.Load()
}

func (e *entry) dead(now int64) bool {
	return now > e.staleAt.Load()
}

// withinSWR reports whether a stale entry is still inside its
// stale-while-revalidate window (serve stale + refresh in background).
// Beyond it — but before staleAt — the entry is an error fallback only.
func (ix *index) withinSWR(e *entry, now int64) bool {
	return now <= e.expiresAt.Load()+int64(ix.swr)
}

// mapping groups the variants of one base key.
type mapping struct {
	varyHeaders []string
	variants    sync.Map // varyKey -> *entry
}

// expiryItem is a deadline scheduled for the sweeper.
type expiryItem struct {
	key     string
	staleAt int64
}

type expiryHeap []expiryItem

func (h expiryHeap) Len() int            { return len(h) }
func (h expiryHeap) Less(i, j int) bool  { return h[i].staleAt < h[j].staleAt }
func (h expiryHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x interface{}) { *h = append(*h, x.(expiryItem)) }

func (h *expiryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = expiryItem{}
	*h = old[:n-1]

	return item
}

// index is the two-tier (RAM body tier + disk capacity tier) cache store.
type index struct {
	entries  sync.Map // full key -> *entry
	mappings sync.Map // base key -> *mapping

	dir          string
	memBudget    int64
	diskBudget   int64
	promoteBelow int64
	maxItems     int64
	defaultTTL   time.Duration
	swr          time.Duration
	sie          time.Duration
	varyList     []string
	noTouch      bool
	honorReqCC   bool
	storeCookie  bool
	ramOnly      bool // disk_size 0: bodies live in RAM only, nothing is persisted
	keyTmpl      string
	matchHeaders map[string][]string // canonical name -> allowed values
	metrics      *cacheMetrics

	memSize   atomic.Int64
	diskSize  atomic.Int64
	fileCount atomic.Int64
	fileSeq   atomic.Uint64 // unique suffix for body files; seeded from the wall clock
	purgeSeq  atomic.Uint64 // bumped by every purge; in-flight captures compare

	expQueue  expiryHeap
	heapMu    sync.Mutex
	mappingMu sync.Mutex
	evictMu   sync.Mutex
	evictCh   chan struct{} // 1-slot wakeup for evictLoop; sends coalesce
	sweepStop chan struct{}
	stopOnce  sync.Once
}

func newIndex(cfg *FastCache) *index {
	ix := &index{
		dir:          cfg.Path,
		memBudget:    cfg.memoryBytes,
		diskBudget:   cfg.diskBytes,
		promoteBelow: cfg.promoteBelow,
		maxItems:     int64(cfg.MaxItems),
		defaultTTL:   cfg.ttl,
		swr:          cfg.swr,
		sie:          cfg.sie,
		varyList:     cfg.Vary,
		noTouch:      cfg.DisableTouchOnHit,
		honorReqCC:   cfg.HonorRequestCC,
		storeCookie:  cfg.StoreSetCookie,
		ramOnly:      cfg.diskBytes <= 0,
		keyTmpl:      cfg.CacheKey,
		matchHeaders: func() map[string][]string {
			if len(cfg.MatchHeaders) == 0 {
				return nil
			}

			m := make(map[string][]string, len(cfg.MatchHeaders))
			for name, vals := range cfg.MatchHeaders {
				m[http.CanonicalHeaderKey(name)] = vals
			}

			return m
		}(),
		evictCh:   make(chan struct{}, 1),
		sweepStop: make(chan struct{}),
		metrics:   newCacheMetrics(),
	}

	// Seeding from the clock keeps file names unique across restarts: a
	// counter restarting at zero could reuse the path of an entry rebuilt
	// from disk, and committing over it would then delete the new file.
	ix.fileSeq.Store(uint64(time.Now().UnixNano()))

	return ix
}

// start launches the background workers (expiry sweeper + eviction worker).
// Called only after a successful rebuild, so a failed provision leaves no
// goroutines behind; stop() halts both.
func (ix *index) start() {
	go ix.evictLoop()
	go ix.sweep()
}

// lookup returns the live variant for a request, and whether it is stale.
// A stale entry may be within its SWR window or only retained as an error
// fallback (between expiresAt+swr and staleAt) — ServeHTTP distinguishes.
func (ix *index) lookup(r *http.Request) (e *entry, stale, hit bool) {
	key := ix.baseKey(r)

	v, ok := ix.mappings.Load(key)
	if !ok {
		return nil, false, false
	}

	m := v.(*mapping)

	ev, ok := m.variants.Load(varyKey(r, m.varyHeaders))
	if !ok {
		return nil, false, false
	}

	e = ev.(*entry)

	now := time.Now().UnixNano()

	if e.dead(now) {
		if ix.deleteEntry(e) {
			ix.metrics.evict("expired")
		}

		return nil, false, false
	}

	if !ix.noTouch {
		e.touch(now)
	}

	return e, e.expired(now), true
}

// commit installs a freshly stored variant.
func (ix *index) commit(e *entry) {
	ix.entries.Store(e.key, e)

	ix.mappingMu.Lock()

	var m *mapping

	if v, ok := ix.mappings.Load(e.baseKey); ok {
		m = v.(*mapping)
	} else {
		m = &mapping{varyHeaders: e.varyH}
		ix.mappings.Store(e.baseKey, m)
	}

	// Upstream changed its Vary list: existing variants are keyed under the
	// old scheme and lookups under the new one would never find them —
	// and worse, the old variants would keep serving to requests that now
	// deserve a different representation. Drop the whole mapping.
	if !slices.Equal(m.varyHeaders, e.varyH) {
		var orphans []*entry
		m.variants.Range(func(_, ev interface{}) bool {
			orphans = append(orphans, ev.(*entry))
			return true
		})

		m = &mapping{varyHeaders: e.varyH}
		ix.mappings.Store(e.baseKey, m)

		for _, oe := range orphans {
			ix.removeEntry(oe)
		}
	}

	prev, _ := m.variants.Swap(e.varyK, e)
	ix.mappingMu.Unlock()

	if prev != nil && prev != e {
		ix.removeEntry(prev.(*entry))
	}

	ix.diskSize.Add(e.fileSize)
	ix.fileCount.Add(1)
	ix.scheduleExpiry(e)
	ix.maybeEvict()
}

func (ix *index) scheduleExpiry(e *entry) {
	ix.heapMu.Lock()
	heap.Push(&ix.expQueue, expiryItem{key: e.key, staleAt: e.staleAt.Load()})
	ix.heapMu.Unlock()
}

// deleteEntry removes an entry from the index (file removal included). It
// reports whether this call was the one that removed it.
func (ix *index) deleteEntry(e *entry) bool {
	removed := ix.entries.CompareAndDelete(e.key, e)

	ix.mappingMu.Lock()
	if v, ok := ix.mappings.Load(e.baseKey); ok {
		m := v.(*mapping)
		m.variants.CompareAndDelete(e.varyK, e)

		empty := true
		m.variants.Range(func(_, _ interface{}) bool { empty = false; return false })

		if empty {
			ix.mappings.Delete(e.baseKey)
		}
	}
	ix.mappingMu.Unlock()

	if removed {
		ix.dropFile(e)
	}

	return removed
}

// removeEntry drops an entry from the index and deletes its files.
func (ix *index) removeEntry(e *entry) {
	if ix.entries.CompareAndDelete(e.key, e) {
		ix.dropFile(e)
	}
}

func (ix *index) dropFile(e *entry) {
	ix.diskSize.Add(-e.fileSize)
	ix.fileCount.Add(-1)

	if b := e.body.Swap(nil); b != nil {
		ix.memSize.Add(-int64(len(*b)))
	}

	if e.path != "" {
		_ = os.Remove(e.path)
	}
	if e.metaPath != "" {
		_ = os.Remove(e.metaPath)
	}
}

func (ix *index) reapExpired() {
	now := time.Now().UnixNano()

	for {
		ix.heapMu.Lock()

		if len(ix.expQueue) == 0 || ix.expQueue[0].staleAt > now {
			ix.heapMu.Unlock()

			return
		}

		item := heap.Pop(&ix.expQueue).(expiryItem)
		ix.heapMu.Unlock()

		if v, ok := ix.entries.Load(item.key); ok {
			e := v.(*entry)
			if e.staleAt.Load() == item.staleAt && ix.deleteEntry(e) {
				ix.metrics.evict("expired")
			}
		}
	}
}

func (ix *index) sweep() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ix.sweepStop:
			return
		case <-ticker.C:
			ix.reapExpired()
		}
	}
}

func (ix *index) stop() {
	ix.stopOnce.Do(func() { close(ix.sweepStop) })
}

// candidatesBySeq returns live entries accepted by keep, sorted least→most
// recently used.
func (ix *index) candidatesBySeq(keep func(*entry) bool) []*entry {
	var out []*entry

	ix.entries.Range(func(_, v interface{}) bool {
		if e := v.(*entry); keep == nil || keep(e) {
			out = append(out, e)
		}

		return true
	})

	sort.Slice(out, func(i, j int) bool { return out[i].seq.Load() < out[j].seq.Load() })

	return out
}

// evictSlack is the fraction of a budget freed beyond the limit once a
// budget is exceeded. Each eviction pass is a full O(n log n) scan of the
// index, so at steady state (disk or RAM full) evicting exactly to the limit
// would rescan on every single store; a 5% low-water mark amortizes one scan
// over ~5% worth of churn.
const evictSlack = 20

// overBudget reports whether any configured budget is currently exceeded.
func (ix *index) overBudget() bool {
	return (ix.diskBudget > 0 && ix.diskSize.Load() > ix.diskBudget) ||
		(ix.maxItems > 0 && ix.fileCount.Load() > ix.maxItems) ||
		(ix.memBudget > 0 && ix.memSize.Load() > ix.memBudget)
}

// maybeEvict wakes the eviction worker when a budget is exceeded. Sends
// coalesce on the 1-slot channel, so enforcement runs at most once per
// backlog — off the request path.
func (ix *index) maybeEvict() {
	if !ix.overBudget() {
		return
	}

	select {
	case ix.evictCh <- struct{}{}:
	default:
	}
}

// evictLoop performs budget enforcement on a dedicated goroutine. Woken by
// maybeEvict, it reaps expired entries and scans the index once per wakeup,
// freeing each budget to its low-water mark. Budgets may overshoot
// transiently between the signal and the scan.
func (ix *index) evictLoop() {
	for {
		select {
		case <-ix.sweepStop:
			return
		case <-ix.evictCh:
		}

		if !ix.overBudget() {
			continue
		}

		ix.evictMu.Lock()
		ix.enforceDiskBudget()
		ix.enforceCountCap()
		ix.freeRAM()
		ix.evictMu.Unlock()
	}
}

// The enforce*/freeRAM scans run only on the evictLoop goroutine, under
// evictMu. They must not take it again.

func (ix *index) enforceDiskBudget() {
	if ix.diskBudget <= 0 {
		return
	}

	ix.reapExpired()

	over := ix.diskSize.Load() - ix.diskBudget
	if over <= 0 {
		return
	}

	over += ix.diskBudget / evictSlack

	for _, e := range ix.candidatesBySeq(nil) {
		if over <= 0 {
			return
		}

		if ix.deleteEntry(e) {
			ix.metrics.evict("disk")
			over -= e.fileSize
		}
	}
}

func (ix *index) enforceCountCap() {
	if ix.maxItems <= 0 {
		return
	}

	over := ix.fileCount.Load() - ix.maxItems
	if over <= 0 {
		return
	}

	over += ix.maxItems / evictSlack

	for _, e := range ix.candidatesBySeq(nil) {
		if over <= 0 {
			return
		}

		if ix.deleteEntry(e) {
			ix.metrics.evict("count")
			over--
		}
	}
}

// freeRAM drops the lowest-seq in-memory bodies until the RAM budget fits.
func (ix *index) freeRAM() {
	if ix.memBudget <= 0 {
		return
	}

	over := ix.memSize.Load() - ix.memBudget
	if over <= 0 {
		return
	}

	over += ix.memBudget / evictSlack

	for _, e := range ix.candidatesBySeq(func(e *entry) bool { return e.body.Load() != nil }) {
		if over <= 0 {
			return
		}

		if ix.ramOnly {
			// No disk copy exists to demote to: evict the whole entry.
			if ix.deleteEntry(e) {
				ix.metrics.evict("memory")
				over -= e.size
			}

			continue
		}

		// Only demote; the disk copy remains servable.
		b := e.body.Swap(nil)
		if b == nil {
			continue
		}

		ix.memSize.Add(-int64(len(*b)))
		over -= int64(len(*b))
	}
}

// promotable reports whether a body of size bytes is worth holding in the
// RAM tier: within both the memory budget and promote_below. Larger bodies
// serve faster via sendfile from the page cache than from a heap copy.
func (ix *index) promotable(size int64) bool {
	return ix.memBudget > 0 && size <= ix.memBudget && size <= ix.promoteBelow
}

// openBody opens the standalone body file (a plain *os.File so the caller
// can sendfile it). The returned done closure closes it and, when the body
// fits the RAM budget, promotes it to the RAM tier.
func (ix *index) openBody(e *entry) (*os.File, func(), error) {
	f, err := os.Open(e.path)
	if err != nil {
		return nil, nil, err
	}

	done := func() { _ = f.Close() }

	// RAM promotion: keep small bodies resident when the budget allows.
	// Only one concurrent hit does the read; the others just serve.
	if ix.promotable(e.size) && e.promoting.CompareAndSwap(false, true) {
		done = func() {
			defer e.promoting.Store(false)
			defer func() { _ = f.Close() }()

			// The serve consumed f, so read from offset 0 explicitly (pread).
			data := make([]byte, e.size)
			if _, rerr := io.ReadFull(io.NewSectionReader(f, 0, e.size), data); rerr != nil {
				return
			}

			if e.body.CompareAndSwap(nil, &data) {
				ix.memSize.Add(e.size)

				// The entry may have been evicted while we served —
				// promoting into a dead entry would leak memSize forever
				// (freeRAM only scans indexed entries). Reclaim it, unless
				// dropFile's swap already accounted the removal.
				if v, ok := ix.entries.Load(e.key); !ok || v != e {
					if e.body.CompareAndSwap(&data, nil) {
						ix.memSize.Add(-e.size)
					}

					return
				}

				ix.maybeEvict()
			}
		}
	}

	return f, done, nil
}

// cacheFileRe matches only the file names fastcache itself creates:
// <24-hex>-<seq> body, +".tmp" in-flight body, +".meta" sidecar,
// +".meta.tmp" in-flight sidecar. The orphan pass in rebuild deletes only
// these — anything else under path is foreign and left alone.
var cacheFileRe = regexp.MustCompile(`^[0-9a-f]{24}-[0-9]+(\.meta)?(\.tmp)?$`)

// shardDirRe matches the 2-hex shard directories bodies live under.
var shardDirRe = regexp.MustCompile(`^[0-9a-f]{2}$`)

// filePath returns a unique on-disk path for a key's body file; the meta
// sidecar appends ".meta". The name is a hash of the key (keys can exceed
// NAME_MAX) plus a unique suffix; the key itself lives in the meta sidecar.
// Files are sharded into 256 directories by the name's first two hex chars —
// a flat directory degrades on some filesystems at millions of entries.
func (ix *index) filePath(key string) string {
	sum := sha256.Sum256([]byte(key))
	name := fmt.Sprintf("%x-%d", sum[:12], ix.fileSeq.Add(1))

	return filepath.Join(ix.dir, name[:2], name)
}

// metaPathFor returns the sidecar path for a body file.
func metaPathFor(bodyPath string) string { return bodyPath + ".meta" }

// writeMeta serializes magic+len+gob(meta) into the meta sidecar.
func writeMeta(w io.Writer, m *metaHeader) (int64, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		return 0, err
	}

	if _, err := w.Write(fileMagic); err != nil {
		return 0, err
	}

	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(buf.Len()))

	if _, err := w.Write(lb[:]); err != nil {
		return 0, err
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return 0, err
	}

	return int64(8 + buf.Len()), nil
}

// readMeta parses a meta sidecar.
func readMeta(path string) (*metaHeader, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	if len(data) < 8 || !bytes.Equal(data[:4], fileMagic) {
		return nil, 0, errors.New("not a fastcache meta file")
	}

	metaLen := binary.BigEndian.Uint32(data[4:8])
	if metaLen > 1<<20 || int64(len(data)) < 8+int64(metaLen) {
		return nil, 0, fmt.Errorf("implausible meta length %d", metaLen)
	}

	var m metaHeader
	if err = gob.NewDecoder(bytes.NewReader(data[8 : 8+metaLen])).Decode(&m); err != nil {
		return nil, 0, err
	}

	return &m, int64(len(data)), nil
}

// rebuild scans the storage tree — bodies and meta sidecars live in 256
// shard directories under dir — and repopulates the index from meta
// sidecars. Expired, malformed, or orphaned files are removed.
func (ix *index) rebuild() error {
	var metas, allFiles, dirs []string

	err := filepath.WalkDir(ix.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if p != ix.dir {
				dirs = append(dirs, p)
			}
			return nil
		}
		allFiles = append(allFiles, p)
		if strings.HasSuffix(d.Name(), ".meta") {
			metas = append(metas, p)
		}
		return nil
	})
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	tracked := map[string]struct{}{}

	for _, metaPath := range metas {
		// RAM-only mode loads nothing; the orphan pass below removes any
		// files left over from a previous disk-backed configuration.
		if ix.ramOnly {
			continue
		}

		bodyPath := strings.TrimSuffix(metaPath, ".meta")

		m, metaSize, err := readMeta(metaPath)
		if err != nil {
			_ = os.Remove(metaPath)

			continue
		}

		info, err := os.Stat(bodyPath)
		if err != nil {
			_ = os.Remove(metaPath)

			continue
		}

		if now > m.StaleAt {
			_ = os.Remove(metaPath)
			_ = os.Remove(bodyPath)

			continue
		}

		fullKey := m.Key
		if m.VaryKey != "" {
			fullKey = m.Key + "|v|" + m.VaryKey
		}

		hdr := http.Header(m.Headers)
		e := &entry{
			key:      fullKey,
			baseKey:  m.Key,
			varyH:    m.VaryHeaders,
			varyK:    m.VaryKey,
			path:     bodyPath,
			metaPath: metaPath,
			size:     info.Size(),
			fileSize: info.Size() + metaSize,
			status:   m.Status,
		}
		e.headers.Store(&hdr)
		e.storedAt.Store(m.StoredAt)
		e.expiresAt.Store(m.ExpiresAt)
		e.staleAt.Store(m.StaleAt)
		e.mustRevalidate.Store(m.MustRevalidate)
		e.seq.Store(uint64(m.StoredAt))

		ix.entries.Store(fullKey, e)
		ix.diskSize.Add(e.fileSize)
		ix.fileCount.Add(1)
		ix.scheduleExpiry(e)
		tracked[metaPath] = struct{}{}
		tracked[bodyPath] = struct{}{}

		ix.mappingMu.Lock()
		v, _ := ix.mappings.LoadOrStore(m.Key, &mapping{varyHeaders: m.VaryHeaders})
		mp := v.(*mapping)
		if len(m.VaryHeaders) > 0 && len(mp.varyHeaders) == 0 {
			mp.varyHeaders = m.VaryHeaders
		}
		mp.variants.Store(m.VaryKey, e)
		ix.mappingMu.Unlock()
	}

	// Remove unreferenced fastcache files, then the now empty shard
	// directories. Foreign files and directories under path are left
	// alone (Remove also fails harmlessly on non-empty dirs).
	for _, p := range allFiles {
		if _, ok := tracked[p]; !ok && cacheFileRe.MatchString(filepath.Base(p)) {
			_ = os.Remove(p)
		}
	}
	for _, d := range dirs {
		if shardDirRe.MatchString(filepath.Base(d)) {
			_ = os.Remove(d)
		}
	}

	// A restored index may already exceed a budget — no store will signal
	// the eviction worker otherwise.
	ix.maybeEvict()

	return nil
}

// refresh304 merges a 304 response into a stored entry (RFC 9111 §4.3.4):
// the 304's header fields replace same-named stored fields, freshness
// deadlines are recomputed, and the meta sidecar is rewritten so the update
// survives a restart. The body file is untouched.
func (ix *index) refresh304(e *entry, h http.Header) {
	old := *e.headers.Load()
	merged := make(http.Header, len(old)+len(h))
	for k, vv := range old {
		merged[k] = vv
	}
	for k, vv := range cacheableHeaders(h) {
		merged[k] = vv
	}
	e.headers.Store(&merged)

	now := time.Now()
	if a, err := strconv.Atoi(strings.TrimSpace(merged.Get("Age"))); err == nil && a > 0 {
		now = now.Add(-time.Duration(a) * time.Second)
	}

	fresh := freshness(ix, merged)
	e.storedAt.Store(now.UnixNano())
	e.expiresAt.Store(now.Add(fresh).UnixNano())
	e.staleAt.Store(now.Add(fresh + max(ix.swr, ix.sie)).UnixNano())

	cc := responseCC(merged)
	_, mr := cc["must-revalidate"]
	if _, ok := cc["proxy-revalidate"]; ok {
		mr = true
	}
	e.mustRevalidate.Store(mr)

	ix.scheduleExpiry(e)
	ix.persistMeta(e)
}

// persistMeta rewrites an entry's meta sidecar after an in-place update.
// No-op for RAM-only entries.
func (ix *index) persistMeta(e *entry) {
	if ix.ramOnly || e.metaPath == "" {
		return
	}

	tmp := e.metaPath + ".tmp"
	mf, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return
	}

	_, werr := writeMeta(mf, e.meta())
	_ = mf.Close()

	if werr != nil {
		_ = os.Remove(tmp)
		return
	}

	_ = os.Rename(tmp, e.metaPath)
}

// purge removes the whole variant set for a base key; reason feeds the
// evictions_total metric ("purge" for explicit/admin purges, "invalidate"
// for unsafe-method invalidation).
func (ix *index) purge(base, reason string) int {
	// Bump the generation first: a capture that began before this purge
	// must not commit the just-purged content afterwards. One counter for
	// all keys — a purge may also abort an unrelated in-flight store,
	// which merely costs a re-fetch.
	ix.purgeSeq.Add(1)

	v, ok := ix.mappings.Load(base)
	if !ok {
		return 0
	}

	m := v.(*mapping)

	var toDelete []*entry

	m.variants.Range(func(_, ev interface{}) bool {
		toDelete = append(toDelete, ev.(*entry))

		return true
	})

	for _, e := range toDelete {
		if ix.deleteEntry(e) {
			ix.metrics.evict(reason)
		}
	}

	return len(toDelete)
}

// baseKey is the cache key before the vary suffix: host + requestURI, or
// the configured cache_key placeholder template when set.
func baseKey(r *http.Request) string {
	return r.Host + r.RequestURI
}

func (ix *index) baseKey(r *http.Request) string {
	if ix.keyTmpl == "" {
		return baseKey(r)
	}

	repl, _ := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	if repl == nil {
		// No caddy replacer (unit tests, non-caddy callers).
		return baseKey(r)
	}

	return repl.ReplaceKnown(ix.keyTmpl, "")
}

// varyKey computes the variant suffix from the request's vary-relevant
// headers (only those in the entry's stored vary list). Components are
// length-prefixed — header values may legally contain '=' and ';', so a
// plain name=value join could collide two different header sets onto one
// variant. All values of a repeated header are folded per RFC 9110 §5.3.
func varyKey(r *http.Request, headers []string) string {
	if len(headers) == 0 {
		return ""
	}

	var b strings.Builder
	for _, h := range headers {
		fmt.Fprintf(&b, "%d:%s", len(h), h)
		v := strings.Join(r.Header.Values(h), ",")
		fmt.Fprintf(&b, "%d:%s;", len(v), v)
	}

	return b.String()
}
