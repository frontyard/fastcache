# fastcache

[![CI](https://github.com/frontyard/fastcache/actions/workflows/ci.yml/badge.svg)](https://github.com/frontyard/fastcache/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/frontyard/fastcache/branch/main/graph/badge.svg)](https://codecov.io/gh/frontyard/fastcache)

A standalone Caddy HTTP handler (`http.handlers.fastcache`, Caddyfile directive
`fastcache`) that caches GET/HEAD responses in a two-tier store:

- a **RAM tier** bounded by `memory_size`, holding small bodies and serving
  hits with a single `Write`. Only bodies up to `promote_below` (default
  16KB) are promoted — for larger bodies `sendfile` from the kernel page
  cache beats a Go-heap copy to the socket by ~25% (measured), while under
  ~16KB the open/sendfile syscall overhead dominates and the RAM copy wins.
  This is why nginx needs no RAM tier: the page cache already is one.
- a **disk tier** bounded by `disk_size`, serving bodies from a plain `*os.File`
  so the kernel can `sendfile`/`splice` them. Set `disk_size 0` for RAM-only
  mode: nothing is written to disk and nothing survives a restart — useful for
  a small hot set (a small site). Bodies larger than
  `memory_size` are then uncacheable, and RAM pressure evicts whole entries
  instead of demoting them to disk. Conversely `memory_size 0` gives a
  disk-only cache: bodies never occupy Go heap, and hits still ride the
  kernel page cache via `sendfile` — suited to working sets far larger than
  RAM. Setting both to `0` stores nothing (a pass-through).

The index lives in memory and is rebuilt from on-disk metadata at startup, so
the cache survives restarts.

## Why

`caddy + souin` benchmarks around 40–50k req/s where nginx
`proxy_cache` does ~200–240k. Storage contributes <1% of request latency — the
gap is per-hit middleware machinery, and the souin `Storer` interface returns
`[]byte`, which makes sendfile impossible. This module owns the whole request
path instead: lock-free hits, RAM-resident bodies, disk bodies served through
`ServeContent`/`io.Copy` on a real file.

## Building Caddy with fastcache

Caddy plugins compile into the binary — there is no dynamic loading. Use
[`xcaddy`](https://github.com/caddyserver/xcaddy), which writes a `main` that
imports the plugin and runs `go build` (Go ≥ the version in `go.mod`
required):

```sh
go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest

# tagged release / module path
xcaddy build --with github.com/frontyard/fastcache

# or from a local checkout (same syntax a go.mod replace uses)
xcaddy build --with github.com/frontyard/fastcache=/path/to/fastcache
```

This produces a `./caddy` binary. Verify the module and the admin API are in:

```sh
./caddy list-modules | grep fastcache
#   http.handlers.fastcache
#   admin.api.fastcache
```

The module self-registers via `init()` — nothing else is needed in the
binary. For the JSON config (no Caddyfile), the handler module ID is
`http.handlers.fastcache` inside an app's `routes`/`handle` chain; the
`fastcache` directive maps onto it.

Docker: the official builder image already ships Go + xcaddy:

```dockerfile
FROM caddy:builder AS builder
RUN xcaddy build --with github.com/frontyard/fastcache

FROM caddy
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
```

## Usage

```caddyfile
http://localhost:9083 {
    route {
        fastcache {
            path                     /var/cache/fastcache
            memory_size              64MB
            disk_size                1GB
            ttl                      1h
            stale_while_revalidate   30s
            max_items                100000
            vary                     Accept-Encoding
            purge_allow              10.0.0.0/8
        }
        reverse_proxy localhost:9081
    }
}
```

`fastcache` must sit inside a `route` block (or otherwise before) the upstream
handler — on a miss it wraps the response writer and tees the upstream response
into the cache while streaming it to the client.

### Options

| Option                        | Default                | Meaning                                                                                                                                                                                  |
|-------------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`                        | `/var/cache/fastcache` | Directory for body files + `.meta` sidecars                                                                                                                                              |
| `name`                        | none                   | Instance name for the `name` metric label; unnamed instances aggregate under `name=""`                                                                                                   |
| `memory_size`                 | `64MB`                 | RAM body-tier budget (humanize sizes or bytes); `0` = disk-only mode                                                                                                                     |
| `disk_size`                   | `1GB`                  | Disk-tier budget; `0` = RAM-only mode (no files, no persistence)                                                                                                                         |
| `max_items`                   | unlimited              | Cap on cached entries                                                                                                                                                                    |
| `ttl`                         | `1h`                   | Fallback freshness when the response carries no `max-age`/`s-maxage`/`Expires`                                                                                                           |
| `stale_while_revalidate`      | `0` (off)              | Serve an expired entry while one background refresh runs                                                                                                                                 |
| `stale_if_error`              | `0` (off)              | Retain expired entries this long as upstream-failure fallback (see below)                                                                                                                |
| `revalidate_timeout`          | `30s`                  | Bound on any single upstream revalidation (`0` = unbounded)                                                                                                                              |
| `cache_key`                   | `{host}{requestURI}`   | Cache key as a caddy placeholder template, e.g. `{http.request.host}{http.request.uri.path}`                                                                                             |
| `match_header`                | none                   | Store only responses whose header `<name>` equals one of the given values (repeatable)                                                                                                   |
| `vary`                        | none                   | Whitelist of `Vary` header names to key on                                                                                                                                               |
| `disable_touch_on_hit`        | off                    | Skip the LRU timestamp update on hits (rarely needed — the touch is a coarse atomic write)                                                                                               |
| `purge_allow`                 | loopback only          | IPs/CIDRs allowed to send `PURGE`                                                                                                                                                        |
| `honor_request_cache_control` | off                    | Honor client `no-cache`/`no-store`/`Pragma` as a bypass (default ignores them, nginx-style)                                                                                              |
| `promote_below`               | `16KB`                 | Only bodies up to this size promote to the RAM tier; larger ones always serve via sendfile. `0` = never promote                                                                          |
| `store_set_cookie`            | off                    | Store responses carrying `Set-Cookie`. Off by default: a stored cookie is replayed to every later client (session fixation). Only enable for upstreams whose Set-Cookie is safe to share |

`path` must be a dedicated directory per `fastcache` block — the cache owns
its contents (rebuild removes orphaned fastcache files; foreign files are
left alone but don't share the directory anyway). Two blocks pointing at the
same `path` index and evict each other's entries; the second one logs a
warning at provision.

### Examples

```caddyfile
# Key on path + one query param, ignoring all others
cache_key {http.request.host}{http.request.uri.path}?lang={query.lang}

# Cache only responses the upstream opted into
match_header X-Cacheable yes true

# One variant per compression scheme and device class
vary Accept-Encoding X-Device-Class

# Purge allowed from a private range plus one admin host
purge_allow 10.0.0.0/8 192.168.1.10

# RAM-only: small hot set, nothing on disk, cold after restart
disk_size 0

# Disk-only: bodies never occupy Go heap; every hit sendfiles
memory_size 0

# Restore always-promote behavior (promotion limit above the RAM budget)
promote_below 1GB
```

Each `fastcache` block is an independent instance, so tiers can be split
per URL shape — small responses take the RAM path while large ones stay
on sendfile:

```caddyfile
https://example.com {
    route /live/*.m3u8 {
        fastcache {
            path        /var/cache/fc-playlists
            memory_size 32MB
            disk_size   0           # playlists live in RAM only
            ttl         10s
        }
        reverse_proxy origin:8000
    }
    route /live/* {
        fastcache {
            path        /var/cache/fc-segments
            memory_size 0           # segments serve via sendfile only
            disk_size   50GB
        }
        reverse_proxy origin:8000
    }
}
```

### PURGE

`PURGE <uri>` evicts all variants of a key. Without `purge_allow` only loopback
clients may purge; other peers get `403` with `Cache-Status: fastcache;
fwd=request; detail=purge-denied`. The check runs against `RemoteAddr` —
when Caddy sits behind a reverse proxy, allowlist the proxy's CIDR. Responds
`200` with `Cache-Status: fastcache; purged`, or `404` with `Cache-Status:
fastcache; fwd=purge-miss` when nothing was stored. A PURGE also aborts any
still-in-flight upstream capture for that key, so purged content cannot be
re-committed by a racing store.

A richer purge surface is exposed on caddy's **admin endpoint** (module
`admin.api.fastcache`, inheriting the admin listener's access control —
loopback by default):

- `GET /fastcache/keys` — list all cached base keys across instances, sorted
- `DELETE /fastcache/purge` with `{"pattern": "<RE2>"}` — evict every base
  key matching the regex (all variants), e.g. `{"pattern": "/static/"}`.
  The request body is capped at 1MB and `pattern` must be a valid RE2.

### Stale modes

`stale_while_revalidate <dur>` and `stale_if_error <dur>` combine into one
retention window: an entry lives until `expiry + max(swr, sie)`.

- Inside the SWR window, stale hits are served immediately and one
  coalesced background refresh runs.
- Past it (SIE-only window), hits synchronously revalidate — but if upstream
  fails (handler error, or ≥500 status), the stale entry is served instead.
  Concurrent SIE hits coalesce: one revalidation, followers get the stale
  entry — nginx `proxy_cache_use_stale updating` semantics, i.e. followers
  get the stale response *before* the revalidation's outcome is known rather
  than only on actual failure.
- Revalidation is **conditional**: stored `ETag`/`Last-Modified` validators
  are sent as `If-None-Match`/`If-Modified-Since`, and a `304` merges the new
  headers and freshness into the entry in place (RFC 9111 §4.3.4) — no body
  re-fetch. A 304 only merges when it answers the entry's own validators or
  a client `If-None-Match` naming the stored ETag; a bare `*` proves a
  representation exists upstream, not that ours is current, so it is never
  merged.
- Every revalidation — background refresh or synchronous — is bounded by
  `revalidate_timeout` on a context detached from the triggering request,
  so a hung upstream can't leak the goroutine or pin the coalescing flight.
- `must-revalidate`/`proxy-revalidate`/`s-maxage` entries are never served
  stale: no SWR grace, no SIE fallback — upstream errors pass through
  (`s-maxage` implies `proxy-revalidate` for a shared cache, RFC 9111
  §5.2.2.10).

### HTTP semantics

- **Bypassed:** non-GET/HEAD methods, requests with `Authorization`,
  `Proxy-Authorization`, `Upgrade`, and (only with
  `honor_request_cache_control`) client `Cache-Control:
  no-cache`/`no-store`/`Pragma: no-cache` (matched per-token, so
  `Pragma: foo, no-cache` still bypasses). Unsafe methods additionally
  **invalidate** the request URI's cached variants on a non-error (<400)
  upstream response, plus the variants of any `Location`/`Content-Location`
  URIs the response points at (RFC 9111 §4.4).
- **Stored:** responses to GET/HEAD that are cacheable under the shared-cache
  rules and carry a heuristically cacheable status (RFC 9111 §3): `200, 203,
  204, 300, 301, 308, 404, 405, 410, 414, 501`. `206` is deliberately excluded —
  a partial body must never become a stored variant. Upstream `no-store`,
  `private`, `no-cache`, `Vary: *`, and `Set-Cookie` are respected (the last
  unless `store_set_cookie` is set). A response `no-cache` is never stored —
  it forbids reuse without successful revalidation, which the index has no
  entry to hang on. Responses with non-positive freshness
  (`max-age=0`, a past `Expires`) are never stored — so they get no
  stale_if_error fallback either. OPTIONS/TRACE bypass the cache without
  invalidating; only GET responses populate entries (HEAD hits are served
  from them).
- **Targeted Cache-Control** (RFC 9213): `Fastcache-Cache-Control` overrides
  `CDN-Cache-Control`, which overrides `Cache-Control` — an origin can give
  this cache its own TTL/policy without affecting browsers.
- **Freshness:** effective `s-maxage` (shared-cache override) → `max-age` →
  `Expires` → `Last-Modified` heuristic (10% of object age, capped at `ttl`)
  → configured `ttl`. An upstream `Age` header is folded into the stored
  timestamps, so freshness is not overextended.
- **match_header** rules gate storage on *response* headers — `match_header
  Content-Type image/png image/jpeg` caches only those content types. At
  least one value is required. (Request-side matching — paths, methods —
  belongs to caddy's own route matchers; just place `fastcache` inside a
  `handle` block.)
- **cache_key** accepts caddy placeholders resolved per request, e.g.
  `cache_key {http.request.host}{http.request.uri.path}` to ignore the query
  string, or add `{http.request.method}`/header values to split entries.
- **Conditionals:** `If-None-Match` uses weak comparison (`W/"x"` matches
  `"x"`); `If-Match` uses strong comparison — a weak stored ETag satisfies
  nothing, `*` matches any stored entity, and a mismatch is a `412`.
  `Range` requests on hits are honored via `http.ServeContent`. The `Age`
  header is set before conditional evaluation, so 304s carry it too.
- **Hop-by-hop fields:** the static list (`Connection`, `Keep-Alive`,
  `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`,
  `Transfer-Encoding`, `Upgrade`) plus fields the upstream's `Connection`
  header nominates are stripped before the response reaches the client and
  before it is stored. A handler that hijacks the connection mid-response
  aborts the capture — the truncated body is never committed.
- **Cache-Status** (RFC 9211) reports how each response was reached:
  `hit; ttl=N`, `hit; fwd=stale` (stale serve + revalidation),
  `fwd=uri-miss; stored; key="…"` (stored miss), `fwd=request` (bypass),
  `fwd-status=N` (upstream status when stale masked an error), and
  `collapsed` on singleflight followers.

### Metrics

Collectors register into caddy's shared per-config registry in `Provision`.
Expose them with caddy's own `metrics` handler on any route:

```caddyfile
localhost:2020 {
    metrics /metrics
}
```

| Metric                          | Type    | Labels                                                                   |
|---------------------------------|---------|--------------------------------------------------------------------------|
| `fastcache_requests_total`      | counter | `name`, `result`: `hit` `stale` `miss` `bypass` `purged` `revalidated`   |
| `fastcache_revalidations_total` | counter | `name`, `result`: `ok` `not_modified` (304 merge) `error`                |
| `fastcache_evictions_total`     | counter | `name`, `reason`: `expired` `disk` `count` `memory` `purge` `invalidate` |
| `fastcache_entries`             | gauge   | `name`                                                                   |
| `fastcache_bytes`               | gauge   | `name`, `tier`: `memory` `disk`                                          |

`name` is the instance's `name` option — instances without one report
`name=""`. Label sets are bounded (no per-key labels). Multiple `fastcache`
instances in one config share the collectors: counters split per name,
same-named instances aggregate, and gauges sum live instances per name.

## Architecture

| File           | Role                                                                                                                                                                                     |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fastcache.go` | Module registration, config/Caddyfile parsing, `ServeHTTP` (bypass → hit → SWR → singleflight miss), PURGE                                                                               |
| `index.go`     | The cache index: key mappings, variant sets, RAM/disk accounting, near-LRU eviction, expiry heap + sweeper, sidecar IO, startup rebuild                                                  |
| `capture.go`   | `captureWriter` — buffers upstream headers privately (a swallowed/failed response never leaks headers), tees the body to a `.tmp` file, commits by atomic rename (body first, meta last) |
| `cacheable.go` | Request bypass, response cacheability, freshness math, `Vary` handling                                                                                                                   |
| `admin.go`     | `admin.api.fastcache` — `GET /fastcache/keys`, `DELETE /fastcache/purge` (regex)                                                                                                         |
| `metrics.go`   | Prometheus collectors registered into the config's shared registry                                                                                                                       |

### Keying and variants

A request maps to `host + requestURI` → a *mapping* → a *variant set* keyed by
the values of the whitelisted `vary` headers on the stored response. One URL can
therefore hold multiple negotiated variants; PURGE drops the whole set. If the
upstream changes its `Vary` list for a URL, the old mapping and all its variants
are invalidated and rebuilt under the new list — a stale variant key would
otherwise be unreachable forever.

### On-disk format

Each entry is a **body file plus a `.meta` sidecar** (magic `FCH2` — format
version; older-version metas are rejected and reaped at rebuild — length,
gob-encoded headers/status/timestamps) under `path/<xx>/` — a 256-way shard
derived from the key hash, so no single directory accumulates millions of
entries. The meta file is written last, so its presence marks a complete
entry — half-written bodies are orphaned and reaped on rebuild. Body
filenames are `sha256(key)[:12]-<seq>` with `seq` seeded from the wall clock,
so restarts can't collide and long URIs can't hit `ENAMETOOLONG`.
`rebuild()` walks the shard tree at startup, reloads every complete entry, and
deletes orphans and empty shard directories. Only the 2-hex shard
directories are cache-owned — foreign files and subdirectories under `path`
are left alone. Metas are bounded (≤1MiB) and validated before use, and
conflicting `Vary` schemes resolve to the first-loaded meta rather than an
unreachable index. Body and meta files are written `0640` (shard dirs
`0750`).

### Concurrency

The hit path takes no locks:

- `mapping.variants` is a `sync.Map`; `entry.body` is an `atomic.Pointer[[]byte]`
  (swap on promotion/demotion).
- `mappingMu` serializes only mapping lifecycle (create/delete-last-variant),
  so a mapping can't vanish under a concurrent commit.
- Every `entry` field is immutable after commit or atomically swappable
  (`body`, `seq`, and the timestamps/headers a 304 merge refreshes).
- RAM hits write the body slice directly; disk hits open the body file and copy
  (sendfile-eligible). A disk hit within `promote_below` and the RAM budget
  is promoted via `pread` from offset 0, with a `promoting` flag so only one
  concurrent hit does the read.
- Misses are coalesced per key by a `flights` singleflight map; followers wait
  on both completion and `r.Context()`, then loop — re-serving the new entry
  or leading/following the next flight. Waits are capped (`maxFlightRounds`),
  after which a request fetches itself — so concurrent upstream fetches stay
  bounded even when leaders keep finishing without covering the request
  (uncacheable responses under churn). Revalidations coalesce under a
  separate `reval:` flight shared by background refresh and synchronous
  revalidation: at most one upstream validation per variant is ever in
  flight.

### Eviction and expiry

- Budgets (`disk_size`, `memory_size`, `max_items`) evict lowest-`seq` first.
  `seq` is a coarse last-use timestamp, not a global counter — hits only write
  it when it moved (>1 s granularity), so the hit path doesn't bounce a shared
  cache line.
- Enforcement runs on a **dedicated eviction goroutine**: stores and RAM
  promotions add optimistically, then `maybeEvict` wakes the worker through a
  coalescing 1-slot channel when a budget is exceeded. The O(n log n) index
  scan never runs on a request goroutine; the trade-off is transient budget
  overshoot between signal and scan.
- Each pass frees to a **5% low-water mark** (`evictSlack`), amortizing the
  scan over ~5% worth of churn instead of rescanning on every store. RAM
  eviction only *demotes* — the disk copy stays servable (in RAM-only mode it
  evicts the whole entry, since there is no disk copy).
- Expiry uses a min-heap fed by a 500 ms sweeper goroutine. `Cleanup()` stops
  both background goroutines (sweeper + eviction worker) on config reload.

### Correctness under failure

`captureWriter` commits only complete responses: it discards on short/failed
downstream `Write`, handler errors, request-context cancellation, body size
mismatching the upstream `Content-Length`, and on any PURGE/invalidation that
landed while the capture was still in flight — a truncated or superseded
stream can never poison the cache.

## Benchmark results

### 64 KiB body — request-path throughput

`ab -n 20000 -c 100 -k` + `wrk -t4 -c100 -d10s --latency`, 64 KiB
incompressible body, same machine (`bench/run.sh` runs all four servers
side by side). The machine drifts ±15%, so the meaningful comparison is
the within-run ratio:

| Server                | run 1 wrk | run 2 wrk   | run 3 wrk   | run 4 wrk*  | run 5 wrk†  | run 6 wrk‡  |
|-----------------------|-----------|-------------|-------------|-------------|-------------|-------------|
| nginx proxy_cache     | 210,033   | 239,964     | 246,589     | 264,490     | 263,396     | 267,827     |
| **fastcache**         | 134,996   | 167,744     | 178,352     | 174,959     | 210,887     | **249,907** |
| caddy `file_server`   | 44,546    | 51,357      | 54,130      | 56,742      | 128,380     | 129,287     |
| souin                 | 40,461    | 49,172      | 48,606      | 55,847      | 115,455     | 115,137     |

| ratio                      | run 1 | run 2 | run 3 | run 4*  | run 5†  | run 6‡   |
|----------------------------|-------|-------|-------|---------|---------|----------|
| fastcache / nginx          | 64%   | 70%   | 72%   | 66%     | 80%     | **93%**  |
| fastcache / souin          | 3.3×  | 3.4×  | 3.7×  | 3.1×    | 1.8×    | 2.2×     |
| fastcache / file_server    | 3.0×  | 3.3×  | 3.3×  | 3.1×    | 1.6×    | 1.9×     |

\* run 4 on an idle machine (all desktop apps closed), 60s wrk legs —
runs 1–3 ran under normal desktop load, worth ±15% either way.

† run 5 on the idle machine with `GOGC=800` on both caddy processes
(stock `GOGC=100` costs ~25-30% throughput at this request rate;
`GOGC=400` gives ~198k). It lifts every Go-side server — file_server
and souin roughly doubled vs their loaded-machine numbers, compressing
their ratios. nginx is unaffected (no GC; ~264k in both clean-room runs).

‡ run 6 adds `promote_below` (default 16KB): the 64KiB test body now
exceeds the promotion threshold, so hits serve via `sendfile` from the
page cache instead of a heap `Write` — ~250k, i.e. **93% of nginx** on
a machine nginx itself tops out near 268k.

**Run provenance** (code state at each measurement; omitted reruns
measured only machine drift or a stale binary):

- run 1 — initial two-tier index + atomic/sync.Map correctness fixes
- run 2 — + hit-path micro-opts (direct slice `Write`, direct header-map
  assigns, `openBody`/`promoting` rework)
- run 3 — + async eviction worker, `s-maxage`, `stale_if_error`, admin API,
  `cache_key`, `match_header`
- run 4 — + RFC items (unsafe-method invalidation, must-revalidate, 304
  merge, Cache-Status params, heuristic freshness, targeted Cache-Control),
  Vary-change fix, sharded disk layout, metrics; first run *without*
  `disable_touch_on_hit`
- run 5 — same code as run 4, `GOGC=800`
- run 6 — + `promote_below`

Runs 1–3 were measured with `disable_touch_on_hit` in the bench Caddyfile
(a micro-optimization that skips the per-hit recency write); runs 4–6 use
the default configuration. A controlled A/B showed the flag costs nothing
measurable (the recency timestamp stores at most once per
`touchGranularity`), and a CPU profile of the hit path puts all
recently-added per-hit work — metrics counter, `Cache-Status` parameter,
atomic field loads — at ~1.5% of CPU. Run 4 reproducing run 3's ~175k on
the quiet machine confirms the apparent regression in the first
default-config run (151k, stale binary, omitted) was ambient noise,
not code.

- `ab` saturates at ~50k req/s (single-threaded client) — the wrk column is the
  measurement.
- Latency (run 6): fastcache 290µs mean / 2.06ms p99 vs nginx 244µs / 618µs.
  The residual gap is Caddy's per-request HTTP stack, not the cache.
- fastcache beats `file_server` 1.7–3.3× — a RAM hit is cheaper than per-request
  open/stat/sendfile (the ratio narrows under GOGC=400 since the file-server
  path allocates heavily too).

### 3 MiB body — the sendfile workload

Same harness, `BODY_SIZE=3145728`, idle machine, `GOGC=800`, `promote_below`
default (the 3 MiB body stays disk-backed → every hit sendfiles):

| Server                | ab req/s | wrk req/s | wrk mean | wrk p99 |
|-----------------------|----------|-----------|----------|---------|
| **fastcache**         | **2,979**| **9,004** | 6.5ms    | 15.3ms  |
| nginx proxy_cache     | 1,738    | 5,416     | 23.0ms   | 175.6ms |
| caddy `file_server`   | 2,147    | 4,313     | 25.9ms   | 135.1ms |
| souin                 | 1,613    | 2,079     | 32.6ms   | 103.3ms |

**1.7× nginx, 4.3× souin.** Once the body is large, per-request cost is the
payload copy, not middleware: souin must copy 3 MiB through Go heap on every
hit; fastcache's plain-`os.File` hit path sendfiles like nginx — minus the
shared-zone bookkeeping nginx pays per request.

## Developing

```sh
go build ./...            # module name is `fastcache`; rename to a domain path before publishing
go test -race -count=1    # ~100 tests, ~92% statement coverage, including concurrency-churn under -race
bash bench/run.sh         # end-to-end bench (see bench/README below)
```

### bench/ layout

Ports: backend **9081**, souin **9080** (optional), nginx **9082**,
fastcache **9083**, `file_server` baseline **9084**. `run.sh` builds the
backend, an xcaddy caddy with this module, optionally a souin caddy (needs a
`../storages` checkout; `WITH_SOUIN=0` skips it), and user-land nginx+wrk via
`apt download`/`dpkg -x` (no root needed). Knobs: `REQUESTS CONCURRENCY WARMUP
BODY_SIZE GO WITH_SOUIN READY_TIMEOUT BENCH_TIMEOUT NET_TIMEOUT WRK_DURATION`.
Artifacts live in `/tmp/fastcache-bench{,-cache,-www}` and are rebuilt as
needed.
