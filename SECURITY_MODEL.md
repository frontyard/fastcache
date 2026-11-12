# Security model

This document describes who and what fastcache defends against. It is the
threat model security reviews and code scans start from.

fastcache is an open-source Caddy module (`http.handlers.fastcache` plus
`admin.api.fastcache`) that operators compile into their own Caddy build
and self-host. This model describes a typical operator deployment.

## Trust boundaries

1. **Internet client → Caddy → fastcache**: every request reaching the
   handler is untrusted, including `PURGE` (gated by `purge_allow` on
   `RemoteAddr`, loopback-only by default).
2. **fastcache → upstream handler** (usually `reverse_proxy`): upstream
   responses (headers, bodies, `Vary`, `Cache-Control`, `Set-Cookie`,
   `Location`, 304 replies) are semi-trusted. A malicious or compromised
   upstream is in scope.
3. **Caddy admin listener**: `GET /fastcache/keys` and
   `DELETE /fastcache/purge` inherit the admin listener's access control
   (loopback by default).
4. **Process → cache directory** (`path`, default `/var/cache/fastcache`):
   body files and gob-encoded `.meta` sidecars under 256 shard directories,
   written `0640` (directories `0750`) and reloaded at startup.

The operator and the Caddyfile/JSON config are trusted. Config-dependent
issues are still in scope where a plausible config (for example multiple
hosts or schemes sharing one instance with the default `cache_key`)
silently weakens a guarantee.

## Components

| Component | Code |
| --- | --- |
| Cache core | `fastcache.go` (ServeHTTP, PURGE, revalidation), `index.go` (keys, variants, eviction, on-disk format, rebuild), `capture.go` (capture and atomic commit), `cacheable.go` (bypass, cacheability, freshness), `metrics.go` |
| Admin API | `admin.go`: `GET /fastcache/keys`, `DELETE /fastcache/purge` (RE2 pattern) |

Tests and `bench/` are in scope too.

## Assets

All of these are critical:

- **Cache integrity**: no poisoning, and no response belonging to one user
  served to another (`Authorization`, `Set-Cookie`, `Vary`, key collisions,
  304 header merges).
- **Request and header integrity** toward the upstream and the client
  (hop-by-hop stripping, conditional and range handling).
- **Availability**, and the origin shielding the cache provides.
- The cached data on disk.

## Attackers

All of these are in scope:

- Unauthenticated internet clients.
- Authenticated clients trying to read other users' cached responses.
- Malicious or compromised upstream servers.
- Anyone on the network who can reach the Caddy admin listener.
- Local users on the same host (cache files on disk).

## Existing controls

- Requests carrying `Authorization`, `Proxy-Authorization` or `Upgrade`
  bypass the cache.
- `Set-Cookie` responses are not stored unless `store_set_cookie` is set,
  and `206` is never stored.
- Hop-by-hop fields, plus any nominated by `Connection`, are stripped
  before storing and serving.
- Upstream headers are buffered privately and entries commit atomically
  (body first, meta last). A purge racing a capture aborts the commit.
- `.meta` files are capped at 1 MiB and validated before use. Admin purge
  bodies are capped at 1 MB and `pattern` must be valid RE2.

## Out of scope

Nothing is ruled out. Denial of service, config-dependent weaknesses,
admin-reachable issues and test code are all in scope.
