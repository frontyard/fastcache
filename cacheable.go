package fastcache

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// hopHeaders are connection-scoped headers never stored nor replayed.
var hopHeaders = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Content-Length":      true, // recomputed on serve
}

// requestBypass reports whether the request must skip the cache read path.
func requestBypass(ix *index, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return true
	}

	if r.Header.Get("Authorization") != "" {
		return true
	}

	if r.Header.Get("Upgrade") != "" {
		return true
	}

	// Request Cache-Control/Pragma is only honored when configured —
	// otherwise any client can force-bypass the cache and hammer upstream.
	if !ix.honorReqCC {
		return false
	}

	cc := r.Header.Get("Cache-Control")
	if strings.Contains(cc, "no-cache") || strings.Contains(cc, "no-store") {
		return true
	}

	if strings.EqualFold(r.Header.Get("Pragma"), "no-cache") {
		return true
	}

	return false
}

// responseCC returns the effective response cache directives as a map.
// RFC 9213 targeted fields take precedence: Fastcache-Cache-Control beats
// CDN-Cache-Control beats Cache-Control — a present targeted field replaces
// Cache-Control entirely for this cache.
func responseCC(h http.Header) map[string]string {
	cc := h.Get("Fastcache-Cache-Control")
	if cc == "" {
		cc = h.Get("CDN-Cache-Control")
	}
	if cc == "" {
		cc = h.Get("Cache-Control")
	}
	if cc == "" {
		return nil
	}

	m := make(map[string]string)
	for _, dir := range strings.Split(cc, ",") {
		dir = strings.TrimSpace(dir)
		if k, v, ok := strings.Cut(dir, "="); ok {
			m[strings.ToLower(strings.TrimSpace(k))] = strings.Trim(strings.TrimSpace(v), `"`)
		} else if dir != "" {
			m[strings.ToLower(dir)] = ""
		}
	}

	return m
}

// responseCacheable decides whether a response may be stored, and computes
// its freshness deadlines. mr reports whether the response forbids stale
// serving (must-revalidate/proxy-revalidate).
func responseCacheable(ix *index, r *http.Request, status int, h http.Header) (fresh, stale time.Duration, varyHeaders []string, mr, ok bool) {
	if r.Method != http.MethodGet {
		return 0, 0, nil, false, false
	}

	// Heuristically cacheable statuses (RFC 9111 §3) minus 206: a partial
	// body must never be committed as the full variant.
	switch status {
	case 200, 203, 204, 300, 301, 308, 404, 405, 410, 414, 501:
	default:
		return 0, 0, nil, false, false
	}

	// match_header rules: when configured, every named response header must
	// carry one of the allowed values, else the response is not stored.
	for name, vals := range ix.matchHeaders {
		got := h.Get(name)
		ok := false
		for _, v := range vals {
			if got == v {
				ok = true
				break
			}
		}
		if !ok {
			return 0, 0, nil, false, false
		}
	}

	cc := responseCC(h)
	if _, banned := cc["no-store"]; banned {
		return 0, 0, nil, false, false
	}
	if _, banned := cc["private"]; banned {
		return 0, 0, nil, false, false
	}

	if _, mr = cc["must-revalidate"]; !mr {
		_, mr = cc["proxy-revalidate"]
	}

	vary := h.Get("Vary")
	if vary == "*" {
		return 0, 0, nil, false, false
	}

	if vary != "" {
		allowed := map[string]bool{}
		for _, v := range ix.varyList {
			allowed[http.CanonicalHeaderKey(v)] = true
		}

		for _, v := range strings.Split(vary, ",") {
			name := http.CanonicalHeaderKey(strings.TrimSpace(v))
			if name == "" {
				continue
			}

			if !allowed[name] {
				// Vary on a header outside the whitelist → uncacheable.
				return 0, 0, nil, false, false
			}

			varyHeaders = append(varyHeaders, name)
		}
	}

	fresh = freshness(ix, h)

	return fresh, ix.swr, varyHeaders, mr, true
}

// freshness computes the response TTL from the effective Cache-Control
// s-maxage/max-age (s-maxage wins for shared caches, RFC 9111 §5.2.2.10),
// then Expires, then a Last-Modified heuristic (§4.2.2: 10% of the object's
// age, capped at the default TTL), then the configured default.
func freshness(ix *index, h http.Header) time.Duration {
	if cc := responseCC(h); len(cc) > 0 {
		maxAge := -1
		if v, ok := cc["s-maxage"]; ok {
			maxAge, _ = strconv.Atoi(v)
		}
		if maxAge < 0 {
			if v, ok := cc["max-age"]; ok {
				maxAge, _ = strconv.Atoi(v)
			}
		}

		if maxAge >= 0 {
			if maxAge <= 0 {
				return 0
			}

			return time.Duration(maxAge) * time.Second
		}
	}

	if exp := h.Get("Expires"); exp != "" {
		if t, err := http.ParseTime(exp); err == nil {
			if d := time.Until(t); d > 0 {
				return d
			}

			return 0
		}
	}

	if lm := h.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			if age := time.Since(t); age > 0 {
				if d := age / 10; d < ix.defaultTTL {
					return d
				}
			}
		}
	}

	return ix.defaultTTL
}

// cacheableHeaders returns the headers to persist (hop-by-hop stripped).
func cacheableHeaders(h http.Header) http.Header {
	out := make(http.Header, len(h))

	for k, vv := range h {
		if hopHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}

		cp := make([]string, len(vv))
		copy(cp, vv)
		out[k] = cp
	}

	return out
}
