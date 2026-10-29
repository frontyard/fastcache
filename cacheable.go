package fastcache

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// hopHeaders are connection-scoped headers never stored nor replayed to
// the client (RFC 9110 §7.6.1). Content-Length is deliberately absent: it
// is hop-by-hop only for storage (recomputed on serve) but must forward
// intact on a live upstream response.
var hopHeaders = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// requestBypass reports whether the request must skip the cache read path.
func requestBypass(ix *index, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return true
	}

	// Proxy-Authorization is hop-by-hop proxy credentials: like
	// Authorization, its response must not become a shared entry.
	if r.Header.Get("Authorization") != "" || r.Header.Get("Proxy-Authorization") != "" {
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

	for tok := range strings.SplitSeq(r.Header.Get("Pragma"), ",") {
		if strings.EqualFold(strings.TrimSpace(tok), "no-cache") {
			return true
		}
	}

	return false
}

// responseCC returns the effective response cache directives as a map.
// RFC 9213 targeted fields take precedence: Fastcache-Cache-Control beats
// CDN-Cache-Control beats Cache-Control — a present targeted field replaces
// Cache-Control entirely for this cache.
func responseCC(h http.Header) map[string]string {
	// Repeated field lines are equivalent to one comma-joined line (RFC
	// 9110 §5.3) — fold them before parsing directives.
	cc := strings.Join(h.Values("Fastcache-Cache-Control"), ",")
	if cc == "" {
		cc = strings.Join(h.Values("CDN-Cache-Control"), ",")
	}
	if cc == "" {
		cc = strings.Join(h.Values("Cache-Control"), ",")
	}
	if cc == "" {
		return nil
	}

	m := make(map[string]string)
	for dir := range strings.SplitSeq(cc, ",") {
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
		if !slices.Contains(vals, h.Get(name)) {
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

	// A stored Set-Cookie is replayed to every later client — session
	// fixation. Never store it unless the operator opted in.
	if !ix.storeCookie && len(h.Values("Set-Cookie")) > 0 {
		return 0, 0, nil, false, false
	}

	mr = mustRevalidate(cc)

	vary := strings.Join(h.Values("Vary"), ",")
	if vary != "" {
		allowed := map[string]bool{}
		for _, v := range ix.varyList {
			allowed[http.CanonicalHeaderKey(v)] = true
		}

		for v := range strings.SplitSeq(vary, ",") {
			name := http.CanonicalHeaderKey(strings.TrimSpace(v))
			if name == "" {
				continue
			}

			if name == "*" {
				return 0, 0, nil, false, false
			}

			if !allowed[name] {
				// Vary on a header outside the whitelist → uncacheable.
				return 0, 0, nil, false, false
			}

			varyHeaders = append(varyHeaders, name)
		}
	}

	// Canonicalize the list: the stored mapping compares it by equality,
	// so an upstream that orders Vary differently per response must not
	// look like a scheme change.
	varyHeaders = slices.Compact(slices.Sorted(slices.Values(varyHeaders)))

	fresh = freshness(ix, h)

	return fresh, ix.swr, varyHeaders, mr, true
}

// freshness computes the response TTL from the effective Cache-Control
// s-maxage/max-age (s-maxage wins for shared caches, RFC 9111 §5.2.2.10),
// then Expires, then a Last-Modified heuristic (§4.2.2: 10% of the object's
// age, capped at the default TTL), then the configured default.
func freshness(ix *index, h http.Header) time.Duration {
	if cc := responseCC(h); len(cc) > 0 {
		// The no-cache response directive forbids reuse without successful
		// revalidation (RFC 9111 §5.2.2.4) — zero freshness is never stored,
		// and a 304 merge carrying it turns the entry instantly stale.
		if _, ok := cc["no-cache"]; ok {
			return 0
		}

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

// mustRevalidate reports whether the response directives forbid serving
// the stored response stale: must-revalidate, proxy-revalidate, or
// s-maxage — which implies proxy-revalidate for a shared cache
// (RFC 9111 §5.2.2.10).
func mustRevalidate(cc map[string]string) bool {
	if _, ok := cc["must-revalidate"]; ok {
		return true
	}
	if _, ok := cc["proxy-revalidate"]; ok {
		return true
	}
	_, ok := cc["s-maxage"]

	return ok
}

// connHeaderTokens returns the field names a response nominates as
// hop-by-hop via its Connection header (RFC 9110 §7.6.1).
func connHeaderTokens(h http.Header) map[string]bool {
	var out map[string]bool

	for _, line := range h.Values("Connection") {
		for tok := range strings.SplitSeq(line, ",") {
			if name := http.CanonicalHeaderKey(strings.TrimSpace(tok)); name != "" {
				if out == nil {
					out = map[string]bool{}
				}

				out[name] = true
			}
		}
	}

	return out
}

// cacheableHeaders returns the headers to persist: hop-by-hop fields,
// Connection-nominated fields, and trailer-value keys are all excluded.
func cacheableHeaders(h http.Header) http.Header {
	connToks := connHeaderTokens(h)
	out := make(http.Header, len(h))

	for k, vv := range h {
		ck := http.CanonicalHeaderKey(k)
		if hopHeaders[ck] || ck == "Content-Length" || connToks[ck] || strings.HasPrefix(k, http.TrailerPrefix) {
			continue
		}

		cp := make([]string, len(vv))
		copy(cp, vv)
		out[k] = cp
	}

	return out
}
