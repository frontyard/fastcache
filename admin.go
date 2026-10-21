package fastcache

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync"

	"github.com/caddyserver/caddy/v2"
)

// liveCaches tracks provisioned FastCache instances so the admin API module
// can reach them — request-path handlers are not otherwise addressable.
var liveCaches sync.Map // *FastCache -> struct{}

func init() {
	caddy.RegisterModule(adminAPI{})
}

// adminAPI exposes cache inspection and bulk purge on caddy's admin
// endpoint as module admin.api.fastcache. Access control is inherited from
// the admin listener (loopback-only by default).
type adminAPI struct{}

func (adminAPI) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "admin.api.fastcache",
		New: func() caddy.Module { return new(adminAPI) },
	}
}

// Routes implements caddy.AdminRouter.
func (adminAPI) Routes() []caddy.AdminRoute {
	return []caddy.AdminRoute{
		{Pattern: "/fastcache/keys", Handler: caddy.AdminHandlerFunc(listCacheKeys)},
		{Pattern: "/fastcache/purge", Handler: caddy.AdminHandlerFunc(purgeCacheKeys)},
	}
}

// listCacheKeys answers GET /fastcache/keys with every cached base key
// (host + requestURI, or the resolved cache_key) across all instances.
func listCacheKeys(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: fmt.Errorf("method not allowed")}
	}

	var keys []string
	instances := 0

	liveCaches.Range(func(k, _ any) bool {
		instances++

		fc := k.(*FastCache)
		fc.idx.mappings.Range(func(kk, _ any) bool {
			keys = append(keys, kk.(string))
			return true
		})

		return true
	})

	if keys == nil {
		keys = []string{}
	}

	w.Header().Set("Content-Type", "application/json")

	return json.NewEncoder(w).Encode(map[string]any{"instances": instances, "keys": keys})
}

// purgeCacheKeys answers DELETE /fastcache/purge with a JSON body
// {"pattern": "<RE2>"}: every cached base key matching the pattern is
// evicted (all variants). Returns the number of variants removed.
func purgeCacheKeys(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodDelete {
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: fmt.Errorf("method not allowed")}
	}

	var payload struct {
		Pattern string `json:"pattern"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return caddy.APIError{HTTPStatus: http.StatusBadRequest, Err: err}
	}

	re, err := regexp.Compile(payload.Pattern)
	if err != nil {
		return caddy.APIError{HTTPStatus: http.StatusBadRequest, Err: fmt.Errorf("invalid pattern: %w", err)}
	}

	purged := 0

	liveCaches.Range(func(k, _ any) bool {
		fc := k.(*FastCache)

		var bases []string
		fc.idx.mappings.Range(func(kk, _ any) bool {
			if base := kk.(string); re.MatchString(base) {
				bases = append(bases, base)
			}

			return true
		})

		for _, base := range bases {
			purged += fc.idx.purge(base, "purge")
		}

		return true
	})

	w.Header().Set("Content-Type", "application/json")

	return json.NewEncoder(w).Encode(map[string]int{"purged": purged})
}

var _ caddy.AdminRouter = adminAPI{}
