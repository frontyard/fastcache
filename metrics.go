package fastcache

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// cacheMetrics holds the module's Prometheus collectors. They are allocated
// per index in newIndex — increments work even when unregistered (tests) —
// and registered into the shared caddy registry in Provision, which is also
// what the http.handlers.metrics module serves at /metrics.
type cacheMetrics struct {
	name          string                 // the instance's `name` option; "" aggregates unnamed instances
	requests      *prometheus.CounterVec // result: hit|stale|miss|bypass|purged|revalidated
	revalidations *prometheus.CounterVec // result: ok|not_modified|error
	evictions     *prometheus.CounterVec // reason: expired|disk|count|memory|purge|invalidate

	// Resolved child counters: WithLabelValues does a map lookup under a
	// mutex per call — too expensive to run on every hit. resolve() rebuilds
	// these after (re)registration.
	reqC   map[string]prometheus.Counter
	revalC map[string]prometheus.Counter
	evictC map[string]prometheus.Counter
}

func newCacheMetrics(name string) *cacheMetrics {
	m := &cacheMetrics{
		name: name,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fastcache",
			Name:      "requests_total",
			Help:      "Requests handled by the cache, by outcome.",
		}, []string{"name", "result"}),
		revalidations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fastcache",
			Name:      "revalidations_total",
			Help:      "Upstream revalidations of stale entries, by outcome (ok = fresh body stored, not_modified = 304 merge, error = upstream failure).",
		}, []string{"name", "result"}),
		evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fastcache",
			Name:      "evictions_total",
			Help:      "Entries removed from the index, by reason (expired, disk/count/memory budget, purge, invalidate).",
		}, []string{"name", "reason"}),
	}

	// Pre-create every label child so a scrape always shows the families,
	// even before the first increment.
	m.resolve()

	return m
}

// resolve (re)builds the resolved child counters for the current vecs.
// Called by newCacheMetrics and again by register, since a reused
// already-registered vec replaces the CounterVec the children came from.
func (m *cacheMetrics) resolve() {
	m.reqC = map[string]prometheus.Counter{}
	for _, r := range []string{"hit", "stale", "miss", "bypass", "purged", "revalidated"} {
		m.reqC[r] = m.requests.WithLabelValues(m.name, r)
	}

	m.revalC = map[string]prometheus.Counter{}
	for _, r := range []string{"ok", "not_modified", "error"} {
		m.revalC[r] = m.revalidations.WithLabelValues(m.name, r)
	}

	m.evictC = map[string]prometheus.Counter{}
	for _, r := range []string{"expired", "disk", "count", "memory", "purge", "invalidate"} {
		m.evictC[r] = m.evictions.WithLabelValues(m.name, r)
	}
}

func (m *cacheMetrics) req(result string)   { m.reqC[result].Inc() }
func (m *cacheMetrics) reval(result string) { m.revalC[result].Inc() }
func (m *cacheMetrics) evict(reason string) { m.evictC[reason].Inc() }

// register attaches the collectors to the config's shared metrics registry.
// Other FastCache instances reuse the already-registered counter vecs — the
// `name` label keeps their series distinct, and same-named instances
// aggregate, as do the gauges, which sum all live instances per name.
func (m *cacheMetrics) register(reg *prometheus.Registry) {
	m.requests = registerOrReuse(reg, m.requests)
	m.revalidations = registerOrReuse(reg, m.revalidations)
	m.evictions = registerOrReuse(reg, m.evictions)
	m.resolve()

	name := m.name

	registerOrReuse(reg, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   "fastcache",
		Name:        "entries",
		Help:        "Currently indexed cache entries (all instances of the name).",
		ConstLabels: prometheus.Labels{"name": name},
	}, func() float64 { return float64(sumIndexes(name, func(ix *index) int64 { return ix.fileCount.Load() })) }))

	registerOrReuse(reg, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   "fastcache",
		Name:        "bytes",
		Help:        "Bytes held by the cache tiers (all instances of the name).",
		ConstLabels: prometheus.Labels{"name": name, "tier": "memory"},
	}, func() float64 { return float64(sumIndexes(name, func(ix *index) int64 { return ix.memSize.Load() })) }))

	registerOrReuse(reg, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   "fastcache",
		Name:        "bytes",
		Help:        "Bytes held by the cache tiers (all instances of the name).",
		ConstLabels: prometheus.Labels{"name": name, "tier": "disk"},
	}, func() float64 { return float64(sumIndexes(name, func(ix *index) int64 { return ix.diskSize.Load() })) }))
}

// sumIndexes totals fn over the index of every live instance with the
// given name ("" sums the unnamed instances).
func sumIndexes(name string, fn func(*index) int64) int64 {
	var total int64
	liveCaches.Range(func(k, _ any) bool {
		if ix := k.(*FastCache).idx; ix != nil && ix.name == name {
			total += fn(ix)
		}
		return true
	})
	return total
}

// registerOrReuse registers c, reusing an existing identical collector when
// this registry already has one (multiple FastCache instances share a
// registry). On any other error the collector stays unregistered.
func registerOrReuse[T prometheus.Collector](reg *prometheus.Registry, c T) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
		}
	}
	return c
}
