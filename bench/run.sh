#!/usr/bin/env bash
# End-to-end benchmark:
#   caddy file_server (:9084, ceiling check)
#   caddy + fastcache (:9083)
#   caddy + souin + simplefs (:9080, only when ../storages exists)
#   nginx proxy_cache (:9082)
# all proxying the same 64KiB incompressible body from backend :9081.
#
# Prerequisites:
#   - go >= 1.26 (set GO= or PATH accordingly, e.g. GO=~/sdk/go/bin/go)
#   - nginx: either installed system-wide, or fetched user-land automatically
#     via `apt download` + `dpkg -x` (no sudo needed; same for wrk)
#   - a load tool: ab (apache2-utils); wrk is used too when available
#   - xcaddy:     go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest
#
# Tunables (env): REQUESTS CONCURRENCY WARMUP BODY_SIZE GO WITH_SOUIN
#                 READY_TIMEOUT (s, default 30)   readiness poll per server
#                 BENCH_TIMEOUT (s, default 300)  cap on each ab/hey/wrk call
#                 NET_TIMEOUT   (s, default 120)  cap on apt download/go install
#                 WRK_DURATION  (s, default 10)   wrk run length per server
#                 GOGC          (default 400)     GC target % for the caddy
#                             processes; at 100k+ req/s per-request garbage
#                             makes stock GOGC=100 cost ~25-30% throughput.
#                 GOMEMLIMIT    (default 16GiB)   soft heap cap per caddy
#                             process. Both are applied to both caddy
#                             servers for fairness; backend and nginx are
#                             left alone. WARNING: GOGC=off without a
#                             GOMEMLIMIT lets each caddy grow to ~0.9xRAM —
#                             two processes will OOM the machine.
#
# Note: this environment cannot reach proxy.golang.org, hence GOPROXY below.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORAGES="$(cd "$REPO_ROOT/../storages" 2>/dev/null && pwd || true)"
WORK=/tmp/fastcache-bench
WWW=/tmp/fastcache-bench-www
CACHE=/tmp/fastcache-bench-cache

PORT_BACKEND=9081
PORT_SOUIN=9080
PORT_NGINX=9082
PORT_FASTCACHE=9083
PORT_FILESERVER=9084

REQUESTS="${REQUESTS:-20000}"
CONCURRENCY="${CONCURRENCY:-100}"
WARMUP="${WARMUP:-500}"
BODY_SIZE="${BODY_SIZE:-65536}"
READY_TIMEOUT="${READY_TIMEOUT:-30}"
BENCH_TIMEOUT="${BENCH_TIMEOUT:-300}"
NET_TIMEOUT="${NET_TIMEOUT:-120}"
WRK_DURATION="${WRK_DURATION:-10}"
GOGC="${GOGC:-800}"
GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"

WITH_SOUIN="${WITH_SOUIN:-auto}"
if [ "$WITH_SOUIN" = auto ]; then
	[ -n "$STORAGES" ] && WITH_SOUIN=1 || WITH_SOUIN=0
fi

export GOWORK=off
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

# fastcache requires go >= 1.26 (go.mod). `go version` run inside a module
# dir reports the toolchain the MODULE selects, not the binary's — probe with
# GOTOOLCHAIN=local to get the real one. If no local toolchain qualifies,
# fall back to auto toolchain switching (it can fetch via GOPROXY).
TOOLCHAIN=local
pick_go() {
	local cand v
	for cand in ${GO:+$GO} go "$HOME/sdk/go/bin/go" /usr/local/go/bin/go; do
		v=$(GOTOOLCHAIN=local "$cand" version 2>/dev/null | \
			grep -oE 'go[0-9]+\.[0-9]+' | head -1 | tr -d 'go' || true)
		[ -n "$v" ] || continue
		if [ "${v%%.*}" -ge 2 ] || [ "${v#*.}" -ge 26 ]; then
			GO="$cand"
			return 0
		fi
	done
	return 1
}
if ! pick_go; then
	GO=go
	TOOLCHAIN=auto
	echo "no local go >= 1.26 found; falling back to toolchain auto-switch" >&2
fi
echo "go: $(GOTOOLCHAIN=local "$GO" version) (GOTOOLCHAIN=$TOOLCHAIN)"

PIDS=()
cleanup() {
	for pid in "${PIDS[@]:-}"; do
		kill "$pid" 2>/dev/null || true
	done
	# In case nginx daemonized anyway, stop it via its pidfile too.
	for pidfile in "$WORK"/nginx/*.pid; do
		[ -f "$pidfile" ] && kill "$(cat "$pidfile")" 2>/dev/null || true
	done
	# Reap tracked children so none are left as zombies.
	wait 2>/dev/null || true
}
trap cleanup EXIT

say() { printf '\n=== %s ===\n' "$*"; }

have() { command -v "$1" >/dev/null 2>&1; }

# Wait until $port accepts TCP connections or READY_TIMEOUT elapses. If the
# owning process ($3, optional pid) dies first, bail immediately and dump the
# tail of every server log so the failure is visible instead of hanging.
wait_port() {
	local name=$1 port=$2 pid=${3:-} deadline=$((SECONDS + READY_TIMEOUT))
	while ! (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
		if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
			echo "ERROR: $name exited before listening on :$port" >&2
			tail -n 20 "$WORK"/*.log "$WORK"/*.out "$WORK"/nginx/*error*.log 2>/dev/null >&2
			exit 1
		fi
		if [ "$SECONDS" -ge "$deadline" ]; then
			echo "ERROR: $name not listening on :$port after ${READY_TIMEOUT}s" >&2
			tail -n 20 "$WORK"/*.log "$WORK"/*.out "$WORK"/nginx/*error*.log 2>/dev/null >&2
			exit 1
		fi
		sleep 0.2
	done
	echo "$name listening on :$port"
}

# fetch_pkg <name> — user-land .deb download+extract into $WORK/pkg/<name>,
# no sudo needed.
fetch_pkg() {
	mkdir -p "$WORK/pkg/$1"
	(cd "$WORK/pkg/$1" && timeout "$NET_TIMEOUT" apt download "$1" >/dev/null \
		&& dpkg -x "$1"_*.deb .)
}

say "building backend"
mkdir -p "$WORK"
(cd "$REPO_ROOT/bench" && GOTOOLCHAIN="$TOOLCHAIN" "$GO" build -o "$WORK/backend" ./backend)

# One caddy build covers both fastcache (:9083) and file_server (:9084).
if [ ! -x "$WORK/caddy" ]; then
	if ! have xcaddy && [ ! -x "$("$GO" env GOPATH)/bin/xcaddy" ]; then
		echo "xcaddy not found, installing…"
		timeout "$NET_TIMEOUT" env GOTOOLCHAIN="$TOOLCHAIN" "$GO" install \
			github.com/caddyserver/xcaddy/cmd/xcaddy@latest
	fi
	XCADDY=xcaddy
	have xcaddy || XCADDY="$("$GO" env GOPATH)/bin/xcaddy"

	say "building caddy with fastcache"
	(cd "$WORK" && PATH="$(dirname "$GO"):$PATH" GOTOOLCHAIN="$TOOLCHAIN" "$XCADDY" build v2.11.7 \
		--with "github.com/frontyard/fastcache=$REPO_ROOT")
fi

if [ "$WITH_SOUIN" = 1 ] && [ ! -x "$WORK/caddy-souin" ]; then
	if ! have xcaddy && [ ! -x "$("$GO" env GOPATH)/bin/xcaddy" ]; then
		echo "xcaddy not found, installing…"
		timeout "$NET_TIMEOUT" env GOTOOLCHAIN="$TOOLCHAIN" "$GO" install \
			github.com/caddyserver/xcaddy/cmd/xcaddy@latest
	fi
	XCADDY=xcaddy
	have xcaddy || XCADDY="$("$GO" env GOPATH)/bin/xcaddy"

	say "building caddy with souin + simplefs (from $STORAGES)"
	(cd "$WORK" && PATH="$(dirname "$GO"):$PATH" GOTOOLCHAIN="$TOOLCHAIN" "$XCADDY" build v2.11.7 \
		--with github.com/darkweak/souin/plugins/caddy \
		--with "github.com/darkweak/storages/core=$STORAGES/core" \
		--with "github.com/darkweak/storages/simplefs=$STORAGES/simplefs" \
		--with "github.com/darkweak/storages/simplefs/caddy=$STORAGES/simplefs/caddy" \
		--output caddy-souin)
fi

NGINX=nginx
if ! have nginx; then
	say "fetching nginx user-land (apt download, no sudo)"
	fetch_pkg nginx
	NGINX="$WORK/pkg/nginx/usr/sbin/nginx"
fi

WRK=wrk
WRK_LD=""
if ! have wrk; then
	say "fetching wrk user-land (apt download, no sudo)"
	# The wrk deb does not bundle luajit; it Depends on libluajit-5.1-2.
	if fetch_pkg wrk && fetch_pkg libluajit-5.1-2 && \
		[ -x "$WORK/pkg/wrk/usr/bin/wrk" ]; then
		WRK="$WORK/pkg/wrk/usr/bin/wrk"
		WRK_LD="$WORK/pkg/libluajit-5.1-2/usr/lib/x86_64-linux-gnu"
	else
		WRK=""
		echo "wrk unavailable, ab-only run"
	fi
fi

rm -rf "$CACHE" /tmp/simplefs-bench-cache "$WORK/nginx"
mkdir -p "$WORK/nginx/cache" "$WORK/nginx/tmp/client_body" "$WORK/nginx/tmp/proxy" \
	"$WORK/nginx/tmp/fastcgi" "$WORK/nginx/tmp/uwsgi" "$WORK/nginx/tmp/scgi" "$WWW"

# Static body for the file_server ceiling route (same size, incompressible).
head -c "$BODY_SIZE" /dev/urandom >"$WWW/body.bin"

say "starting backend :$PORT_BACKEND"
"$WORK/backend" ":$PORT_BACKEND" "$BODY_SIZE" & BACKEND_PID=$!
PIDS+=("$BACKEND_PID")

say "starting caddy fastcache+file_server :$PORT_FASTCACHE,:$PORT_FILESERVER (GOGC=$GOGC GOMEMLIMIT=$GOMEMLIMIT)"
env GOGC="$GOGC" GOMEMLIMIT="$GOMEMLIMIT" "$WORK/caddy" run \
	--config "$REPO_ROOT/bench/caddy/Caddyfile" \
	--adapter caddyfile >"$WORK/caddy.log" 2>&1 & CADDY_PID=$!
PIDS+=("$CADDY_PID")

if [ "$WITH_SOUIN" = 1 ]; then
	say "starting caddy souin+simplefs :$PORT_SOUIN (GOGC=$GOGC GOMEMLIMIT=$GOMEMLIMIT)"
	env GOGC="$GOGC" GOMEMLIMIT="$GOMEMLIMIT" "$WORK/caddy-souin" run \
		--config "$REPO_ROOT/bench/caddy/Caddyfile.souin" \
		--adapter caddyfile >"$WORK/caddy-souin.log" 2>&1 & SOUIN_PID=$!
	PIDS+=("$SOUIN_PID")
fi

say "starting nginx :$PORT_NGINX"
# nginx.conf runs with `daemon off` so the master stays a tracked child.
"$NGINX" -p "$WORK/nginx" -c "$REPO_ROOT/bench/nginx/nginx.conf" \
	>"$WORK/nginx.out" 2>&1 & NGINX_PID=$!
PIDS+=("$NGINX_PID")

wait_port backend "$PORT_BACKEND" "$BACKEND_PID"
wait_port "caddy (fastcache)" "$PORT_FASTCACHE" "$CADDY_PID"
wait_port "caddy (file_server)" "$PORT_FILESERVER" "$CADDY_PID"
[ "$WITH_SOUIN" = 1 ] && wait_port "caddy (souin)" "$PORT_SOUIN" "$SOUIN_PID"
wait_port nginx "$PORT_NGINX" "$NGINX_PID"

warm() {
	local name=$1 port=$2
	timeout "$BENCH_TIMEOUT" ab -n "$WARMUP" -c 10 -k "http://localhost:$port/" \
		>/dev/null 2>&1
	echo "warmed $name"
}

measure() {
	local name=$1 port=$2
	warm "$name" "$port"
	timeout "$BENCH_TIMEOUT" ab -n "$REQUESTS" -c "$CONCURRENCY" -k \
		"http://localhost:$port/" | \
		grep -E "Requests per second|Time per request|Failed|99%"
	if [ -n "$WRK" ]; then
		timeout "$BENCH_TIMEOUT" env ${WRK_LD:+LD_LIBRARY_PATH="$WRK_LD"} \
			"$WRK" -t4 -c"$CONCURRENCY" \
			-d"${WRK_DURATION}s" --latency "http://localhost:$port/" | \
			grep -E "Requests/sec|^\s+Latency|99%"
	fi
}

say "caddy file_server — ceiling (:$PORT_FILESERVER)"
measure file_server "$PORT_FILESERVER"

say "caddy + fastcache (:$PORT_FASTCACHE)"
measure fastcache "$PORT_FASTCACHE"

if [ "$WITH_SOUIN" = 1 ]; then
	say "caddy + souin + simplefs (:$PORT_SOUIN)"
	measure souin "$PORT_SOUIN"
fi

say "nginx proxy_cache (:$PORT_NGINX)"
measure nginx "$PORT_NGINX"
