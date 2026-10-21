// Backend is a minimal upstream server used by the fastcache vs nginx
// benchmarks. It serves a fixed-size, incompressible cacheable response.
package main

import (
	"crypto/rand"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {
	addr := ":9081"
	size := 64 * 1024

	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	if len(os.Args) > 2 {
		if s, err := strconv.Atoi(os.Args[2]); err == nil {
			size = s
		}
	}

	body := make([]byte, size)
	if _, err := rand.Read(body); err != nil {
		log.Fatal(err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"bench"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})

	log.Printf("backend listening on %s, body %d bytes", addr, size)
	log.Fatal(http.ListenAndServe(addr, handler))
}
