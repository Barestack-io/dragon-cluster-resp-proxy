// Command perf issues pipelined GET/SET against a unix-socket proxy.
// Usage: go run ./test/perf -sock /tmp/dragon-cluster-resp-proxy.sock -n 100000 -pipeline 32 -c 32 -keys 4096
package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func main() {
	sock := flag.String("sock", "/tmp/dragon-cluster-resp-proxy.sock", "proxy unix socket")
	n := flag.Int("n", 100000, "total commands")
	pipeline := flag.Int("pipeline", 32, "commands per write burst")
	clients := flag.Int("c", 16, "concurrent clients")
	keys := flag.Int("keys", 4096, "keyspace size (spread across cluster slots; no hash tag)")
	mode := flag.String("mode", "get", "get | set | mix")
	prefix := flag.String("prefix", "bench", "key prefix")
	flag.Parse()

	var ok atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	per := *n / *clients
	for i := 0; i < *clients; i++ {
		id := i
		wg.Go(func() {
			c, err := net.Dial("unix", *sock)
			if err != nil {
				panic(err)
			}
			defer c.Close()
			r := resp.NewReader(c)
			buf := make([]byte, 0, 8*1024)
			sent := 0
			for sent < per {
				burst := *pipeline
				if sent+burst > per {
					burst = per - sent
				}
				buf = buf[:0]
				for j := 0; j < burst; j++ {
					k := *prefix + ":" + strconv.Itoa((id*per+sent+j)%*keys)
					cmd := "GET"
					switch *mode {
					case "set":
						cmd = "SET"
					case "mix":
						if (sent+j)%2 == 0 {
							cmd = "SET"
						}
					}
					if cmd == "SET" {
						buf = resp.EncodeCommand(buf, [][]byte{[]byte("SET"), []byte(k), []byte("v")})
					} else {
						buf = resp.EncodeCommand(buf, [][]byte{[]byte("GET"), []byte(k)})
					}
				}
				if _, err := c.Write(buf); err != nil {
					panic(err)
				}
				for j := 0; j < burst; j++ {
					if _, err := r.ReadValue(); err != nil {
						panic(err)
					}
					ok.Add(1)
				}
				sent += burst
			}
		})
	}
	wg.Wait()
	d := time.Since(start)
	fmt.Printf("ops=%d elapsed=%s rps=%.0f clients=%d pipeline=%d keys=%d mode=%s\n",
		ok.Load(), d, float64(ok.Load())/d.Seconds(), *clients, *pipeline, *keys, *mode)
}
