# dragon-cluster-resp-proxy

Unix-socket RESP proxy that presents a **standalone Redis** to clients and routes to a **Dragonfly cluster** (or Redis Cluster) behind the scenes.

Clients connect to one socket. The proxy hashes keys into the 16,384-slot space, follows `MOVED`, hides cluster topology, and pipelines bursts to the owning masters. Built for high IOPS on a host next to the cluster (millions of GET/s in on-box tests).

## What it does

- Accepts standalone Redis clients on a unix socket (`PING`, `GET`/`SET`, pipelining, `MULTI`/`EXEC`/`WATCH` same-slot, Lua, blocking commands, pub/sub).
- Discovers topology via `CLUSTER SHARDS` / `CLUSTER SLOTS` and keeps a 16,384-entry slot map.
- Follows `MOVED` (Dragonfly has no `ASK`/`ASKING`) and never exposes cluster redirects to clients.
- Rewrites global pub/sub to Dragonfly sharded pub/sub (`PUBLISH` → `SPUBLISH`, `SUBSCRIBE` → `SSUBSCRIBE`). `PSUBSCRIBE` is rejected on Dragonfly cluster.
- Virtual numbered databases: `SELECT 0` is unprefixed; `SELECT N` (`N>0`) stores keys as `dbN:<key>`.
- Optional advertised-address rewrite for SSH tunnels / NAT (`address_map`).

Non-goals: no local transaction cache, no OpenTelemetry traces on the data path, no `go-redis` on the data path.

## Requirements

- Go 1.27 (`GOTOOLCHAIN=auto` will fetch it if needed)
- A Dragonfly cluster (`--cluster_mode=yes`, tested against **v1.40.2**) or Redis Cluster
- Place the proxy on a host that can reach the advertised node IPs (or use `address_map`)

## Build and run

```bash
make build
./bin/dragon-cluster-resp-proxy --config configs/config.yaml
```

Environment variables always win over YAML. Prefix is `DCRP_`.

```bash
DCRP_UNIX_SOCKET=/tmp/dragon-cluster-resp-proxy.sock \
DCRP_CLUSTER_SEEDS=10.0.0.1:6379,10.0.0.2:6379 \
DCRP_CLUSTER_PASSWORD=... \
DCRP_BACKEND_FLAVOR=dragonfly \
./bin/dragon-cluster-resp-proxy --config configs/config.yaml
```

Connect as a standalone client:

```bash
redis-cli -s /tmp/dragon-cluster-resp-proxy.sock
```

`--version` prints the build version. `--pprof` enables pprof on `pprof_addr` (default `127.0.0.1:6264`).

## Configuration

See [`configs/config.yaml`](configs/config.yaml). Important fields:

| YAML | Env | Notes |
|---|---|---|
| `unix_socket` | `DCRP_UNIX_SOCKET` | Client listen path |
| `cluster_seeds` | `DCRP_CLUSTER_SEEDS` | Comma-separated seeds; the rest is discovered |
| `cluster_password` | `DCRP_CLUSTER_PASSWORD` | Backend `AUTH` |
| `backend_flavor` | `DCRP_BACKEND_FLAVOR` | `auto`, `dragonfly`, or `redis` |
| `address_map` | `DCRP_ADDRESS_MAP` | `src=dst,src=dst` rewrite of advertised endpoints |
| `max_databases` | `DCRP_MAX_DATABASES` | Virtual `SELECT` range (`0..N-1`, default 16) |
| `max_pipeline` | `DCRP_MAX_PIPELINE` | Commands coalesced per backend burst (default 128) |
| `pool_max_per_node` | `DCRP_POOL_MAX_PER_NODE` | Raise this for high client counts |
| `metrics_addr` | `DCRP_METRICS_ADDR` | Prometheus + `/healthz` (default `:9294`) |
| `client_auth_password` | `DCRP_CLIENT_AUTH_PASSWORD` | Optional AUTH from unix-socket clients |
| `read_from_replicas` | `DCRP_READ_FROM_REPLICAS` | Off by default; Dragonfly `READONLY` is a stub |

### Address map (tunnels / NAT)

`CLUSTER SLOTS` advertises the nodes' private IPs. If you reach them through forwards, map advertised → local:

```bash
DCRP_ADDRESS_MAP=192.168.0.131:6383=127.0.0.1:16383,192.168.0.18:6383=127.0.0.1:26383
```

### Virtual databases

Dragonfly cluster has no numbered DBs. The proxy keeps a per-session `SELECT` and prefixes keys for `N>0`:

- `SELECT 0` then `GET foo` → `GET foo`
- `SELECT 4` then `GET foo` → `GET db4:foo`

Reads/writes, `WATCH`, Lua `KEYS`, `SCAN`/`KEYS` patterns, `COPY`/`MOVE`, and key-echoing replies are translated. Pub/sub channels are not prefixed. `FLUSHALL` and `SWAPDB` are rejected.

## Multi-key commands

Standalone clients can send multi-key commands without hash tags. The proxy splits independent per-key ops by slot, runs the groups in parallel, and merges replies:

- `DEL` / `UNLINK` / `EXISTS` / `TOUCH` — integer replies are summed
- `MGET` — array in the original key order
- `MSET` — `OK` if every group succeeds

Transactional or algebraic multi-key ops stay rejected with `CROSSSLOT`: `MSETNX`, `SINTER` / `SUNION` / `SDIFF` and `*STORE`, `SINTERCARD`, `ZUNION*` / `ZINTER*` / `ZDIFF*`, `BITOP`, `PFCOUNT` / `PFMERGE`, `RENAME` / `RENAMENX`, `SMOVE`, `LMOVE`, multi-key `BLPOP` / `BRPOP` / `BZPOP*`, multi-key `EVAL*`, `WATCH`, and `MULTI` spanning slots.

## Dragonfly notes

- Hash slots are the Redis 16,384-slot space: `crc16(tag(key)) & 0x3FFF`. The proxy does not hardcode a 2-way split; it uses whatever `CLUSTER SLOTS` reports.
- After a migration ACK, Dragonfly `MOVED`s even if `CLUSTER SLOTS` still lists the source. The proxy trusts `MOVED`.
- Do not send `DFLYCLUSTER` / `DFLYMIGRATE` through the proxy.
- `SELECT` on the backend is only DB 0; virtual DBs are proxy-side.
- Global `PUBLISH` is unsupported in `--cluster_mode=yes`; the proxy rewrites to sharded pub/sub.

## Observability

- Prometheus: `http://<metrics_addr>/metrics` (`dragon_cluster_resp_proxy_*`)
- Liveness: `http://<metrics_addr>/healthz`
- JSON logs on stdout (`log_level`)
- Sample alerts: [`deploy/prometheus-alerts.yml`](deploy/prometheus-alerts.yml)

## Tests

```bash
make test          # unit
make test-race
make bench         # microbenchmarks
make test-integration   # needs test/compose (Dragonfly v1.40.2 + Redis 7)
```

Load harness (run it **next to the cluster**, not over a high-RTT SSH tunnel):

```bash
go run ./test/perf -sock /tmp/dragon-cluster-resp-proxy.sock \
  -n 2000000 -c 128 -pipeline 128 -keys 16384 -mode get
```

`-keys` is the keyspace size (`bench:0`…`bench:N-1`), not the slot count. Omit a shared hash tag so keys spread across masters.

Local compose backends: [`test/compose/docker-compose.yml`](test/compose/docker-compose.yml).

## Layout

```
cmd/dragon-cluster-resp-proxy/   entrypoint
internal/proxy/                  unix listener, sessions, pipelining, virtual DBs
internal/cluster/                slots, topology, MOVED, pools
internal/command/                command table, key extraction, DB prefix, fan-out
internal/pubsub/                 Dragonfly sharded rewrite
internal/resp/                   RESP codec
internal/config/                 YAML + DCRP_* env
```

## License

[MIT](LICENSE) © 2026 Barestack
