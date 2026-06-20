# better-otbr-ha-dashboard

Self-hosted live dashboard for Dockerized OpenThread Border Router networks, built for Home Assistant, OTBR, and Matter Server stacks.

`better-otbr-ha-dashboard` combines OTBR REST data, `ot-ctl` diagnostics, Home Assistant Matter names, python-matter-server diagnostics, local aliases, and live traffic history into one browser UI. It is designed for trusted local servers where the Thread border router already runs in Docker.

## Features

- Thread topology graph for OTBR, routers, children, relay paths, retained links, and Matter diagnostics observations
- Smooth traffic visualization across direct and inferred multi-hop paths
- Traffic log with local timestamps, direction, protocol, byte count, RSSI, endpoints, and inferred path
- Accurate counter rate display with stale-read and counter-reset handling
- User-arranged graph layout persisted in browser storage
- Fit, pan, and zoom controls
- Home Assistant Matter device names from `.storage/core.device_registry`
- Matter inventory nodes for known HA devices missing from live OTBR tables
- Matter IP enrichment from python-matter-server websocket
- Matter Thread identity and diagnostics links from python-matter-server data
- Manual aliases from config or selected-node UI, notes, and sticky discovered names
- Fast, slow, and idle refresh lanes
- Shared backend refresh loop for all live clients
- Docker Compose deployment
- Single Go binary with static frontend assets

## Screens

- Live topology graph with retained nodes, weak-link highlighting, and active traffic overlays
- Collapsible status, health, Matter mapping, selected-node, and traffic panels
- Local traffic inspector with pause, clear, search, and direction filters
- Selected-node alias editor for naming unmapped routers directly from the graph

## Data Sources

| Source | Purpose |
| --- | --- |
| `OTBR_REST_URL` | OTBR node and network metadata |
| Docker API + `ot-ctl` | Counters, topology tables, router table, SRP hosts, RX/TX history |
| Home Assistant `.storage/core.device_registry` | Matter names, models, manufacturers, area IDs |
| python-matter-server websocket | Matter node Thread IP addresses |
| python-matter-server data directory | Matter Thread hardware/IP identities, route tables, and neighbor observations |
| `config/aliases.json` | Manual aliases, notes, sticky discovered names |

## Requirements

- Docker
- Docker Compose
- Dockerized OTBR container
- OTBR REST API
- Docker socket access
- Optional Home Assistant `.storage` directory
- Optional python-matter-server data directory and websocket

## Architecture

```text
Browser
  |
  |  /api/events, /api/snapshot, /api/refresh
  v
Go backend
  |-- OTBR REST API
  |-- Docker API -> ot-ctl inside OTBR container
  |-- Home Assistant device registry
  |-- python-matter-server websocket
  |-- python-matter-server diagnostics files
  `-- config/aliases.json
```

The backend runs one shared refresh loop for all connected browsers. Expensive topology and metadata work runs on a slower cadence than live counter and traffic updates.

## Quick Start

```bash
cp .env.example .env
mkdir -p config empty-ha-storage empty-matter-data
cp config/aliases.example.json config/aliases.json
docker compose up -d --build
```

Open:

```text
http://SERVER_IP:8888
```

## Docker Compose

```bash
docker compose up -d --build
docker compose logs -f better-otbr-ha-dashboard
docker compose down
```

### Host Networking

Recommended when OTBR REST and Matter Server listen on localhost:

```env
NETWORK_MODE=host
LISTEN_ADDR=:8888
OTBR_REST_URL=http://127.0.0.1:8981
MATTER_WS_URL=ws://127.0.0.1:5580/ws
```

### Bridge Networking

Use reachable container names or host addresses:

```env
NETWORK_MODE=bridge
LISTEN_ADDR=:8888
OTBR_REST_URL=http://otbr:8981
MATTER_WS_URL=ws://matter-server:5580/ws
```

If you use bridge networking, expose the dashboard port in your Compose file.

## Configuration

All runtime configuration is provided through `.env`.

| Variable | Default | Description |
| --- | --- | --- |
| `THREAD_DASHBOARD_PORT` | `8888` | Human-facing port hint |
| `CONTAINER_NAME` | `better-otbr-ha-dashboard` | Dashboard container name |
| `NETWORK_MODE` | `host` | Docker network mode |
| `LISTEN_ADDR` | `:8888` | HTTP listen address |
| `OTBR_REST_URL` | `http://127.0.0.1:8981` | OTBR REST API URL |
| `OTBR_CONTAINER` | `otbr` | OTBR container name |
| `DOCKER_SOCKET` | `/var/run/docker.sock` | Docker socket path |
| `HA_STORAGE_HOST` | `./empty-ha-storage` | Host path to Home Assistant `.storage` |
| `HA_STORAGE` | `/ha-storage` | Container path for Home Assistant `.storage` |
| `MATTER_DATA_HOST` | `./empty-matter-data` | Host path to python-matter-server data |
| `MATTER_DATA` | `/matter-data` | Container path for python-matter-server data |
| `MATTER_WS_URL` | `ws://127.0.0.1:5580/ws` | python-matter-server websocket URL |
| `MATTER_IP_TTL` | `10m` | Matter IP cache duration |
| `TOPOLOGY_NODE_TTL` | `90s` | Retained missing node duration |
| `TOPOLOGY_LINK_TTL` | `5m` | Retained topology link duration |
| `ALIAS_FILE` | `/config/aliases.json` | Alias file path |
| `POLL_INTERVAL` | `10s` | Fast refresh interval for counters and traffic |
| `SLOW_POLL_INTERVAL` | `60s` | Slow refresh interval for topology, SRP, router table, Matter diagnostics |
| `IDLE_POLL_INTERVAL` | `60s` | Refresh interval with no live browser clients |
| `METADATA_CACHE_TTL` | `30s` | Disk metadata cache duration |
| `NODE_CACHE_TTL` | `30s` | OTBR REST `/node` cache duration |
| `INCLUDE_RAW` | `false` | Include raw `ot-ctl` output in API snapshots |
| `INCLUDE_COUNTERS` | `false` | Include full parsed counter maps in API snapshots |
| `ENABLE_TRAFFIC` | `true` | Collect OTBR RX/TX history |
| `LOG_REFRESH_TIMING` | `true` | Log refresh timing |
| `TRAFFIC_HISTORY_LIMIT` | `2000` | Browser-side retained traffic events |
| `GOGC` | `50` | Go garbage collection target |
| `GOMEMLIMIT` | `64MiB` | Go soft memory limit |

Durations use Go duration syntax: `1s`, `30s`, `5m`, `1h`.

## Example `.env`

```env
THREAD_DASHBOARD_PORT=8888
NETWORK_MODE=host
LISTEN_ADDR=:8888

OTBR_REST_URL=http://127.0.0.1:8981
OTBR_CONTAINER=otbr
DOCKER_SOCKET=/var/run/docker.sock

HA_STORAGE_HOST=/srv/homeassistant/.storage
HA_STORAGE=/ha-storage

MATTER_DATA_HOST=/srv/matter-server/data
MATTER_DATA=/matter-data
MATTER_WS_URL=ws://127.0.0.1:5580/ws
MATTER_IP_TTL=10m

TOPOLOGY_NODE_TTL=90s
TOPOLOGY_LINK_TTL=5m
ALIAS_FILE=/config/aliases.json

POLL_INTERVAL=10s
SLOW_POLL_INTERVAL=60s
IDLE_POLL_INTERVAL=60s
METADATA_CACHE_TTL=30s
NODE_CACHE_TTL=30s

INCLUDE_RAW=false
INCLUDE_COUNTERS=false
ENABLE_TRAFFIC=true
LOG_REFRESH_TIMING=true
TRAFFIC_HISTORY_LIMIT=2000
GOGC=50
GOMEMLIMIT=64MiB
```

## Aliases

Local aliases live in `config/aliases.json` on the host. The file is mounted into the container at `/config/aliases.json` and is intentionally ignored by git so household names and device labels stay local.

Start from the generic example:

```bash
cp config/aliases.example.json config/aliases.json
```

Example:

```json
{
  "nodes": {
    "0x5006": "Kitchen motion sensor",
    "821988813fe1b531": "Kitchen motion sensor"
  },
  "notes": {
    "0x5006": "Weak signal"
  },
  "sticky": {}
}
```

Supported node keys:

- Thread RLOC16
- Extended MAC
- Dashboard node ID

Recommended key order:

- Use extended MAC when available, because it survives RLOC changes.
- Use RLOC16 only for quick temporary naming.
- Use dashboard node ID when it is already an extended MAC or stable `matter-*` ID.

The dashboard may write discovered sticky names to `sticky`. Sticky names help prevent labels from flickering when live Matter IP data is temporarily stale or when OTBR misses a poll.

### Naming From The UI

Click a node in the graph, enter a friendly name in the **Alias** field, and press **Save**. The dashboard writes the alias to `config/aliases.json`, preferring the node extended MAC when available. That keeps the name stable even if Thread assigns a new RLOC16 later.

The alias field offers known Matter names as suggestions, but you can type any local label. Saving refreshes the snapshot immediately.

### Editing By Hand

After editing aliases manually, either wait for `METADATA_CACHE_TTL` to expire or click **Refresh** in the UI:

```bash
curl http://SERVER_IP:8888/api/refresh
```

If you want to reset only local learned names, stop the service and edit or remove the `sticky` object in `config/aliases.json`. Do not commit this file if it contains private room names or device labels.

Automatic name sources are applied in this order:

- Manual alias from `nodes`
- Sticky alias from previous exact matches
- Exact Matter match by ext MAC or Thread IP
- Matter inventory fallback for known HA Matter devices not visible in OTBR topology

## Graph

| Visual | Meaning |
| --- | --- |
| Yellow node | OTBR |
| Blue node | Router |
| Green node | Child/end device |
| Red node | Weak node |
| Gray dashed node | Stale retained node |
| Solid link | Direct OTBR neighbor or child link |
| Dashed link | Relay or mesh link |
| Dotted yellow-green link | Matter diagnostics observation |
| Faint dotted link | Retained fallback link |
| Bright/wide link | Active traffic |
| Red link | Weak link |

Node positions can be rearranged by dragging. Positions are saved in browser `localStorage`; new nodes are added to the existing saved layout.

## Traffic

`ENABLE_TRAFFIC=true` collects OTBR RX/TX history and displays all retained events up to `TRAFFIC_HISTORY_LIMIT`.

Traffic is border-router history reported by OTBR. It is not a full Thread packet sniffer. Multi-hop paths are inferred from Thread route data and Matter diagnostics when available.

## API

| Endpoint | Description |
| --- | --- |
| `/api/snapshot` | Latest dashboard snapshot |
| `/api/refresh` | Manual refresh and snapshot response |
| `/api/events` | Server-sent snapshot stream |

## Security

The Docker socket is mounted read-only in `compose.yml`, but Docker socket access is still powerful. Run this dashboard only on a trusted host and trusted network.

Recommended production posture:

- Keep the service on a trusted LAN or behind your own reverse proxy and authentication.
- Mount Home Assistant and Matter Server paths read-only.
- Do not commit private `config/aliases.json` files if they contain household names or device labels.
- Keep `INCLUDE_RAW=false` unless actively debugging.

## Limitations

- OTBR access currently expects a Dockerized OTBR container and uses `docker exec <OTBR_CONTAINER> ot-ctl`.
- Bare-metal OTBR, SSH, and REST-only modes are not first-class backends yet.
- OTBR RX/TX history only reports traffic observed by the border router.
- Matter and Home Assistant integrations are optional; without them, nodes may display Thread IDs until aliases are added.

## Troubleshooting

Check OTBR REST:

```bash
curl http://127.0.0.1:8981/node
```

Check `ot-ctl` in the OTBR container:

```bash
docker exec otbr ot-ctl state
docker exec otbr ot-ctl router table
docker exec otbr ot-ctl child table
```

Check dashboard logs:

```bash
docker compose logs -f better-otbr-ha-dashboard
```

If topology appears stale or flickers, increase the retention windows:

```env
TOPOLOGY_NODE_TTL=3m
TOPOLOGY_LINK_TTL=10m
```

## Development

```bash
go test ./...
go run .
```

Frontend assets are in `static/`.

## Release

This project uses Git tags for releases.

```bash
git tag v0.2.0
git push origin main v0.2.0
```

## License

See `LICENSE`, `LICENSE-ADDITIONAL-TERMS.md`, and `NOTICE`.
