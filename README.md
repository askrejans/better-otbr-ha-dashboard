# better-otbr-ha-dashboard

A self-hosted live dashboard for OpenThread Border Router networks.

Better OTBR HA Dashboard combines OTBR REST data, `ot-ctl` diagnostics, optional Home Assistant Matter names, optional python-matter-server diagnostics, local aliases, and live traffic history into one browser UI.

It is built for Dockerized OTBR deployments, including Home Assistant + OTBR + Matter Server stacks, while keeping all host-specific paths and endpoints in `.env`.

## Features

- Live Thread topology graph for OTBR, routers, children, sleepy/end devices, relay paths, and observed Matter diagnostics links
- Stable graph rendering with retained nodes and links when OTBR misses a poll
- Direct, relay, observed, and retained/anchor links styled separately
- Friendly names from Home Assistant Matter device registry
- Live Matter IP lookup through python-matter-server websocket
- Sticky local name cache so labels do not flicker when IP-only matches are stale
- Traffic history inspector with pause, clear, search, direction filter, source/destination IPs, RSSI, byte counts, and inferred path display
- Single Go backend and static frontend
- Docker Compose deployment with `.env` configuration
- No external frontend CDN or hosted service dependency

## Maintainer

Arvis Skrējāns <arvis.skrejans@gmail.com>

## How It Works

The dashboard collects data from several optional sources:

- `OTBR_REST_URL`: basic OpenThread node/network metadata.
- `docker exec <OTBR_CONTAINER> ot-ctl ...`: live Thread diagnostics, counters, router/child tables, route table, SRP hosts, and RX/TX packet history.
- Home Assistant `.storage/core.device_registry`: Matter device names, manufacturers, models, and user-friendly names.
- python-matter-server websocket: live Matter node IP addresses for better automatic name matching.
- python-matter-server data directory: Matter Thread Diagnostics attributes used for observed links.
- `config/aliases.json`: manual aliases, notes, and sticky discovered names.

OTBR packet history only shows traffic crossing the border router. Hidden child-to-parent forwarding is not packet-sniffed by this dashboard. Multi-hop paths are inferred from Thread route tables and Matter Thread Diagnostics, and the UI marks those as inferred.

## Requirements

- Docker and Docker Compose
- An existing OTBR container reachable through Docker
- OTBR REST API enabled/reachable
- Read-only access to Docker socket for `docker exec`
- Optional: Home Assistant `.storage` path for friendly Matter names
- Optional: python-matter-server data path and websocket for better linking/diagnostics

## Quick Start

```bash
cd better-otbr-ha-dashboard
cp .env.example .env
mkdir -p config empty-ha-storage empty-matter-data
docker compose up -d --build
```

Open:

```text
http://SERVER_IP:8888
```

## Docker Compose

The included `compose.yml` is designed to be configured through `.env`.

```bash
docker compose up -d --build
docker compose logs -f
```

To update after changing files:

```bash
docker compose up -d --build --force-recreate
```

## Common Deployment Modes

### Host Networking

Recommended when OTBR REST and Matter Server listen on localhost:

```env
NETWORK_MODE=host
LISTEN_ADDR=:8888
OTBR_REST_URL=http://127.0.0.1:8981
MATTER_WS_URL=ws://127.0.0.1:5580/ws
```

### Docker Bridge Networking

Use service/container names or reachable host addresses:

```env
NETWORK_MODE=bridge
LISTEN_ADDR=:8888
OTBR_REST_URL=http://otbr:8981
MATTER_WS_URL=ws://matter-server:5580/ws
```

If using bridge networking, expose/publish `LISTEN_ADDR` as needed in your Compose file.

### Home Assistant + OTBR + Matter Server

Typical configuration:

```env
NETWORK_MODE=host
OTBR_CONTAINER=otbr
OTBR_REST_URL=http://127.0.0.1:8981
HA_STORAGE_HOST=/path/to/homeassistant/.storage
MATTER_DATA_HOST=/path/to/python-matter-server/data
MATTER_WS_URL=ws://127.0.0.1:5580/ws
```

## Configuration

All runtime settings are controlled by `.env`.

| Variable | Default | Required | Description |
| --- | --- | --- | --- |
| `THREAD_DASHBOARD_PORT` | `8888` | No | Human-facing port hint. Currently not used directly by `compose.yml` when `NETWORK_MODE=host`, but useful if you adapt Compose for bridge mode. |
| `CONTAINER_NAME` | `better-otbr-ha-dashboard` | No | Container name for this dashboard. |
| `NETWORK_MODE` | `host` | No | Docker network mode. `host` is easiest for localhost OTBR/Matter endpoints. |
| `LISTEN_ADDR` | `:8888` | Yes | Address/port the dashboard listens on inside the container or host network namespace. |
| `OTBR_REST_URL` | `http://127.0.0.1:8981` | Yes | OpenThread Border Router REST endpoint. |
| `OTBR_CONTAINER` | `otbr` | Yes | Existing OTBR container name used for `docker exec ... ot-ctl`. |
| `DOCKER_SOCKET` | `/var/run/docker.sock` | Yes | Docker socket path mounted read-only so the dashboard can execute `ot-ctl` inside `OTBR_CONTAINER`. |
| `HA_STORAGE_HOST` | `./empty-ha-storage` | No | Host path to Home Assistant `.storage`; enables Matter device names. |
| `HA_STORAGE` | `/ha-storage` | No | Container mount path for Home Assistant `.storage`. |
| `MATTER_DATA_HOST` | `./empty-matter-data` | No | Host path to python-matter-server data; enables Matter Thread Diagnostics inference. |
| `MATTER_DATA` | `/matter-data` | No | Container mount path for python-matter-server data. |
| `MATTER_WS_URL` | `ws://127.0.0.1:5580/ws` | No | python-matter-server websocket endpoint used for live Matter IP lookup. |
| `MATTER_IP_TTL` | `10m` | No | Cache duration for Matter IP lookup results. |
| `TOPOLOGY_LINK_TTL` | `5m` | No | How long to retain recently seen nodes and topology links when OTBR misses data. |
| `ALIAS_FILE` | `/config/aliases.json` | No | Alias file path inside the container. Host file lives under `./config`. |
| `POLL_INTERVAL` | `1s` | No | Backend refresh interval. |

Durations use Go duration syntax, for example `1s`, `30s`, `5m`, `1h`.

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

TOPOLOGY_LINK_TTL=5m
ALIAS_FILE=/config/aliases.json
POLL_INTERVAL=1s
```

## Aliases And Sticky Names

Use `config/aliases.json` to pin names and notes:

```json
{
  "nodes": {
    "0x5006": "Kitchen motion sensor",
    "821988813fe1b531": "Kitchen motion sensor"
  },
  "notes": {
    "0x5006": "Weak signal; move closer to a router"
  },
  "sticky": {}
}
```

Start from the safe example file:

```bash
cp config/aliases.example.json config/aliases.json
```

Supported keys include:

- Thread RLOC16, for example `0x5006`
- Extended MAC, for example `821988813fe1b531`
- Dashboard node ID

The dashboard also writes discovered sticky names to `sticky`. Sticky names intentionally win over volatile IP-only matches so names do not alternate when Matter IP cache data is stale.

## Traffic View

The traffic panel stores a local browser-side history and shows:

- Direction: `rx` means into OTBR; `tx` means from OTBR
- Full inferred path, for example `Device -> Router -> OTBR`
- Protocol
- Byte count
- RSSI when OTBR reports it
- Source and destination IPv6/port
- Whether the route is direct or inferred

Use Pause while investigating; live graph updates can continue while the traffic list is frozen.

## Link Types

| Type | Meaning |
| --- | --- |
| `child` | Direct OTBR child table link. |
| `router` | Direct OTBR neighbor/router link. |
| `relay` | Route-table next-hop relationship between routers. |
| `observed` | Matter Thread Diagnostics neighbor observation. |
| `anchor` | Retained fallback link used to keep a recently seen router attached to OTBR when one poll misses the direct link. |

## Security Notes

The dashboard mounts the Docker socket read-only, but Docker socket access is still powerful because it allows API access to Docker. Run this only on a trusted local server/network.

Recommended hardening:

- Bind `LISTEN_ADDR` only where needed.
- Put the dashboard behind your existing reverse proxy/authentication if exposing beyond a trusted LAN.
- Keep `HA_STORAGE_HOST` and `MATTER_DATA_HOST` read-only as shown in `compose.yml`.
- Do not commit private `config/aliases.json` if it contains household device names.

## Limitations

- The current backend expects a Dockerized OTBR container and uses `docker exec <OTBR_CONTAINER> ot-ctl`.
- Bare-metal OTBR is not first-class yet. A future backend mode could support local `ot-ctl`, SSH, or REST-only operation.
- OTBR RX/TX history is border-router traffic, not a full Thread radio sniffer.
- Multi-hop paths for sleepy/end devices are inferred when available, not directly packet-captured.
- Matter/Home Assistant integration is optional; without it, nodes may show Thread IDs until aliases are added.

## Troubleshooting

### No Data From OTBR

Check REST:

```bash
curl http://127.0.0.1:8981/node
```

Check `ot-ctl` inside the OTBR container:

```bash
docker exec otbr ot-ctl state
docker exec otbr ot-ctl router table
docker exec otbr ot-ctl child table
```

### Friendly Names Missing

Check Home Assistant storage mount:

```bash
docker compose exec better-otbr-ha-dashboard ls -l /ha-storage
```

Check Matter websocket:

```bash
docker compose logs better-otbr-ha-dashboard | grep 'matter ip lookup'
```

### Nodes Or Links Flicker

Increase:

```env
TOPOLOGY_LINK_TTL=10m
```

Then recreate:

```bash
docker compose up -d --build --force-recreate
```

### Dashboard Does Not Update

Check logs:

```bash
docker compose logs -f better-otbr-ha-dashboard
```

Check the browser network tab for `/api/events`.

## Development

Run locally:

```bash
go test ./...
go run .
```

The frontend is static HTML/CSS/JS under `static/`.

## License

This project is distributed under the GNU General Public License version 3.0 with Additional Terms prohibiting use for AI/ML training.

- `LICENSE`: GNU GPLv3 license text
- `LICENSE-ADDITIONAL-TERMS.md`: Additional Terms, including the no-AI-training restriction
- `NOTICE`: copyright, creator, and license summary
