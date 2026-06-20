# Changelog

## v0.3.1 - 2026-06-20

### Added

- Matter Thread identity enrichment from python-matter-server Network Commissioning data, using device hardware address and Thread IPv6 addresses to name live topology nodes without relying only on sticky aliases.
- README guidance for local aliases, sticky aliases, safe private config handling, and manual refresh after alias edits.

### Fixed

- Live routers can keep friendly names when Matter websocket IP lookup is incomplete and local sticky aliases are missing.
- Inventory-only Matter nodes are less likely to duplicate real topology nodes when Matter diagnostics provide a stable ext MAC or Thread IP.

## v0.3.0 - 2026-06-20

### Added

- Matter inventory nodes for Home Assistant Matter devices that are known but absent from the latest OTBR topology and traffic tables.
- Traffic-observed nodes for peers seen in OTBR RX/TX history but missing from current neighbor or child tables.
- Matter matching for inventory node IDs, traffic peer IPs, and existing traffic nodes.
- Regression tests for Matter inventory visibility, traffic-only peers, and hex Matter node IDs.

### Changed

- Default tracked alias config is now generic sample data so releases do not ship deployment-specific device names.
- Matter node ID handling now preserves Home Assistant registry IDs as hex while still parsing python-matter-server decimal IDs.

## v0.2.0 - 2026-06-10

### Added

- Shared live refresh loop with active-viewer detection and idle polling.
- Separate fast, slow, and idle polling intervals for lighter long-running deployments.
- Snapshot versioning for lower-noise server-sent event updates.
- Metadata and OTBR node caching.
- Configurable raw diagnostics, full counters, traffic collection, and traffic history limits.
- Stable node IDs based on extended MAC addresses where available.
- Retained stale nodes with configurable `TOPOLOGY_NODE_TTL`.
- Drag, pan, zoom, fit, and browser-persisted graph layout.
- Collapsible status, Matter mapping, health, selected-node, and traffic panels.
- Traffic event mapping to stable topology nodes and Matter IP-only devices.
- Unit tests for configuration, refresh serialization, SSE output, caching, parsing, graph stability, and traffic handling.

### Changed

- Default `POLL_INTERVAL` is now `10s`, with values below `2s` clamped.
- Default deployment is tuned for lower CPU and memory use with `GOGC=50` and `GOMEMLIMIT=64MiB`.
- README was rewritten for GitHub-ready installation, architecture, security, troubleshooting, and release documentation.

## v0.1.0 - 2026-06-08

Initial public release of `better-otbr-ha-dashboard`.

### Added

- Live OTBR topology graph for routers, children, sleepy/end devices, relay links, observed links, and retained anchor links.
- Stable graph retention with configurable `TOPOLOGY_LINK_TTL`.
- Home Assistant Matter device-name discovery.
- python-matter-server IP lookup and diagnostics inference.
- Sticky local alias cache to prevent name flicker.
- Traffic history inspector with pause, clear, search, direction filter, source/destination addresses, RSSI, byte counts, and inferred path display.
- Docker Compose deployment with `.env` configuration.
- GPLv3 license with separate additional no-AI-training terms.
