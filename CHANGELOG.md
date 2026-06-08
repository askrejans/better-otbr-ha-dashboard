# Changelog

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
