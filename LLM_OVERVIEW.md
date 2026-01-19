# MediaDash Go BLE Client – Token-Optimized Master Overview

## Mission
- Always-on daemon that keeps the llizard UI in sync with the Android MediaDash phone over BLE.
- Uses Redis as the contract between firmware and UI: every connection metric, track field, and command lives under well-known keys so other services can poll state without touching BLE.

## Data + Command Flow
1. **BLE → Redis**
   - `internal/ble` scans + pairs with the MediaDash UUID, subscribes to metadata, playback, and album-art characteristics, then writes fresh values into hashes/lists such as `media:track`, `media:progress`, `system:ble_connected`, `system:ble_name`, and `/var/mediadash/album_art_cache`.
   - Album art chunks are cached with CRC32-safe names and mirrored in Redis (`media:album_art_cache:<hash>`) so UI widgets know when blobs are valid.
2. **Redis → BLE**
   - UI drops commands into `system:playback_cmd_q`; `internal/redis` dequeues, the BLE layer validates, rate-limits, and writes to the control characteristic with retry/backoff on ATT failures.
3. **Heartbeat**
   - `cmd/mediadash-client/main.go` ticks `monitorStatus`, emitting `/tmp/llizard_ble_status.json` (running, connected, strength, last_update) for watchdogs and the Settings page.

## Key Packages
- `cmd/mediadash-client`: loads `config.json`, wires graceful shutdown, and starts BLE IO, Redis command pump, and the status writer goroutines.
- `internal/ble`: wraps TinyGo bluetooth adapters, maintains connection quality metrics, throttles writes, and exposes hooks for album-art streaming.
- `internal/redis`: typed helpers (`StoreMediaState`, `StoreConnectionHealth`, `QueuePlaybackCommand`, `AlbumArtCache`) so all DB reads/writes stay centralized.
- `internal/albumart`: normalizes hashes, persists blobs under `/var/mediadash/album_art_cache`, and slices chunks according to `config.albumArt.ble.chunkSize` for BLE transport.
- `internal/settings`: hot-reloads scan intervals, RSSI thresholds, and feature flags without rebuilding.

## Ops + Tooling
- **Config:** `config.json` declares device UUIDs, Redis key names, album-art paths, and toggleable features (debug logs, simulated device IDs).
- **Scripts:** `build-deploy.sh`, `test_build.sh`, and the Dockerfile cross-compile to ARM, push binaries, and run integration tests.
- **Docs:** `COMMAND_PROCESSING.md`, `PERFORMANCE_OPTIMIZATIONS.md`, and `ALBUM_ART_PROTOCOL_FIXES.md` capture ATT mitigations, Redis schemas, and transport tweaks for future LLM/codegen reference.

In short: the Go client is a single, container-friendly service that keeps BLE complexity hidden behind Redis. Connection health, media metadata, album art, and user commands all flow through that database, guaranteeing the llizard UI always has an up-to-date, replayable source of truth.
