# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MediaDash Go BLE Client is an always-on daemon that bridges the Android MediaDash phone companion app and the llizard CarThing UI over Bluetooth Low Energy. It uses Redis as the contract between BLE and the UI—connection health, media metadata, album art, and user commands all flow through Redis keys.

**Target Platform:** ARM Linux (Spotify CarThing - ARMv7, Cortex-A7)
**BLE Stack:** TinyGo Bluetooth (`tinygo.org/x/bluetooth`)
**Database:** Redis for state synchronization with llizardgui-host UI

## Build Commands

### Native Build (Development/Testing)
```bash
go build -o bin/mediadash-client ./cmd/mediadash-client
go run ./cmd/mediadash-client
```

### Cross-Compile for CarThing (ARM)
```bash
GOOS=linux GOARCH=arm GOARM=7 go build -o bin/mediadash-client ./cmd/mediadash-client
```

### Build and Deploy
```bash
./build-deploy.sh build-deploy    # Build ARM binary and deploy to CarThing
./build-deploy.sh build           # Just build
./build-deploy.sh deploy          # Deploy existing binary
./build-deploy.sh status          # Check deployment status
./build-deploy.sh --native        # Build for local machine instead of ARM
```

### Testing Commands via Redis
```bash
./scripts/test-commands.sh        # Interactive command queue testing
```

### Deployment
CarThing device: IP `172.16.42.2`, user `root`, password `llizardOS`

## Architecture

### Data Flow

```
BLE → Redis (metadata, progress, connection status, album art)
Redis → BLE (playback commands: play, pause, seek, volume, next, previous)
```

### Key Packages

- **`cmd/mediadash-client`** - Entry point; loads config, wires graceful shutdown, starts BLE IO, Redis command pump, and status writer goroutines

- **`internal/ble`** - Core BLE client (`client.go`):
  - Device scanning with MediaDash service UUID filtering
  - Characteristic subscription (media state, album art data)
  - Rate-limited writes to prevent ATT error 0x0e
  - Connection health monitoring with activity-based checks
  - Command retry queue with exponential backoff
  - TimeTracker for real-time position updates

- **`internal/redis`** - Redis operations (`store.go`):
  - `StoreMediaState()` - Writes track/artist/album/position/duration
  - `DequeuePlaybackCommand()` - Pops from `system:playback_cmd_q`
  - `PublishBLEStatus()` - Connection status for UI health indicators
  - `SetAlbumArtFilePath()` - Points UI to cached album art

- **`internal/albumart`** - Album art handling:
  - `GenerateAlbumArtHash(artist, album)` - CRC32 hash matching Android algorithm
  - Chunked transfer reassembly with base64 decoding
  - Disk cache at `/var/mediadash/album_art_cache/`
  - Retry manager for failed transfers

- **`internal/config`** - Embedded config (`config.json` via `//go:embed`)

- **`internal/settings`** - Hot-reload of runtime settings

### Configuration

Configuration is embedded via `//go:embed` in `internal/config/loader.go`. Key settings in `internal/config/config.json`:

```json
{
  "ble": {
    "serviceUUID": "0000a0d0-0000-1000-8000-00805f9b34fb",
    "rateLimiting": { "writeIntervalMs": 10 }
  },
  "redis": {
    "address": "127.0.0.1:6379",
    "keyMap": {
      "playbackCommandQueue": "system:playback_cmd_q",
      "trackTitle": "media:track",
      "isPlaying": "media:playing"
    }
  }
}
```

### Redis Key Schema

| Key | Description |
|-----|-------------|
| `media:track` | Current track title |
| `media:artist` | Current artist |
| `media:album` | Current album |
| `media:playing` | "true" or "false" |
| `media:duration` | Duration in seconds |
| `media:progress` | Position in seconds |
| `media:art_path` | File path to cached album art |
| `system:ble_connected` | BLE connection status |
| `system:ble_name` | Connected device name |
| `system:playback_cmd_q` | Command queue (list) |
| `ble:status:*` | Detailed BLE status keys |

### Command Queue Format

Commands in `system:playback_cmd_q` are JSON objects:
```json
{"action":"play|pause|next|previous|volume|seek|toggle","value":0,"timestamp":1234567890}
```

## Key Patterns

### BLE Rate Limiting
All BLE writes go through `rateLimitedWrite()` to prevent ATT error 0x0e (resource exhaustion). Default interval is 10ms between writes.

### Album Art Protocol
Android sends album art as base64-encoded chunks. The hash is CRC32 of `"artist.lowercase()|album.lowercase()"` as a decimal string (not hex). See `ALBUM_ART_PROTOCOL_FIXES.md` for details.

### TimeTracker
Maintains real-time playback position by incrementing locally (1s ticks) between Android updates. Prevents position jumpback from stale duplicate BLE notifications.

### Command Retry
Failed commands are queued with exponential backoff (2s, 4s, 6s) up to 3 attempts. Retryable errors include ATT failures and connection issues.

## External Documentation

- `LLM_OVERVIEW.md` - High-level architecture summary
- `COMMAND_PROCESSING.md` - Command queue protocol details
- `ALBUM_ART_PROTOCOL_FIXES.md` - Album art compatibility fixes
- `PERFORMANCE_OPTIMIZATIONS.md` - BLE performance tuning

## CarThing Redis Setup

Redis must be running on the CarThing for the client to function:
```bash
sshpass -p llizardOS ssh root@172.16.42.2 "sv start redis"
sshpass -p llizardOS ssh root@172.16.42.2 "redis-cli ping"
```
