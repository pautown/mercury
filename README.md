# MediaDash Go BLE Client

An always-on daemon that bridges the MediaDash Android companion app and the llizard CarThing UI over Bluetooth Low Energy. It uses Redis as the contract between BLE and the UI, enabling connection health monitoring, media metadata synchronization, album art transfers, podcast data management, lyrics display, and playback commands to flow through well-known Redis keys.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [BLE Protocol](#ble-protocol)
- [Redis Schema](#redis-schema)
- [Album Art Protocol](#album-art-protocol)
- [Command Processing](#command-processing)
- [Configuration](#configuration)
- [Build and Deployment](#build-and-deployment)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Overview

MediaDash BLE Client acts as a bridge between:

- **Phone** (MediaDash Android app) - Sends media state, album art, podcast data, and lyrics via BLE notifications
- **CarThing UI** (llizardgui-host) - Reads state from Redis and enqueues commands

All data flows through Redis, providing a clean separation between BLE complexity and the UI layer.

**Target Platform:** ARM Linux (Spotify CarThing - ARMv7, Cortex-A7)
**BLE Stack:** TinyGo Bluetooth (`tinygo.org/x/bluetooth`)
**Database:** Redis 6+ for state synchronization

### Key Features

- Intelligent BLE device scanning with service UUID filtering
- Rate-limited BLE writes to prevent ATT error 0x0e (resource exhaustion)
- Activity-based connection health monitoring
- Command retry queue with exponential backoff
- Chunked album art transfers with base64/binary protocol support
- Real-time playback position tracking (TimeTracker)
- Podcast data lazy loading with compact BLE format
- Lyrics synchronization support

## Architecture

### Data Flow

```
                              ┌──────────────────────────────────────┐
                              │         MediaDash BLE Client         │
                              │                                      │
┌─────────────────┐  BLE      │  ┌────────────┐    ┌─────────────┐  │  Redis     ┌─────────────────┐
│                 │  Notify   │  │            │    │             │  │  Keys      │                 │
│     Android     │──────────>│  │  BLE I/O   │───>│ Redis Store │──┼──────────>│  CarThing UI    │
│   MediaDash     │           │  │            │    │             │  │           │  (llizardgui)   │
│                 │<──────────│  │            │<───│             │<─┼───────────│                 │
└─────────────────┘  BLE      │  └────────────┘    └─────────────┘  │  Redis    └─────────────────┘
                     Write    │                                      │  Queue
                   (Commands) │  ┌────────────────────────────────┐  │
                              │  │  Album Art Handler             │  │
                              │  │  - Chunked reassembly          │  │
                              │  │  - CRC32 validation            │  │
                              │  │  - Disk + Redis caching        │  │
                              │  └────────────────────────────────┘  │
                              └──────────────────────────────────────┘
```

### Package Structure

```
golang_ble_client/
├── cmd/
│   └── mediadash-client/
│       └── main.go              # Entry point, signal handling, status monitoring
├── internal/
│   ├── ble/
│   │   ├── client.go            # Core BLE client, scanning, connections
│   │   └── errorlog.go          # Crash/error logging to persistent files
│   ├── redis/
│   │   └── store.go             # Redis operations, key mappings, type definitions
│   ├── albumart/
│   │   ├── handler.go           # Transfer reassembly, disk caching
│   │   ├── validation.go        # Security validation, format detection
│   │   ├── retry.go             # Retry manager with exponential backoff
│   │   └── migration.go         # Cache migration utilities
│   ├── config/
│   │   ├── loader.go            # Embedded config loading
│   │   └── config.json          # BLE UUIDs, Redis keys, settings
│   ├── settings/
│   │   └── handler.go           # Runtime settings hot-reload
│   └── debug/
│       └── debug.go             # Debug logging utilities
├── scripts/
│   └── test-commands.sh         # Interactive command testing
├── config.json                  # Root config (embedded at build)
└── build-deploy.sh              # Build and deployment automation
```

### Key Components

#### BLE Client (`internal/ble/client.go`)

The core BLE client manages:

- **Device Scanning**: Filters for MediaDash service UUID, falls back to name-based matching
- **Characteristic Subscriptions**: Media state, album art, podcast info, lyrics
- **Rate-Limited Writes**: Configurable intervals (default 15ms) to prevent ATT errors
- **Connection Health**: Activity-based monitoring, automatic reconnection
- **TimeTracker**: Real-time position updates between Android notifications
- **Flow Control**: Separate notification buffers for media state (200) and album art (4096)

#### Redis Store (`internal/redis/store.go`)

Provides typed helpers for all Redis operations:

- `StoreMediaState()` - Track, artist, album, position, duration, playing state
- `DequeuePlaybackCommand()` - Pop commands from queue with timeout
- `PublishBLEStatus()` - Connection status for UI health indicators
- `StorePodcastList()`, `StoreRecentEpisodes()`, `StorePodcastEpisodes()` - Podcast data
- `StoreLyricsChunk()`, `GetLyrics()` - Lyrics with timestamp support
- `StoreAlbumArtCache()`, `GetAlbumArtCache()` - Album art blob caching

#### Album Art Handler (`internal/albumart/`)

Manages album art transfers with:

- **Chunked Reassembly**: Handles both legacy JSON/base64 and binary protocols
- **CRC32 Validation**: Per-chunk integrity checking
- **Disk Caching**: `/var/mediadash/album_art_cache/` with WebP format
- **Redis Mirroring**: Syncs disk cache to Redis for quick lookups
- **Retry Manager**: Automatic retry with selective chunk re-requests
- **Format Validation**: JPEG, PNG, WebP detection via magic bytes

## BLE Protocol

### Service and Characteristics

**Service UUID:** `0000a0d0-0000-1000-8000-00805f9b34fb`

| Characteristic | UUID | Direction | Description |
|----------------|------|-----------|-------------|
| Media State | `0000a0d1-...` | Android → Client | JSON media metadata notifications |
| Playback Control | `0000a0d2-...` | Client → Android | JSON playback commands |
| Album Art Request | `0000a0d3-...` | Client → Android | Request specific album art (recovery) |
| Album Art Data | `0000a0d4-...` | Android → Client | Chunked album art (binary/base64) |
| Podcast Info | `0000a0d5-...` | Bidirectional | Podcast data with compact format |
| Lyrics Request | `0000a0d6-...` | Client → Android | Request lyrics for track |
| Lyrics Data | `0000a0d7-...` | Android → Client | Chunked lyrics with timestamps |
| Settings | `0000a0d8-...` | Bidirectional | Configuration sync |
| Time Sync | `0000a0d9-...` | Bidirectional | Clock synchronization |

### Media State Notification Format

```json
{
  "isPlaying": true,
  "playbackState": "playing",
  "trackTitle": "Song Name",
  "artist": "Artist Name",
  "album": "Album Name",
  "duration": 240000,
  "position": 120000,
  "volume": 75,
  "albumArtHash": "1234567890",
  "mediaChannel": "Spotify"
}
```

**Note:** Duration and position are in milliseconds from Android, converted to seconds for Redis storage.

### Rate Limiting

All BLE writes use rate limiting to prevent ATT error 0x0e (resource exhaustion):

```go
func (c *Client) rateLimitedWrite(char *bluetooth.DeviceCharacteristic, data []byte) (int, error) {
    // Minimum 15ms between writes (configurable)
    // Token bucket pattern with write mutex
}
```

## Redis Schema

### Media State Keys

| Key | Type | Description | Example |
|-----|------|-------------|---------|
| `media:track` | String | Current track title | "Bohemian Rhapsody" |
| `media:artist` | String | Current artist | "Queen" |
| `media:album` | String | Current album | "A Night at the Opera" |
| `media:playing` | String | Playback state | "true" or "false" |
| `media:duration` | String | Duration in seconds | "354" |
| `media:progress` | String | Position in seconds | "120" |
| `media:album_art_path` | String | Path to cached art | "/var/mediadash/album_art_cache/1234567890.webp" |
| `media:controlled_channel` | String | Active media app | "Spotify" |
| `media:channels` | JSON | Available media apps | `{"channels":["Spotify","YouTube Music"],...}` |

### BLE Status Keys

| Key | Type | Description |
|-----|------|-------------|
| `system:ble_connected` | String | "true" or "false" |
| `system:ble_name` | String | Connected device name |
| `ble:status:connected` | String | Detailed connection status |
| `ble:status:device_name` | String | Device name |
| `ble:status:device_address` | String | MAC address |
| `ble:status:rssi` | String | Signal strength |
| `ble:status:connection_quality` | String | "excellent", "good", "fair", "poor" |
| `ble:status:last_update` | String | Unix timestamp (ms) |
| `ble:status:commands_processed` | String | Total commands sent |
| `ble:status:commands_failed` | String | Failed command count |
| `ble:status:scanning` | String | "true" if scanning |

### Command Queues

| Key | Type | Description |
|-----|------|-------------|
| `system:playback_cmd_q` | List | Playback commands (JSON) |
| `system:podcast_request_q` | List | Podcast data requests (JSON) |
| `system:ble_reconnect_request` | String | Timestamp to trigger reconnect |

### Album Art Cache

| Key Pattern | Type | Description |
|-------------|------|-------------|
| `mediadash:albumart:cache:<hash>` | Binary | Cached album art data |
| `mediadash:albumart:cache:<hash>:meta` | JSON | Metadata (size, timestamp) |
| `mediadash:albumart:request` | JSON | Pending request |

### Podcast Keys

| Key | Type | Description |
|-----|------|-------------|
| `podcast:list` | JSON | Channel list (compact format) |
| `podcast:recent_episodes` | JSON | Recent episodes across all podcasts |
| `podcast:episodes:<hash>` | JSON | Paginated episodes for specific podcast |
| `podcast:channel_count` | String | Total podcast count |

### Lyrics Keys

| Key | Type | Description |
|-----|------|-------------|
| `lyrics:data` | JSON | Complete lyrics with timestamps |
| `lyrics:hash` | String | CRC32 hash of "artist\|track" |
| `lyrics:synced` | String | "true" if has timestamps |
| `lyrics:enabled` | String | Feature toggle |

## Album Art Protocol

### Hash Generation

Both Android and Go client use identical CRC32 hashing:

```go
func GenerateAlbumArtHash(artist, album string) string {
    // Normalize: lowercase and trim whitespace
    artistLower := strings.ToLower(strings.TrimSpace(artist))
    albumLower := strings.ToLower(strings.TrimSpace(album))

    // Composite string with pipe separator
    composite := artistLower + "|" + albumLower

    // CRC32-IEEE checksum as DECIMAL string (not hex!)
    hash := crc32.ChecksumIEEE([]byte(composite))
    return fmt.Sprintf("%d", hash)  // e.g., "3462671303"
}
```

**Critical:** The hash is a decimal string (8-10 digits), not hexadecimal.

### Binary Protocol (Current)

16-byte header + raw image data per chunk:

| Offset | Size | Type | Field |
|--------|------|------|-------|
| 0 | 4 | uint32 | hash (CRC32 as uint32, little-endian) |
| 4 | 2 | uint16 | chunkIndex (0-based) |
| 6 | 2 | uint16 | totalChunks |
| 8 | 2 | uint16 | dataLength |
| 10 | 4 | uint32 | dataCRC32 (chunk data checksum) |
| 14 | 2 | uint16 | reserved |
| 16+ | N | bytes | raw image data (max 496 bytes) |

### Legacy JSON Protocol (Deprecated)

```json
{
  "hash": "3462671303",
  "chunkIndex": 0,
  "totalChunks": 15,
  "data": "base64EncodedImageData...",
  "crc32": 12345678
}
```

### Transfer Flow

1. **Track Change**: Android sends media state with `albumArtHash`
2. **Cache Check**: Client checks disk cache for existing art
3. **Cache Hit**: Update `media:album_art_path` immediately
4. **Cache Miss**: Wait for Android to proactively send chunks
5. **Chunk Reception**: Validate CRC32, store in transfer buffer
6. **Completion**: Reassemble chunks, validate format, cache to disk and Redis
7. **Recovery**: On failure, request retransmission via Album Art Request characteristic

## Command Processing

### Command Format

```json
{
  "action": "play|pause|next|previous|seek|volume|toggle|stop",
  "value": 0,
  "timestamp": 1234567890
}
```

### Supported Actions

| Action | Value | Description |
|--------|-------|-------------|
| `play` | - | Start playback |
| `pause` | - | Pause playback |
| `toggle` | - | Toggle play/pause |
| `next` | - | Next track |
| `previous` | - | Previous track |
| `seek` | ms | Seek to position |
| `volume` | 0-100 | Set volume level |
| `stop` | - | Stop playback |

### Podcast Commands

```json
{
  "action": "request_podcast_list",
  "timestamp": 1234567890
}

{
  "action": "request_recent_episodes",
  "timestamp": 1234567890
}

{
  "action": "request_podcast_episodes",
  "podcastId": "abc12345",
  "offset": 0,
  "limit": 50,
  "timestamp": 1234567890
}

{
  "action": "play_episode",
  "episodeHash": "def67890",
  "timestamp": 1234567890
}
```

### Lyrics Commands

```json
{
  "action": "request_lyrics",
  "artist": "Artist Name",
  "track": "Track Name",
  "timestamp": 1234567890
}
```

### Processing Pipeline

```
Redis Queue → Validation → BLE Conversion → Rate Limiting → Android GATT
                 ↓ (on failure)
           Retry Queue → Exponential Backoff → Retry (max 3 attempts)
```

### Retry Configuration

- **Max Retries:** 3 attempts
- **Initial Delay:** 2 seconds
- **Backoff:** Exponential (2s, 4s, 6s)
- **Retryable Errors:** ATT failures, connection issues, rate limit violations
- **Non-Retryable:** Validation failures, JSON errors

## Configuration

Configuration is embedded via `//go:embed` from `internal/config/config.json`:

```json
{
  "ble": {
    "deviceName": "MediaDash",
    "serviceUUID": "0000a0d0-0000-1000-8000-00805f9b34fb",
    "mediaStateCharacteristicUUID": "0000a0d1-0000-1000-8000-00805f9b34fb",
    "playbackControlCharacteristicUUID": "0000a0d2-0000-1000-8000-00805f9b34fb",
    "albumArtRequestCharacteristicUUID": "0000a0d3-0000-1000-8000-00805f9b34fb",
    "albumArtDataCharacteristicUUID": "0000a0d4-0000-1000-8000-00805f9b34fb",
    "podcastInfoCharacteristicUUID": "0000a0d5-0000-1000-8000-00805f9b34fb",
    "lyricsRequestCharacteristicUUID": "0000a0d6-0000-1000-8000-00805f9b34fb",
    "lyricsDataCharacteristicUUID": "0000a0d7-0000-1000-8000-00805f9b34fb",
    "rateLimiting": {
      "writeIntervalMs": 15
    },
    "connectionMonitoring": {
      "healthCheckIntervalMinutes": 1,
      "activityTimeoutMinutes": 1
    }
  },
  "redis": {
    "address": "127.0.0.1:6379",
    "keyMap": {
      "trackTitle": "media:track",
      "artistName": "media:artist",
      "albumName": "media:album",
      "isPlaying": "media:playing",
      "durationMs": "media:duration",
      "progressMs": "media:progress",
      "albumArtPath": "media:album_art_path",
      "bleConnected": "system:ble_connected",
      "bleName": "system:ble_name",
      "playbackCommandQueue": "system:playback_cmd_q"
    }
  },
  "albumArt": {
    "cacheDirectory": "/var/mediadash/album_art_cache",
    "ble": {
      "chunkSize": 512
    }
  }
}
```

## Build and Deployment

### Prerequisites

- Go 1.21+
- Redis 6+ (on CarThing or localhost for testing)
- CarThing device with llizardOS

### Native Build (Development)

```bash
go build -o bin/mediadash-client ./cmd/mediadash-client
./bin/mediadash-client
```

### Cross-Compile for CarThing (ARM)

```bash
GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -o bin/mediadash-client ./cmd/mediadash-client
```

### Build and Deploy Script

```bash
./build-deploy.sh build           # Build ARM binary
./build-deploy.sh deploy          # Deploy to CarThing
./build-deploy.sh build-deploy    # Build and deploy
./build-deploy.sh status          # Check deployment status
./build-deploy.sh logs            # View client logs
./build-deploy.sh --native        # Build for local machine
```

### CarThing Access

- **IP:** `172.16.42.2` (USB network)
- **User:** `root`
- **Password:** `llizardos`

### Manual Deployment

```bash
# Build ARM binary
GOOS=linux GOARCH=arm GOARM=7 go build -o bin/mediadash-client ./cmd/mediadash-client

# Copy to CarThing
scp bin/mediadash-client root@172.16.42.2:/usr/bin/

# SSH and verify
ssh root@172.16.42.2
sv status mediadash-client
```

### Running as a Service

On llizardOS, the client runs as a runit service:

```bash
# Service control
sv start mediadash-client
sv stop mediadash-client
sv restart mediadash-client
sv status mediadash-client

# View logs
tail -f /var/log/mediadash-client/current
```

### Redis Setup on CarThing

```bash
# Start Redis
sv start redis

# Check status
sv status redis

# Test connection
redis-cli ping
```

## Testing

### Interactive Command Testing

```bash
./scripts/test-commands.sh
```

This interactive script provides a menu for:
- Queuing playback commands
- Monitoring queue status
- Viewing Redis state
- Testing podcast and lyrics commands

### Manual Redis Commands

```bash
# Queue a play command
redis-cli LPUSH system:playback_cmd_q '{"action":"play","timestamp":'$(date +%s)'}'

# Queue a seek command
redis-cli LPUSH system:playback_cmd_q '{"action":"seek","value":60000,"timestamp":'$(date +%s)'}'

# Check media state
redis-cli GET media:track
redis-cli GET media:artist
redis-cli GET media:playing
redis-cli GET media:progress

# Check BLE connection
redis-cli GET system:ble_connected
redis-cli GET ble:status:connection_quality

# Check album art
redis-cli GET media:album_art_path
redis-cli KEYS "mediadash:albumart:cache:*"

# Check podcast data
redis-cli GET podcast:list
redis-cli GET podcast:recent_episodes
```

### Debug Flags

```bash
# Enable verbose lyrics logging
./bin/mediadash-client -debug-lyrics
```

## Troubleshooting

### Client Won't Connect

1. **Verify Android app is running** with BLE enabled
2. **Check Bluetooth adapter:**
   ```bash
   hciconfig
   hciconfig hci0 up
   ```
3. **Verify service UUID** matches between Android and client config
4. **Check logs for scanning errors:**
   ```bash
   journalctl -u mediadash-client -f
   ```

### Commands Not Reaching Phone

1. **Verify Redis connection:**
   ```bash
   redis-cli ping
   ```
2. **Check command queue:**
   ```bash
   redis-cli LRANGE system:playback_cmd_q 0 -1
   ```
3. **Monitor BLE status:**
   ```bash
   redis-cli GET ble:status:connected
   redis-cli GET ble:status:commands_processed
   redis-cli GET ble:status:commands_failed
   ```
4. **Check rate limiting** in logs (15ms minimum interval)

### Album Art Not Appearing

1. **Verify cache directory exists:**
   ```bash
   ls -la /var/mediadash/album_art_cache/
   ```
2. **Check Redis cache:**
   ```bash
   redis-cli KEYS "mediadash:albumart:cache:*"
   ```
3. **Verify hash algorithm** - must be decimal CRC32, not hex
4. **Check for transfer errors** in logs (chunk validation, CRC32 mismatch)

### ATT Error 0x0e (Resource Exhaustion)

1. **Increase write interval** in config (try 20ms or 25ms)
2. **Restart Android app** to clear BLE resources
3. **Disable Android battery optimization** for MediaDash
4. **Check for notification buffer drops** in metrics

### Position Not Updating

1. **Verify TimeTracker initialization** in logs
2. **Check Android is sending position updates**
3. **Monitor progress key:**
   ```bash
   watch -n1 "redis-cli GET media:progress"
   ```
4. **Verify playback state:**
   ```bash
   redis-cli GET media:playing
   ```

### High Memory Usage

1. **Check notification buffer utilization:**
   ```bash
   # Diagnostic info includes buffer stats
   redis-cli GET ble:status:last_update
   ```
2. **Verify stale transfers are being cleaned up**
3. **Check retry queue size** in logs

## Additional Documentation

- `COMMAND_PROCESSING.md` - Detailed command queue protocol
- `ALBUM_ART_PROTOCOL_FIXES.md` - Album art compatibility fixes
- `PERFORMANCE_OPTIMIZATIONS.md` - BLE performance tuning
- `OPTIMIZATION_SUMMARY.md` - Summary of optimization changes
- `LLM_OVERVIEW.md` - Token-optimized architecture summary
- `CLAUDE.md` - Development guidelines for Claude Code

## License

See parent project for license information.
