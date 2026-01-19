# Mercury

An always-on daemon that bridges the Janus phone companion app and the llizard CarThing UI over Bluetooth Low Energy. It uses Redis as the contract between BLE and the UI—connection health, media metadata, album art, podcast data, and user commands all flow through Redis keys.

## Overview

Mercury acts as a bridge between:
- **Phone** (Janus app - Android now, iOS coming soon) - Sends media state, album art, and podcast data via BLE
- **CarThing UI** (llizardgui-host) - Reads state from Redis and sends commands

All data flows through Redis, providing a clean separation between the BLE complexity and the UI layer.

**Target Platform:** ARM Linux (Spotify CarThing - ARMv7, Cortex-A7)
**BLE Stack:** TinyGo Bluetooth (`tinygo.org/x/bluetooth`)
**Database:** Redis for state synchronization

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

### Build and Deploy Script
```bash
./build-deploy.sh build-deploy    # Build ARM binary and deploy to CarThing
./build-deploy.sh build           # Just build
./build-deploy.sh deploy          # Deploy existing binary
./build-deploy.sh status          # Check deployment status
./build-deploy.sh --native        # Build for local machine instead of ARM
```

## Architecture

### Data Flow

```
┌─────────────────┐                 ┌──────────────┐                 ┌─────────────────┐
│                 │  BLE (Notify)   │              │   Redis Keys    │                 │
│      Phone      │ ───────────────>│  Go Client   │ ───────────────>│  CarThing UI    │
│     (Janus)     │                 │              │                 │  (llizardgui)   │
│                 │<────────────────│              │<────────────────│                 │
└─────────────────┘  BLE (Write)    └──────────────┘  Redis Queue    └─────────────────┘
                    (Commands)                        (Commands)
```

#### BLE → Redis
The client subscribes to BLE characteristics and writes incoming data to Redis:
- **Media metadata** → `media:track`, `media:artist`, `media:album`, `media:playing`
- **Playback position/duration** → `media:progress`, `media:duration`
- **Album art** → `/var/mediadash/album_art_cache/` + `media:art_path`
- **Podcast data** → `podcast:list`, `podcast:recent`, `podcast:episodes:<hash>`
- **Connection status** → `system:ble_connected`, `system:ble_name`, `ble:status:*`

#### Redis → BLE
The UI enqueues commands in Redis; the client dequeues and forwards them via BLE:
- **Playback commands** → `system:playback_cmd_q` (play, pause, next, previous, seek, volume)
- **Album art requests** → `mediadash:albumart:request`
- **Podcast requests** → `podcast:request_q`

### Key Packages

#### `cmd/mediadash-client`
Entry point that:
- Loads embedded configuration from `internal/config/config.json`
- Initializes BLE adapter and Redis connection
- Wires graceful shutdown handlers
- Starts goroutines for BLE I/O, command processing, and status monitoring

#### `internal/ble`
Core BLE client (`client.go`):
- **Device scanning** with Janus service UUID filtering
- **Characteristic subscriptions** for media state, album art, and podcast data
- **Rate-limited writes** (15ms intervals) to prevent ATT error 0x0e
- **Connection health monitoring** with activity-based checks
- **Command retry queue** with exponential backoff
- **TimeTracker** for real-time position updates (1-second increments)
- **Chunk reassembly** for album art and podcast responses

#### `internal/redis`
Redis operations (`store.go`):
- `StoreMediaState()` - Writes track/artist/album/position/duration
- `DequeuePlaybackCommand()` - Pops commands from `system:playback_cmd_q`
- `PublishBLEStatus()` - Updates connection status for UI health indicators
- `SetAlbumArtFilePath()` - Points UI to cached album art
- `StorePodcastList()`, `StoreRecentEpisodes()`, `StorePodcastEpisodes()` - Podcast data storage
- `StoreAlbumArtCache()`, `GetAlbumArtCache()` - Album art cache management

**Compact BLE Response Types:**
- `CompactPodcastListResponse` - Podcast channel list (type 1)
- `CompactRecentEpisodesResponse` - Recent episodes across all podcasts (type 2)
- `CompactPodcastEpisodesResponse` - Paginated episodes for a specific podcast (type 3)

#### `internal/albumart`
Album art handling (`handler.go`):
- `GenerateAlbumArtHash(artist, album)` - CRC32 hash matching Android algorithm
- **Chunked transfer reassembly** with base64 decoding
- **Disk cache** at `/var/mediadash/album_art_cache/`
- **Retry manager** for failed transfers

#### `internal/config`
Configuration management:
- Embedded `config.json` via `//go:embed`
- BLE UUIDs, rate limiting parameters
- Redis key mappings
- Album art cache settings

#### `internal/settings`
Runtime settings hot-reload:
- Scan intervals, RSSI thresholds, feature flags
- No rebuild required for configuration changes

## Compact BLE Format

The client implements a bandwidth-optimized format for podcast responses to minimize BLE transmission overhead.

### 3-Byte Header Format

Podcast responses use a **3-byte header** followed by chunked JSON data:

```
[type][chunkIndex][totalChunks][data...]
```

- **Byte 0 (type):** Response type (1-3)
  - `1` = Podcast List (A-Z channel list)
  - `2` = Recent Episodes (recent episodes across all podcasts)
  - `3` = Podcast Episodes (paginated episodes for a specific podcast)
- **Byte 1 (chunkIndex):** Current chunk index (0-based)
- **Byte 2 (totalChunks):** Total number of chunks

**Legacy 2-byte header** (backwards compatibility):
```
[chunkIndex][totalChunks][data...]
```
- Detected when byte 0 is outside 1-3 range
- Used for old Android app versions

### JSON Field Compression

JSON field names are shortened to reduce payload size by ~55%:

#### Type 1: Podcast List
```json
{
  "p": [
    {"h": "abc12345", "n": "Podcast Name", "c": 50}
  ],
  "np": {"h": "abc12345", "t": "Episode Title", "i": 0}
}
```
- `p` = podcasts array
- `h` = hash (podcast ID)
- `n` = name (podcast title)
- `c` = count (episode count)
- `np` = now playing (optional)
- `t` = title (episode title)
- `i` = index (episode index)

#### Type 2: Recent Episodes
```json
{
  "e": [
    {"h": "a1b2c3d4", "c": "Channel", "t": "Title", "d": 3600, "i": 0}
  ],
  "t": 100
}
```
- `e` = episodes array
- `h` = hash (episode hash)
- `c` = channel (podcast/channel title)
- `t` = title (episode title)
- `d` = duration (in SECONDS)
- `i` = index (episode index for playback)

#### Type 3: Podcast Episodes
```json
{
  "h": "abc",
  "n": "Podcast",
  "t": 50,
  "o": 0,
  "m": true,
  "e": [
    {"h": "a1b2c3d4", "t": "Episode Title", "d": 3600}
  ]
}
```
- `h` = hash (podcast ID hash)
- `n` = name (podcast title)
- `t` = total (total episodes)
- `o` = offset (current offset)
- `m` = more (has more episodes)
- `e` = episodes array
  - `h` = hash (episode hash)
  - `t` = title (episode title)
  - `d` = duration (in SECONDS)

### Chunk Reassembly

The client maintains **separate chunk storage** for each response type to support concurrent transfers:
- `podcastListChunks` - Type 1 chunks
- `podcastRecentChunks` - Type 2 chunks
- `podcastEpisodesChunks` - Type 3 chunks

Chunks are reassembled when all pieces arrive, then the complete JSON is parsed and stored in Redis.

## Redis Key Schema

### Media State Keys
| Key | Description | Example Value |
|-----|-------------|---------------|
| `media:track` | Current track title | "Song Title" |
| `media:artist` | Current artist | "Artist Name" |
| `media:album` | Current album | "Album Name" |
| `media:playing` | Playback state | "true" or "false" |
| `media:duration` | Duration in seconds | "240" |
| `media:progress` | Position in seconds | "120" |
| `media:art_path` | File path to cached album art | "/var/mediadash/album_art_cache/1234567890.jpg" |

### BLE Connection Keys
| Key | Description | Example Value |
|-----|-------------|---------------|
| `system:ble_connected` | BLE connection status | "true" or "false" |
| `system:ble_name` | Connected device name | "Pixel 7" |
| `ble:status:connected` | Detailed connection status | "true" |
| `ble:status:device_name` | Device name | "Pixel 7" |
| `ble:status:device_address` | Device MAC address | "AA:BB:CC:DD:EE:FF" |
| `ble:status:rssi` | Signal strength | "-65" |
| `ble:status:connection_quality` | Connection quality | "good" |
| `ble:status:last_update` | Last update timestamp (ms) | "1234567890123" |
| `ble:status:commands_processed` | Total commands processed | "42" |
| `ble:status:commands_failed` | Total commands failed | "2" |

### Command Queues
| Key | Description | Type |
|-----|-------------|------|
| `system:playback_cmd_q` | Playback command queue | List (JSON objects) |
| `podcast:request_q` | Podcast info request queue | List (JSON objects) |

### Album Art Cache
| Key | Description | Type |
|-----|-------------|------|
| `mediadash:albumart:cache:<hash>` | Cached album art data | Binary (JPEG) |
| `mediadash:albumart:cache:<hash>:meta` | Album art metadata | JSON |
| `mediadash:albumart:request` | Album art request | JSON |

### Podcast Keys
| Key | Description | Type |
|-----|-------------|------|
| `podcast:list` | Compact podcast channel list (type 1) | JSON |
| `podcast:recent` | Compact recent episodes (type 2) | JSON |
| `podcast:episodes:<hash>` | Compact podcast episodes (type 3) | JSON |
| `podcast:channel_count` | Total podcast channel count | Integer |

### Legacy Podcast Keys (Deprecated)
| Key | Description |
|-----|-------------|
| `podcast:show_name` | Current podcast show name |
| `podcast:episode_title` | Current episode title |
| `podcast:episode_description` | Current episode description |
| `podcast:author` | Podcast author |
| `podcast:episode_count` | Total episodes |
| `podcast:current_index` | Current episode index |
| `podcast:episode_list` | Episode list (JSON array) |

## Command Queue Format

Commands in `system:playback_cmd_q` are JSON objects:

### Playback Commands
```json
{
  "action": "play|pause|next|previous|toggle",
  "timestamp": 1234567890
}
```

### Seek Command
```json
{
  "action": "seek",
  "value": 120,
  "timestamp": 1234567890
}
```

### Volume Command
```json
{
  "action": "volume",
  "value": 75,
  "timestamp": 1234567890
}
```

### Podcast Playback Command
```json
{
  "action": "play_podcast_episode",
  "podcastId": "abc12345",
  "episodeIndex": 3,
  "timestamp": 1234567890
}
```

### Podcast Request Commands
```json
{
  "action": "request_podcast_list",
  "timestamp": 1234567890
}
```

```json
{
  "action": "request_recent_episodes",
  "timestamp": 1234567890
}
```

```json
{
  "action": "request_podcast_episodes",
  "podcastId": "abc12345",
  "offset": 0,
  "limit": 50,
  "timestamp": 1234567890
}
```

## Configuration

Configuration is embedded in the binary via `//go:embed` in `internal/config/config.json`:

### BLE Settings
```json
{
  "ble": {
    "serviceUUID": "0000a0d0-0000-1000-8000-00805f9b34fb",
    "mediaStateCharacteristicUUID": "0000a0d1-0000-1000-8000-00805f9b34fb",
    "playbackControlCharacteristicUUID": "0000a0d2-0000-1000-8000-00805f9b34fb",
    "albumArtRequestCharacteristicUUID": "0000a0d3-0000-1000-8000-00805f9b34fb",
    "albumArtDataCharacteristicUUID": "0000a0d4-0000-1000-8000-00805f9b34fb",
    "podcastInfoCharacteristicUUID": "0000a0d5-0000-1000-8000-00805f9b34fb",
    "rateLimiting": {
      "writeIntervalMs": 15
    },
    "connectionMonitoring": {
      "healthCheckIntervalMinutes": 1,
      "activityTimeoutMinutes": 1
    }
  }
}
```

### Redis Settings
```json
{
  "redis": {
    "address": "127.0.0.1:6379",
    "keyMap": {
      "trackTitle": "media:track",
      "artistName": "media:artist",
      "albumName": "media:album",
      "isPlaying": "media:playing",
      "durationMs": "media:duration",
      "progressMs": "media:progress",
      "albumArtPath": "media:art_path",
      "bleConnected": "system:ble_connected",
      "bleName": "system:ble_name",
      "playbackCommandQueue": "system:playback_cmd_q",
      "podcastRequestQueue": "podcast:request_q"
    }
  }
}
```

### Album Art Settings
```json
{
  "albumArt": {
    "cacheDirectory": "/var/mediadash/album_art_cache",
    "image": {
      "format": "jpg",
      "maxWidth": 200,
      "maxHeight": 200,
      "quality": 75
    },
    "ble": {
      "chunkSize": 300
    }
  }
}
```

## Deployment

### CarThing Device Access
- **IP:** `172.16.42.2`
- **User:** `root`
- **Password:** `llizardOS`

### Manual Deployment
```bash
# Build for ARM
GOOS=linux GOARCH=arm GOARM=7 go build -o bin/mediadash-client ./cmd/mediadash-client

# Copy to CarThing
scp bin/mediadash-client root@172.16.42.2:/tmp/

# SSH into CarThing and run
ssh root@172.16.42.2
cd /tmp
./mediadash-client
```

### Automated Deployment
```bash
./build-deploy.sh build-deploy
```

This script:
1. Builds the ARM binary
2. Copies it to `/tmp/` on the CarThing
3. Ensures Redis is running
4. Starts the client

### Redis Setup on CarThing

Redis must be running for the client to function:

```bash
# Start Redis service
sshpass -p llizardOS ssh root@172.16.42.2 "sv start redis"

# Check Redis status
sshpass -p llizardOS ssh root@172.16.42.2 "sv status redis"

# Test Redis connection
sshpass -p llizardOS ssh root@172.16.42.2 "redis-cli ping"
```

### Running as a Service

The client can be configured to run as a runit service on the CarThing for automatic startup.

## Key Implementation Patterns

### BLE Rate Limiting

All BLE writes go through `rateLimitedWrite()` to prevent **ATT error 0x0e** (resource exhaustion). The default interval is **15ms** between writes, providing a safety margin for Android BLE stack variability and battery optimization delays.

### Album Art Protocol

Android sends album art as base64-encoded chunks. The hash is **CRC32** of `"artist.lowercase()|album.lowercase()"` as a **decimal string** (not hex). See `ALBUM_ART_PROTOCOL_FIXES.md` for details.

### TimeTracker

Maintains real-time playback position by incrementing locally (1-second ticks) between Android updates. This prevents position jumpback from stale duplicate BLE notifications. The tracker:
- Only starts after receiving initial position data from Android
- Increments position every second when playing
- Stops when paused or at end of track
- Resets on track changes

### Command Retry

Failed commands are queued with **exponential backoff** (2s, 4s, 6s) up to **3 attempts**. Retryable errors include:
- ATT failures (error 0x0e)
- BLE connection issues
- Rate limit violations

### Connection Health Monitoring

The client monitors connection health using **activity-based checks**:
- Tracks last send/receive activity
- Only performs health checks if no activity for 1 minute
- Avoids unnecessary connection tests during active streaming
- Updates `ble:status:*` keys with connection quality metrics

## Testing Commands

### Interactive Command Testing
```bash
./scripts/test-commands.sh
```

This script provides an interactive menu for testing playback commands via Redis.

### Manual Redis Testing
```bash
# Queue a play command
redis-cli LPUSH system:playback_cmd_q '{"action":"play","timestamp":1234567890}'

# Queue a seek command
redis-cli LPUSH system:playback_cmd_q '{"action":"seek","value":60,"timestamp":1234567890}'

# Check media state
redis-cli GET media:track
redis-cli GET media:artist
redis-cli GET media:playing

# Check BLE connection
redis-cli GET system:ble_connected
redis-cli GET system:ble_name
```

## Documentation

- **`LLM_OVERVIEW.md`** - High-level architecture summary
- **`COMMAND_PROCESSING.md`** - Command queue protocol details
- **`ALBUM_ART_PROTOCOL_FIXES.md`** - Album art compatibility fixes
- **`PERFORMANCE_OPTIMIZATIONS.md`** - BLE performance tuning
- **`CLAUDE.md`** - Development guidelines for Claude Code

## Troubleshooting

### Client won't connect to phone
1. Ensure Janus app is running and BLE is enabled
2. Check that CarThing Bluetooth adapter is working: `hciconfig`
3. Verify the service UUID matches between Janus and client
4. Check client logs for scanning/pairing errors

### Commands not reaching phone
1. Verify Redis is running: `redis-cli ping`
2. Check command queue: `redis-cli LRANGE system:playback_cmd_q 0 -1`
3. Monitor BLE status: `redis-cli GET ble:status:connected`
4. Check rate limiting isn't blocking writes (15ms minimum interval)

### Album art not appearing
1. Verify cache directory exists: `/var/mediadash/album_art_cache/`
2. Check for album art hash in Redis: `redis-cli KEYS mediadash:albumart:cache:*`
3. Monitor album art transfer logs for chunk reassembly issues
4. Ensure hash algorithm matches Android (CRC32 decimal, not hex)

### Position not updating
1. Check TimeTracker initialization in logs
2. Verify Android is sending position updates
3. Monitor `media:progress` key: `redis-cli GET media:progress`
4. Ensure playback state is correct: `redis-cli GET media:playing`

## License

See parent project for license information.
