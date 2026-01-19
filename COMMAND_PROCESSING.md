# MediaDash Enhanced BLE Client - Command Processing

This document describes the enhanced Redis command queue monitoring and BLE command forwarding system implemented in the MediaDash golang_ble_client.

## Overview

The enhanced system provides a robust pipeline for processing media control commands from the LVGL UI through Redis to the Android GATT server via BLE:

```
LVGL UI → Redis Queue → Validation → BLE Conversion → Rate Limiting → Android GATT
                           ↓ (on failure)
                     Retry Queue → Exponential Backoff → Retry (max 3 attempts)
```

## Key Features

### 1. Enhanced Command Validation
- **Command Structure Validation**: Ensures commands have required fields (action, timestamp for logging)
- **Action Type Validation**: Only accepts known actions (play, pause, next, previous, volume, seek, stop, toggle)
- **Value Range Validation**: Validates ranges for volume (0-100) and seek positions (≥0)
- **Comprehensive Error Messages**: Provides clear feedback on validation failures

### 2. Intelligent Retry System
- **Automatic Retry**: Failed commands are queued for retry with exponential backoff
- **Error Classification**: Distinguishes between retryable (connection issues) and non-retryable (validation) errors
- **Maximum Attempts**: Each command gets up to 3 retry attempts
- **Backoff Strategy**: Delays increase exponentially: 2s, 4s, 6s
- **Duplicate Prevention**: Prevents duplicate commands in retry queue

### 3. Advanced BLE Integration
- **Rate Limiting**: Prevents ATT error 0x0e with configurable write intervals (20ms default)
- **Connection Monitoring**: Intelligent activity-based health checking
- **Error Pattern Analysis**: Tracks and analyzes BLE error patterns
- **Flow Control**: Prevents notification buffer overflow

### 4. Comprehensive Monitoring
- **Real-time Statistics**: Tracks processed/failed commands and success rates
- **Command Latency Tracking**: Monitors timing information for debugging purposes
- **Retry Queue Monitoring**: Visibility into failed commands and retry attempts
- **Health Reports**: Periodic status reports every 30 seconds

## Configuration

The system uses embedded configuration from `internal/config/config.json`:

```json
{
  "ble": {
    "rateLimiting": {
      "writeIntervalMs": 20,
      "description": "Minimum interval between BLE writes to prevent ATT error 0x0e"
    },
    "connectionMonitoring": {
      "healthCheckIntervalMinutes": 1,
      "activityTimeoutMinutes": 1,
      "description": "Only perform connection health checks if no activity for specified timeout"
    }
  },
  "redis": {
    "keyMap": {
      "playbackCommandQueue": "system:playback_cmd_q"
    }
  }
}
```

## Command Format

Commands in the Redis queue use JSON format:

```json
{
  "action": "play|pause|next|previous|volume|seek|stop|toggle",
  "value": 0,         // Optional: volume level (0-100) or seek position (ms)
  "timestamp": 1234567890  // Unix timestamp
}
```

### Supported Commands

| Command | Description | Value Range | Example |
|---------|-------------|-------------|---------|
| `play` | Start playback | N/A | `{"action":"play","timestamp":1234567890}` |
| `pause` | Pause playback | N/A | `{"action":"pause","timestamp":1234567890}` |
| `next` | Next track | N/A | `{"action":"next","timestamp":1234567890}` |
| `previous` | Previous track | N/A | `{"action":"previous","timestamp":1234567890}` |
| `volume` | Set volume | 0-100 | `{"action":"volume","value":75,"timestamp":1234567890}` |
| `seek` | Seek to position | ≥0 (ms) | `{"action":"seek","value":30000,"timestamp":1234567890}` |
| `stop` | Stop playback | N/A | `{"action":"stop","timestamp":1234567890}` |
| `toggle` | Toggle play/pause | N/A | `{"action":"toggle","timestamp":1234567890}` |

## Usage

### Starting the Enhanced Client

```bash
cd golang_ble_client
go run ./cmd/mediadash-client
```

The client will display enhanced startup information:

```
==========================================
MediaDash Enhanced BLE Client is now running
==========================================
Enhanced features:
  ✓ Intelligent BLE device scanning (service: 0000a0d0-0000-1000-8000-00805f9b34fb)
  ✓ Advanced Redis command queue processing with validation
  ✓ Command retry system with exponential backoff
  ✓ Comprehensive BLE error handling and recovery
  ✓ Rate-limited BLE operations to prevent ATT errors
  ✓ Activity-based connection health monitoring
  ✓ Album art transfers with retry logic
  ✓ Real-time command processing metrics

Command processing pipeline:
  Redis Queue → Validation → BLE Conversion → Rate Limiting → Android GATT
  Failed commands → Retry Queue → Exponential Backoff → Retry (max 3 attempts)

Monitoring:
  • Redis command queue: system:playback_cmd_q
  • Command polling interval: 50ms
  • Health reports: Every 30 seconds
  • BLE write rate limit: 20ms
```

### Testing Commands

Use the provided test script:

```bash
./scripts/test-commands.sh
```

This interactive script allows you to:
- Queue individual commands (play, pause, volume, etc.)
- Queue multiple test commands
- Monitor queue status
- Clear the command queue
- View current Redis state

### Queuing Commands Manually

Using Redis CLI:

```bash
# Queue a play command
redis-cli lpush system:playback_cmd_q '{"action":"play","timestamp":'$(date +%s)'}'

# Queue a volume command
redis-cli lpush system:playback_cmd_q '{"action":"volume","value":75,"timestamp":'$(date +%s)'}'

# Check queue length
redis-cli llen system:playback_cmd_q

# View queued commands
redis-cli lrange system:playback_cmd_q 0 -1
```

## Monitoring and Diagnostics

### Health Reports

The client automatically reports command processing health every 30 seconds:

```
=== Command Processing Health ===
  Commands processed: 15
  Commands failed: 2
  Success rate: 88.2%
  Retry queue size: 1
  Time since last command: 5s
  Status: Active
================================
```

### Detailed Diagnostics

Access comprehensive diagnostics via the client's diagnostic methods:

```go
diagnostics := bleClient.GetDiagnosticInfo()
commandProcessing := diagnostics["commandProcessing"].(map[string]interface{})

fmt.Printf("Success Rate: %.1f%%\n", commandProcessing["successRate"].(float64))
fmt.Printf("Retry Queue: %d\n", commandProcessing["retryQueue"].(map[string]interface{})["size"].(int))
```

### Log Examples

**Successful Command Processing:**
```
Processing command: volume (value: 75, age: 123ms)
Converted command: volume -> BLE{Action: volume, Value: 75}
Rate-limited BLE write successful (45 bytes)
✓ Successfully sent command: volume
```

**Command Retry:**
```
Command queued for retry: play (reason: BLE write failed: ATT error: 0x0e)
Retrying command: play (attempt 2/3, reason: BLE write failed: ATT error: 0x0e)
✓ Command retry succeeded: play
```

**Validation Failure:**
```
Invalid command rejected: volume value out of range (0-100): 150
```

## Error Handling

### Error Classifications

**Retryable Errors:**
- ATT errors (connection issues)
- Connection timeouts
- Resource exhaustion
- Device not found

**Non-retryable Errors:**
- JSON marshaling errors
- Command validation failures
- Invalid command format

### Recovery Strategies

1. **Connection Loss**: Commands queued for retry until connection restored
2. **ATT Error 0x0e**: Rate limiting prevents resource exhaustion
3. **Validation Failures**: Commands rejected with detailed error messages
4. **Queue Overflow**: Flow control prevents system overload

## Performance Characteristics

- **Command Latency**: Typically <100ms from Redis to BLE transmission
- **Throughput**: Up to 50 commands/second (limited by BLE rate limiting)
- **Memory Usage**: Minimal overhead with bounded retry queues
- **CPU Usage**: Efficient polling with 50ms intervals
- **Recovery Time**: Exponential backoff with maximum 6s delay

## Integration Points

### LVGL UI Integration

The LVGL UI should queue commands to Redis:

```c
// Example: Queue a play command
redis_command("LPUSH system:playback_cmd_q '{\"action\":\"play\",\"timestamp\":%ld}'", time(NULL));
```

### Android GATT Server Integration

The Android server receives commands via the playback control characteristic:

```kotlin
// Commands arrive as JSON on the playback control characteristic
private fun handlePlaybackCommand(command: PlaybackCommand) {
    when (command.action) {
        "play" -> mediaController.play()
        "pause" -> mediaController.pause()
        "volume" -> mediaController.setVolume(command.value)
        // ... other commands
    }
}
```

## Best Practices

1. **Command Timestamps**: Include timestamp for debugging and logging purposes
2. **Value Validation**: Validate values before queuing to prevent rejection
3. **Queue Monitoring**: Monitor queue depth to detect processing issues
4. **Error Handling**: Check logs for validation failures and retry patterns
5. **Rate Limiting**: Respect BLE write intervals to prevent ATT errors

## Troubleshooting

### Command Processing

### High Failure Rate
- Check BLE connection stability
- Review ATT error patterns in logs
- Verify Android companion app is running
- Check battery optimization settings on Android

### Commands Not Processing
- Verify Redis connection
- Check queue key configuration
- Ensure BLE client is connected
- Review command format validation

### Slow Processing
- Monitor retry queue size
- Check BLE rate limiting settings
- Verify connection health
- Review system resource usage

## Future Enhancements

- **Priority Queuing**: Support for high-priority commands
- **Batch Processing**: Multiple commands in single BLE transaction  
- **Adaptive Rate Limiting**: Dynamic adjustment based on error rates
- **Command Deduplication**: Prevention of duplicate commands
- **Metrics Export**: Integration with monitoring systems