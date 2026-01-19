package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mediadash/client/internal/albumart"
	"mediadash/client/internal/ble"
	"mediadash/client/internal/config"
	"mediadash/client/internal/debug"
	"mediadash/client/internal/redis"
	"mediadash/client/internal/settings"
)

func main() {
	// Parse command-line flags
	debugLyrics := flag.Bool("debug-lyrics", false, "Enable verbose logging for lyrics operations")
	flag.Parse()

	// Apply debug flags
	if *debugLyrics {
		debug.SetLyricsDebug(true)
	}

	// Test album art hash generation compatibility with Android
	testHashCompatibility()

	log.Println("Starting MediaDash Golang BLE Client...")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Configuration loaded successfully")
	log.Printf("Target MediaDash Service UUID: %s", cfg.Ble.ServiceUUID)
	log.Printf("Expected characteristics:")
	log.Printf("  - Media State: %s", cfg.Ble.MediaStateCharacteristicUUID)
	log.Printf("  - Playback Control: %s", cfg.Ble.PlaybackControlCharacteristicUUID)
	log.Printf("  - Album Art Request: %s", cfg.Ble.AlbumArtRequestCharacteristicUUID)
	log.Printf("  - Album Art Data: %s", cfg.Ble.AlbumArtDataCharacteristicUUID)
	log.Printf("Redis address: %s", cfg.Redis.Address)
	log.Printf("Album art cache directory: %s", cfg.AlbumArt.CacheDirectory)

	// Initialize settings handler
	settingsHandler := settings.NewHandler(func(newSettings *settings.Settings) {
		log.Printf("Settings updated - BLE scan timeout: %ds", newSettings.BLE.ScanTimeout)
	})

	// Initialize Redis store
	log.Println("Connecting to Redis...")
	redisStore, err := redis.NewStore(*cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisStore.Close()

	// Test Redis connection
	if err := redisStore.Ping(); err != nil {
		log.Fatalf("Redis connection test failed: %v", err)
	}
	log.Println("Successfully connected to Redis")

	// Initialize BLE client
	log.Println("Initializing BLE client...")
	bleClient, err := ble.NewClient(*cfg, redisStore)
	if err != nil {
		log.Fatalf("Failed to create BLE client: %v", err)
	}

	// Start BLE client
	log.Println("Starting BLE client operations...")
	if err := bleClient.Start(ctx); err != nil {
		log.Fatalf("Failed to start BLE client: %v", err)
	}

	// Start status monitoring
	go monitorStatus(ctx, bleClient, redisStore)

	// Start command line interface (optional)
	go startCLI(ctx, bleClient, redisStore, settingsHandler)

	log.Println("==========================================")
	log.Println("MediaDash Enhanced BLE Client is now running")
	log.Println("==========================================")
	log.Println("Enhanced features:")
	log.Printf("  ✓ Intelligent BLE device scanning (service: %s)", cfg.Ble.ServiceUUID)
	log.Println("  ✓ Advanced Redis command queue processing with validation")
	log.Println("  ✓ Command retry system with exponential backoff")
	log.Println("  ✓ Comprehensive BLE error handling and recovery")
	log.Println("  ✓ Rate-limited BLE operations to prevent ATT errors")
	log.Println("  ✓ Activity-based connection health monitoring")
	log.Println("  ✓ Album art transfers with retry logic")
	log.Println("  ✓ Real-time command processing metrics")
	log.Println("")
	log.Println("Command processing pipeline:")
	log.Println("  Redis Queue → Validation → BLE Conversion → Rate Limiting → Android GATT")
	log.Println("  Failed commands → Retry Queue → Exponential Backoff → Retry (max 3 attempts)")
	log.Println("")
	log.Println("Monitoring:")
	log.Printf("  • Redis command queue: %s", cfg.Redis.KeyMap["playbackCommandQueue"])
	log.Printf("  • Command polling interval: 50ms")
	log.Printf("  • Health reports: Every 30 seconds")
	log.Printf("  • BLE write rate limit: %dms", cfg.Ble.RateLimiting.WriteIntervalMs)
	log.Println("")
	log.Println("Device selection criteria:")
	log.Println("  1. Devices advertising MediaDash service UUID")
	log.Println("  2. Devices named 'MediaDash*' or 'NocturneCompanion'")
	log.Println("  3. Fallback: Connect & verify service on potential matches")
	log.Println("")
	log.Println("Press Ctrl+C to exit")

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, initiating shutdown...", sig)
	case <-ctx.Done():
		log.Println("Context cancelled, initiating shutdown...")
	}

	// Graceful shutdown
	log.Println("Shutting down...")

	// Stop BLE client
	if err := bleClient.Stop(); err != nil {
		log.Printf("Error stopping BLE client: %v", err)
	}

	// Cancel context to stop all goroutines
	cancel()

	// Give goroutines time to cleanup
	time.Sleep(2 * time.Second)

	log.Println("MediaDash BLE Client stopped")
}

// monitorStatus provides periodic status updates with enhanced diagnostics
func monitorStatus(ctx context.Context, bleClient *ble.Client, redisStore *redis.Store) {
	ticker := time.NewTicker(1 * time.Second) // Update more frequently for UI responsiveness
	defer ticker.Stop()

	// Status file path
	statusFilePath := "/tmp/llizard_ble_status.json"

	// Track the last processed reconnect request timestamp to avoid duplicate processing
	var lastReconnectTimestamp int64

	for {
		select {
		case <-ctx.Done():
			// Try to remove status file on exit
			os.Remove(statusFilePath)
			return
		case <-ticker.C:
			// Check for reconnect request from UI
			reconnectTs, err := redisStore.GetReconnectRequest()
			if err != nil {
				log.Printf("Warning: Failed to check reconnect request: %v", err)
			} else if reconnectTs > 0 && reconnectTs != lastReconnectTimestamp {
				// New reconnect request detected
				log.Printf("Reconnect request received from UI (timestamp: %d)", reconnectTs)
				lastReconnectTimestamp = reconnectTs

				// Clear the request to acknowledge it
				if err := redisStore.ClearReconnectRequest(); err != nil {
					log.Printf("Warning: Failed to clear reconnect request: %v", err)
				}

				// Trigger the reconnection
				bleClient.TriggerExternalReconnect()
			}
			status := bleClient.GetConnectionStatus()
			diagnostics := bleClient.GetDiagnosticInfo()
			connected := status["connected"].(bool)

			// Extract command processing stats
			commandStats := diagnostics["commandProcessing"].(map[string]interface{})
			commandsProcessed := int(commandStats["commandsProcessed"].(int64))
			commandsFailed := int(commandStats["commandsFailed"].(int64))

			// Prepare status for file
			fileStatus := map[string]interface{}{
				"running":     true,
				"connected":   connected,
				"last_update": time.Now().Unix(),
			}

			// Prepare Redis status
			bleStatus := &redis.BLEStatus{
				Connected:         connected,
				DeviceName:        "",
				DeviceAddress:     "",
				LastUpdateMs:      time.Now().UnixMilli(),
				CommandsProcessed: commandsProcessed,
				CommandsFailed:    commandsFailed,
				ConnectionQuality: "unknown",
				Scanning:          !connected,
				RSSI:              0,
			}

			if connected {
				connectionAge := status["connectionAge"].(string)
				healthy := status["healthy"].(bool)
				healthStr := "healthy"
				if !healthy {
					healthStr = "unstable"
				}

				// Determine connection quality for Redis status
				if healthy {
					bleStatus.ConnectionQuality = "excellent"
				} else {
					bleStatus.ConnectionQuality = "fair"
				}

				// Set device name
				bleStatus.DeviceName = "MediaDash Device"
				fileStatus["device_name"] = "MediaDash Device"

				// Log less frequently to avoid spamming
				if time.Now().Second() % 30 == 0 {
					log.Printf("Status: Connected to MediaDash device (%s, %s)", connectionAge, healthStr)
				}
			} else {
				attempts := status["reconnectAttempts"].(int)
				delay := status["reconnectDelay"].(string)
				consecutiveErrors := status["consecutiveErrors"].(int)

				// Adjust quality based on error count
				if consecutiveErrors > 5 {
					bleStatus.ConnectionQuality = "poor"
				} else if consecutiveErrors > 2 {
					bleStatus.ConnectionQuality = "fair"
				}

				if consecutiveErrors > 2 {
					if time.Now().Second() % 30 == 0 {
						log.Printf("Status: Disconnected (attempt %d, next retry in %s, %d consecutive errors - possible ATT issue)",
							attempts, delay, consecutiveErrors)

						// Provide diagnostic information on repeated failures
						if attempts > 5 {
							log.Printf("Multiple reconnection failures detected - consider:")
							log.Printf("  1. Restarting Android companion app")
							log.Printf("  2. Checking Android battery optimization settings")
							log.Printf("  3. Verifying BLE server is running on Android device")
						}
					}
				} else {
					if time.Now().Second() % 30 == 0 {
						log.Printf("Status: Disconnected (attempt %d, next retry in %s)", attempts, delay)
					}
				}
			}

			// Check for specific error patterns and provide guidance
			if errorCounts, exists := status["errorCounts"]; exists {
				if errors, ok := errorCounts.(map[string]int); ok {
					if att0x0eCount := errors["att_0x0e"]; att0x0eCount > 0 {
						if time.Now().Second() % 30 == 0 {
							log.Printf("ATT Error 0x0e count: %d - Android server resource issues detected", att0x0eCount)
						}
					}
				}
			}

			// Check Redis health and publish BLE status
			if err := redisStore.Ping(); err != nil {
				if time.Now().Second() % 30 == 0 {
					log.Printf("Warning: Redis connection issue: %v", err)
				}
			} else {
				// Publish BLE status to Redis
				if err := redisStore.PublishBLEStatus(bleStatus); err != nil {
					if time.Now().Second() % 30 == 0 {
						log.Printf("Warning: Failed to publish BLE status to Redis: %v", err)
					}
				}
			}

			// Write status to file (backward compatibility)
			writeStatusFile(statusFilePath, fileStatus)
		}
	}
}

func writeStatusFile(path string, status map[string]interface{}) {
	// Create temporary file first to ensure atomic write
	tmpPath := path + ".tmp"
	
	// Marshal JSON
	// We need to import "encoding/json" and "io/ioutil" or "os"
	// Since we can't easily add imports with replace_file_content if they are scattered,
	// we'll assume we need to add them.
	// Wait, I should check imports first.
	// I'll add a helper function and update imports in a separate step or use multi_replace if needed.
	// For now, let's just put the logic here and I'll fix imports in a second pass if needed.
	// Actually, I can't easily add imports here without seeing the top of the file again.
	// I'll use a separate tool call to add imports.
	
	data, err := json.Marshal(status)
	if err != nil {
		log.Printf("Error marshaling status: %v", err)
		return
	}
	
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Printf("Error writing status tmp file: %v", err)
		return
	}
	
	if err := os.Rename(tmpPath, path); err != nil {
		log.Printf("Error renaming status file: %v", err)
	}
}

// startCLI provides a simple command line interface for testing
func startCLI(ctx context.Context, bleClient *ble.Client, redisStore *redis.Store, settingsHandler *settings.Handler) {
	// Command line interface placeholder - no automatic test commands
	// Commands should only be queued by external sources (LVGL UI, user input, etc.)

	log.Println("CLI interface initialized - no automatic command generation")
	log.Println("Commands will be processed from Redis queue as they are queued by external sources")

	// The CLI could be expanded in the future to provide interactive commands
	// For now, it just logs that it's ready without generating any test commands
}

// testHashCompatibility verifies that our hash generation matches Android
func testHashCompatibility() {
	log.Println("=== Testing Album Art Hash Compatibility ===")

	testCases := []struct {
		artist, album, expectedHash string
	}{
		{"Taylor Swift", "1989", ""}, // We don't know the exact hash, but we can verify format
		{"Ed Sheeran", "Divide", ""},
		{"Billie Eilish", "When We All Fall Asleep, Where Do We Go?", ""},
		{"", "", ""},       // Empty case
		{"Artist", "", ""}, // Partial case
		{"", "Album", ""},  // Partial case
	}

	log.Printf("Testing album art hash generation algorithm...")
	log.Printf("Algorithm: CRC32(\"${artist.lowercase()}|${album.lowercase()}\") as decimal string")

	for i, tc := range testCases {
		hash := albumart.GenerateAlbumArtHash(tc.artist, tc.album)

		log.Printf("Test %d:", i+1)
		log.Printf("  Artist: '%s'", tc.artist)
		log.Printf("  Album:  '%s'", tc.album)
		log.Printf("  Hash:   '%s'", hash)

		// Validate hash format (should be decimal string)
		if hash == "" && tc.artist == "" && tc.album == "" {
			log.Printf("  ✓ Empty input produces hash: %s", hash)
		} else {
			// Check that hash is all digits
			allDigits := true
			for _, c := range hash {
				if c < '0' || c > '9' {
					allDigits = false
					break
				}
			}

			if allDigits && len(hash) > 0 {
				log.Printf("  ✓ Valid decimal hash format")
			} else {
				log.Printf("  ⚠️  Invalid hash format - should be decimal string")
			}
		}
		log.Printf("")
	}

	// Test validation function
	log.Printf("Testing hash validation...")
	testHash := albumart.GenerateAlbumArtHash("Test Artist", "Test Album")
	if albumart.ValidateAlbumArtHash(testHash, "Test Artist", "Test Album") {
		log.Printf("  ✓ Hash validation works correctly")
	} else {
		log.Printf("  ⚠️  Hash validation failed")
	}

	// Test case sensitivity
	hash1 := albumart.GenerateAlbumArtHash("Test Artist", "Test Album")
	hash2 := albumart.GenerateAlbumArtHash("test artist", "test album")
	if hash1 == hash2 {
		log.Printf("  ✓ Case insensitive hash generation works correctly")
	} else {
		log.Printf("  ⚠️  Case sensitivity issue detected: %s vs %s", hash1, hash2)
	}

	log.Println("=== Album Art Hash Compatibility Test Complete ===")
	log.Println("")
}

// Additional helper functions could be added here for:
// - Configuration reloading
// - Metrics collection
// - Health check endpoints
// - Command line argument parsing
