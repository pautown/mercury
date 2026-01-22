package ble

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mediadash/client/internal/albumart"
	"mediadash/client/internal/config"
	"mediadash/client/internal/debug"
	"mediadash/client/internal/redis"

	"tinygo.org/x/bluetooth"
)

// Client represents a BLE client for MediaDash integration
type Client struct {
	cfg             config.Config
	adapter         *bluetooth.Adapter
	device          *bluetooth.Device
	service         *bluetooth.DeviceService
	characteristics map[string]*bluetooth.DeviceCharacteristic
	redisStore      *redis.Store
	albumHandler    *albumart.Handler

	// Connection state
	connected     bool
	reconnectChan chan struct{}
	stopChan      chan struct{}
	mutex         sync.RWMutex

	// State change detection
	lastMediaState        *MediaStateUpdate
	mediaStateMutex       sync.RWMutex
	lastAlbumArtHash      string
	albumArtHashMutex     sync.RWMutex
	duplicateCounter      int
	duplicateCounterMutex sync.Mutex

	// Lyrics request tracking
	lastLyricsArtist string
	lastLyricsTrack  string
	lyricsMutex      sync.RWMutex

	// Connection monitoring
	connectionHealthy   int32 // atomic boolean for connection health (0=false, 1=true)
	lastHealthCheck     time.Time
	connectionStartTime time.Time

	// Activity tracking for intelligent connection testing
	lastSendActivity    time.Time
	lastReceiveActivity time.Time
	activityMutex       sync.RWMutex

	// Rate limiting for BLE writes
	rateLimiter   chan struct{}
	lastWriteTime time.Time
	writeMutex    sync.Mutex

	// Error tracking
	lastError         error
	consecutiveErrors int
	attErrorCount     map[string]int

	// Reconnection parameters
	maxReconnectDelay time.Duration
	reconnectDelay    time.Duration
	reconnectAttempts int

	// Flow control for notifications - separate channels prevent starvation
	// Media state uses smaller buffer (playback updates are smaller, higher priority)
	// Album art uses larger buffer (handles full 2MB transfers with ~4000 chunks)
	mediaStateFlowControl chan struct{}
	albumArtFlowControl   chan struct{}

	// Notification metrics for monitoring drop rates and system health
	// NOTE: Using pointer to heap-allocated struct to ensure 64-bit alignment on 32-bit ARM
	notificationMetrics *notificationMetricsData

	// Command processing metrics
	// NOTE: Using pointer to heap-allocated struct to ensure 64-bit alignment on 32-bit ARM
	commandProcessingStats *commandProcessingStatsData

	// Command retry queue for failed commands
	retryQueue       []retry
	retryQueueMutex  sync.Mutex
	maxRetryAttempts int
	retryDelay       time.Duration

	// Time tracking for position updates
	timeTracker       *TimeTracker
	timeTrackerMutex  sync.RWMutex
}

// CommandRetry represents a command awaiting retry
type retry struct {
	command   *redis.PlaybackCommand
	attempts  int
	nextRetry time.Time
	reason    string
}

// notificationMetricsData tracks notification drop rates and system health
// Allocated on heap to ensure 64-bit alignment on 32-bit ARM
type notificationMetricsData struct {
	// int64 fields MUST be first for 64-bit alignment on 32-bit ARM
	mediaStateReceived int64
	mediaStateDropped  int64
	albumArtReceived   int64
	albumArtDropped    int64

	// Other fields after int64s
	mediaStateLastDrop time.Time
	albumArtLastDrop   time.Time
	dropAlertThreshold float64 // Alert if drop rate exceeds this percentage
	lastAlertTime      time.Time
	alertCooldown      time.Duration
	mutex              sync.RWMutex // Protects time fields only; counters use atomic ops
}

// commandProcessingStatsData tracks command processing metrics
// Allocated on heap to ensure 64-bit alignment on 32-bit ARM
type commandProcessingStatsData struct {
	// int64 fields MUST be first for 64-bit alignment on 32-bit ARM
	commandsProcessed int64
	commandsFailed    int64

	// Other fields after int64s
	lastCommandTime time.Time
	mutex           sync.RWMutex
}

// TimeTracker manages time tracking for playback position updates
type TimeTracker struct {
	IsPlaying       bool
	CurrentPosition int64 // seconds (converted from Android milliseconds)
	CurrentDuration int64 // seconds (preserved from Android updates)
	LastUpdateTime  time.Time
	Ticker          *time.Ticker
	StopChan        chan struct{}
	mutex           sync.RWMutex
	redisStore      *redis.Store
	initialized     bool  // Flag to track if we've received position data from Android
}

// NewTimeTracker creates a new time tracker
func NewTimeTracker(redisStore *redis.Store) *TimeTracker {
	return &TimeTracker{
		redisStore: redisStore,
		StopChan:   make(chan struct{}),
	}
}

// Start begins time tracking with 1-second increments
// Only starts if we've received position data from Android
func (tt *TimeTracker) Start() {
	tt.mutex.Lock()
	defer tt.mutex.Unlock()

	if tt.Ticker != nil {
		return // Already started
	}

	// Only start ticker if we've been initialized with Android position data
	if !tt.initialized {
		log.Printf("Time tracker start deferred - waiting for Android position data")
		return
	}

	tt.Ticker = time.NewTicker(1 * time.Second)
	go tt.trackingLoop()
	log.Printf("Time tracker started with 1-second increments from position %ds", tt.CurrentPosition)
}

// Stop stops the time tracking
func (tt *TimeTracker) Stop() {
	tt.mutex.Lock()
	defer tt.mutex.Unlock()

	if tt.Ticker != nil {
		tt.Ticker.Stop()
		tt.Ticker = nil
	}

	select {
	case tt.StopChan <- struct{}{}:
	default:
	}

	log.Printf("Time tracker stopped")
}

// startTickerIfReady starts the ticker if we're initialized and not already started
func (tt *TimeTracker) startTickerIfReady() {
	if tt.initialized && tt.Ticker == nil {
		tt.Ticker = time.NewTicker(1 * time.Second)
		go tt.trackingLoop()
		log.Printf("Time tracker ticker started after Android position initialization at %ds", tt.CurrentPosition)
	}
}

// UpdatePosition updates the current position and sync time
// position parameter is in milliseconds from Android, converted to seconds for storage
func (tt *TimeTracker) UpdatePosition(position int64, isPlaying bool) {
	tt.mutex.Lock()
	defer tt.mutex.Unlock()

	oldPositionSeconds := tt.CurrentPosition
	oldPlaying := tt.IsPlaying

	// Convert milliseconds to seconds for internal tracking
	positionSeconds := position / 1000
	
	// Intelligent position update: avoid overwriting with stale Android positions
	// If the TimeTracker is actively running and the new position is significantly 
	// behind our current position, this is likely a stale duplicate notification
	timeSinceLastUpdate := time.Since(tt.LastUpdateTime)
	shouldUpdatePosition := true
	
	if tt.IsPlaying && tt.Ticker != nil && timeSinceLastUpdate < 5*time.Second {
		// We've been actively tracking and this update is recent
		// Check if the Android position is significantly behind our current position
		positionDiff := tt.CurrentPosition - positionSeconds
		
		if positionDiff > 0 && positionDiff <= 10 {
			// Android position is 1-10 seconds behind our tracked position
			// This is likely a stale duplicate notification - don't reset our position
			log.Printf("Time tracker position update rejected: Android position %ds is behind tracked position %ds by %ds (likely stale duplicate)",
				positionSeconds, tt.CurrentPosition, positionDiff)
			shouldUpdatePosition = false
		}
	}
	
	if shouldUpdatePosition {
		tt.CurrentPosition = positionSeconds
		log.Printf("Time tracker position updated: %ds -> %ds (from %dms), playing: %v -> %v",
			oldPositionSeconds, positionSeconds, position, oldPlaying, isPlaying)
		
		// Update Redis immediately with the converted position (in seconds)
		if err := tt.updateRedisPosition(positionSeconds); err != nil {
			log.Printf("Failed to update Redis position: %v", err)
		}
	} else {
		log.Printf("Time tracker position unchanged: keeping %ds (Android sent %ds), playing: %v -> %v",
			tt.CurrentPosition, positionSeconds, oldPlaying, isPlaying)
	}
	
	// Mark as initialized and start ticker if this is the first Android position update
	if !tt.initialized {
		tt.initialized = true
		tt.startTickerIfReady()
		log.Printf("Time tracker initialized with Android position data")
	}
	
	// Always update playing state and timestamp, even if position is rejected
	tt.IsPlaying = isPlaying
	tt.LastUpdateTime = time.Now()
}

// UpdateDuration updates the track duration and preserves it for future position updates
// duration parameter is in milliseconds from Android, converted to seconds for storage
func (tt *TimeTracker) UpdateDuration(duration int64) {
	tt.mutex.Lock()
	defer tt.mutex.Unlock()

	oldDurationSeconds := tt.CurrentDuration
	// Convert milliseconds to seconds for internal tracking
	durationSeconds := duration / 1000
	tt.CurrentDuration = durationSeconds

	log.Printf("Time tracker duration updated: %ds -> %ds (from %dms)",
		oldDurationSeconds, durationSeconds, duration)
}

// trackingLoop runs the time tracking loop
func (tt *TimeTracker) trackingLoop() {
	log.Printf("Time tracking loop started")
	for {
		select {
		case <-tt.StopChan:
			log.Printf("Time tracking loop stopped")
			return
		case <-tt.Ticker.C:
			tt.tick()
		}
	}
}

// tick processes one time tracking tick
func (tt *TimeTracker) tick() {
	tt.mutex.Lock()
	defer tt.mutex.Unlock()

	if !tt.IsPlaying {
		return // Don't increment when paused
	}

	// Increment position by 1 second (TimeTracker now works in seconds)
	tt.CurrentPosition += 1

	// Update Redis with the new position (already in seconds)
	if err := tt.updateRedisPosition(tt.CurrentPosition); err != nil {
		log.Printf("Failed to update Redis position during tick: %v", err)
	}
}

// updateRedisPosition updates the current position in Redis while preserving duration
// position parameter is in seconds (for UI MM:SS formatting)
func (tt *TimeTracker) updateRedisPosition(position int64) error {
	if tt.redisStore == nil {
		return fmt.Errorf("redis store not available")
	}

	// Use the new UpdatePositionOnly method to avoid overwriting duration
	err := tt.redisStore.UpdatePositionOnly(position)
	if err != nil {
		return fmt.Errorf("failed to update Redis position: %w", err)
	}

	return nil
}

// GetCurrentPosition returns the current tracked position in seconds
func (tt *TimeTracker) GetCurrentPosition() int64 {
	tt.mutex.RLock()
	defer tt.mutex.RUnlock()
	return tt.CurrentPosition
}

// GetCurrentDuration returns the current tracked duration in seconds
func (tt *TimeTracker) GetCurrentDuration() int64 {
	tt.mutex.RLock()
	defer tt.mutex.RUnlock()
	return tt.CurrentDuration
}

// MediaStateUpdate represents a media state notification from the server
type MediaStateUpdate struct {
	IsPlaying     bool   `json:"isPlaying"`
	PlaybackState string `json:"playbackState"` // Android sends "playing", "paused", "stopped"
	TrackTitle    string `json:"trackTitle"`
	Artist        string `json:"artist"`       // Fixed: matches Android server @SerializedName
	Album         string `json:"album"`        // Fixed: matches Android server @SerializedName
	Duration      int64  `json:"duration"`     // Fixed: matches Android server @SerializedName
	Position      int64  `json:"position"`     // Fixed: matches Android server @SerializedName
	Volume        int    `json:"volume"`
	AlbumArtHash  string `json:"albumArtHash,omitempty"`
	MediaChannel  string `json:"mediaChannel,omitempty"` // App being controlled (e.g., "Spotify", "YouTube Music")
}

// PlaybackCommand represents a command to send to the server
type PlaybackCommand struct {
	Action       string `json:"action"`
	Value        int64  `json:"value,omitempty"`
	PodcastId    string `json:"podcastId,omitempty"`
	EpisodeHash  string `json:"episodeHash,omitempty"` // Episode hash (CRC32 of feedUrl+pubDate+duration) for play_episode
	EpisodeIndex int    `json:"episodeIndex"`          // DEPRECATED: use EpisodeHash. Note: no omitempty since 0 is valid
	Offset       int    `json:"offset"`                // For pagination - no omitempty since 0 is valid
	Limit        int    `json:"limit,omitempty"`       // For pagination (request_podcast_episodes)
	Channel      string `json:"channel,omitempty"`     // Media channel name for select_media_channel
}

// Note: AlbumArtRequestCommand removed - Android now proactively sends album art

// NewClient creates a new BLE client
func NewClient(cfg config.Config, store *redis.Store) (*Client, error) {
	// Get default adapter
	adapter := bluetooth.DefaultAdapter

	// Enable BLE stack
	err := adapter.Enable()
	if err != nil {
		return nil, fmt.Errorf("failed to enable BLE: %w", err)
	}

	// Initialize album art handler with comprehensive validation and retry logic
	// Callback is called SYNCHRONOUSLY to ensure cache coherence between disk and Redis
	albumHandler, err := albumart.NewHandler(cfg.AlbumArt.CacheDirectory, func(hash string, data []byte) error {
		var lastErr error

		// Cache album art in Redis when transfer completes
		if err := store.StoreAlbumArtCache(hash, data); err != nil {
			log.Printf("Failed to cache album art in Redis: %v", err)
			lastErr = err
		}

		// Update Redis with album art file path for LVGL UI
		artFilePath := filepath.Join(cfg.AlbumArt.CacheDirectory, hash+".webp")
		if err := store.SetAlbumArtFilePath(artFilePath); err != nil {
			log.Printf("Failed to update Redis album art path: %v", err)
			lastErr = err
		} else {
			log.Printf("Updated Redis album art path: %s", artFilePath)
		}

		return lastErr // Return last error for logging; disk cache is still valid
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create album art handler: %w", err)
	}

	client := &Client{
		cfg:                     cfg,
		adapter:                 adapter,
		characteristics:         make(map[string]*bluetooth.DeviceCharacteristic),
		redisStore:              store,
		albumHandler:            albumHandler,
		reconnectChan:           make(chan struct{}, 1),
		stopChan:                make(chan struct{}),
		maxReconnectDelay:       60 * time.Second,
		reconnectDelay:          1 * time.Second,
		attErrorCount:           make(map[string]int),
		mediaStateFlowControl:   make(chan struct{}, 200),  // Handles ~6 seconds of rapid playback updates (30/sec)
		albumArtFlowControl:     make(chan struct{}, 4096), // Handles full 2MB transfer (~4000 chunks at 512B)
		rateLimiter:             make(chan struct{}, 1),    // Rate limiter for BLE writes
		maxRetryAttempts:        3,
		retryDelay:              2 * time.Second,
		timeTracker:             NewTimeTracker(store),
		// Heap-allocate metrics structs for 64-bit alignment on 32-bit ARM
		notificationMetrics:    &notificationMetricsData{},
		commandProcessingStats: &commandProcessingStatsData{},
	}

	// Initialize rate limiter token
	client.rateLimiter <- struct{}{}

	// Initialize notification metrics with alert thresholds
	client.notificationMetrics.dropAlertThreshold = 5.0 // Alert if >5% drop rate
	client.notificationMetrics.alertCooldown = 30 * time.Second

	// Set up album art request functionality for error recovery scenarios
	// While Android proactively sends album art, we need request capability for:
	// 1. Recovery from failed transfers
	// 2. Cache misses on client restart
	// 3. Handling network interruptions
	// 4. Selective chunk retry for partial failures
	albumHandler.SetRequestFunc(func(hash string, chunks []uint16) error {
		return client.requestAlbumArtWithChunks(hash, chunks)
	})

	// Perform startup cache reconciliation between disk and Redis
	// This ensures the UI sees all cached album art even after Redis restart
	go func() {
		result, err := albumHandler.ReconcileCaches(
			// Get all hashes from Redis
			func() ([]string, error) {
				return store.GetAllCachedAlbumArtHashes()
			},
			// Sync disk file to Redis
			func(hash string, data []byte) error {
				return store.StoreAlbumArtCache(hash, data)
			},
			// Remove orphaned Redis entry (optional - only removes if disk doesn't have it)
			func(hash string) error {
				return store.DeleteAlbumArtCache(hash)
			},
		)
		if err != nil {
			log.Printf("Cache reconciliation failed: %v", err)
		} else if result != nil {
			if result.SyncedToRedis > 0 || result.RemovedFromRedis > 0 {
				log.Printf("Cache reconciliation: synced %d to Redis, removed %d orphans",
					result.SyncedToRedis, result.RemovedFromRedis)
			}
		}
	}()

	return client, nil
}

// Start begins the BLE client operations
func (c *Client) Start(ctx context.Context) error {
	log.Println("Starting BLE client...")

	// Start the main client loop
	go c.clientLoop(ctx)

	// Start command processing
	go c.processCommands(ctx)

	// Start album art request processing
	go c.processAlbumArtRequests(ctx)

	// Start retry processor for failed commands
	go c.processRetryQueue(ctx)

	// Start periodic cleanup
	go c.periodicCleanup(ctx)

	// TimeTracker will be started automatically when first Android position data is received

	// Trigger initial connection
	c.triggerReconnect()

	return nil
}

// Stop stops the BLE client
func (c *Client) Stop() error {
	log.Println("Stopping BLE client...")

	// Stop time tracker
	c.timeTrackerMutex.Lock()
	if c.timeTracker != nil {
		c.timeTracker.Stop()
	}
	c.timeTrackerMutex.Unlock()

	close(c.stopChan)

	c.mutex.Lock()
	wasConnected := c.connected

	if c.device != nil && c.connected {
		if err := c.device.Disconnect(); err != nil {
			log.Printf("Failed to disconnect device: %v", err)
		}
	}

	c.connected = false
	c.mutex.Unlock()

	// Update Redis with disconnection status if we were connected
	if wasConnected {
		if err := c.redisStore.SetBLEConnectionStatus(false, ""); err != nil {
			log.Printf("Warning: Failed to update Redis disconnection status on stop: %v", err)
		}
	}

	return nil
}

// clientLoop is the main client event loop
func (c *Client) clientLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-c.reconnectChan:
			if err := c.attemptConnection(ctx); err != nil {
				log.Printf("Connection attempt failed: %v", err)
				c.scheduleReconnect()
			}
		}
	}
}

// attemptConnection attempts to scan, connect, and setup characteristics
func (c *Client) attemptConnection(ctx context.Context) error {
	log.Printf("=== MediaDash BLE Client Connection Attempt ===")
	log.Printf("Target Service UUID: %s", c.cfg.Ble.ServiceUUID)
	log.Printf("Expected Device Names: MediaDash, MediaDash-Server, MediaDash-Companion")
	log.Printf("Note: LLIZARD devices are from a different project and will be ignored")

	// Parse service UUID
	serviceUUID, err := bluetooth.ParseUUID(c.cfg.Ble.ServiceUUID)
	if err != nil {
		return fmt.Errorf("invalid service UUID: %w", err)
	}

	// Start scanning
	scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var targetDevice *bluetooth.ScanResult
	var discoveredDevices []bluetooth.ScanResult

	log.Println("Starting BLE scan for MediaDash devices...")

	err = c.adapter.Scan(func(adapter *bluetooth.Adapter, result bluetooth.ScanResult) {
		localName := result.LocalName()
		address := result.Address.String()
		rssi := result.RSSI

		// Log every discovered device
		log.Printf("Discovered BLE device: %s (%s) RSSI: %d", address, localName, rssi)

		// Store device for potential service verification
		discoveredDevices = append(discoveredDevices, result)

		// Check if device advertises our service UUID in advertisement data
		// Note: TinyGo Bluetooth may not expose Services() method directly
		// We'll check for service UUIDs if the method is available, otherwise rely on name-based filtering
		log.Printf("  Advertisement payload received")

		// First try: Check advertisement data for service UUIDs (implementation-dependent)
		// This is a best-effort check as not all BLE stacks expose this information
		// TODO: If TinyGo bluetooth library adds Services() method support, enable this
		/*
			if advertServices, err := result.AdvertisementPayload.Services(); err == nil {
				log.Printf("  Advertised services: %v", advertServices)
				for _, advertUUID := range advertServices {
					if advertUUID == serviceUUID {
						log.Printf("  ✓ Found MediaDash service UUID in advertisement: %s", advertUUID.String())
						targetDevice = &result
						c.adapter.StopScan()
						return
					}
				}
			}
		*/

		// Second try: Check for known MediaDash device names
		if localName == "MediaDash" || localName == "MediaDash-Server" {
			log.Printf("  ✓ Found device with MediaDash name: %s", localName)
			targetDevice = &result
			c.adapter.StopScan()
			return
		}

		// Third try: Check for potential Android/companion app names (including unnamed devices)
		if localName == "Android" || localName == "NocturneCompanion" ||
			localName == "MediaDash-Companion" || localName == "" {
			log.Printf("  ? Found potential companion device: %s (will verify services)", localName)
			// Don't stop scanning yet - continue looking for better matches
			if targetDevice == nil {
				targetDevice = &result
			}
		}

		// Fourth try: For debugging - log rejection reason for LLIZARD devices
		if localName == "LLIZARD" {
			log.Printf("  ℹ Found LLIZARD device (different project) - Service UUID: %s vs expected %s",
				"402cbeaa-4901-48ce-8278-42b358460c1e", serviceUUID.String())
			log.Printf("  ℹ LLIZARD is from a different BLE project - skipping")
		}

		log.Printf("  - Device does not match MediaDash criteria")
	})

	if err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	// Wait for scan to complete or find device
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-scanCtx.Done():
			c.adapter.StopScan()
			log.Printf("Scan completed. Found %d total devices", len(discoveredDevices))

			if targetDevice == nil {
				// Try fallback: connect to any potential devices and verify services
				targetDevice = c.tryFallbackDeviceSelection(discoveredDevices, serviceUUID)
				if targetDevice == nil {
					return fmt.Errorf("no MediaDash device found within timeout (scanned %d devices)", len(discoveredDevices))
				}
			}
			break
		case <-ticker.C:
			if targetDevice != nil {
				break
			}
			continue
		}
		break
	}

	// Connect to device
	log.Printf("Selected device: %s (%s) RSSI: %d",
		targetDevice.Address.String(), targetDevice.LocalName(), targetDevice.RSSI)
	log.Printf("Establishing connection to MediaDash device...")

	// Use optimized connection parameters for Android devices
	connParams := bluetooth.ConnectionParams{
		ConnectionTimeout: bluetooth.NewDuration(20 * time.Second), // Longer timeout for Android
		// Note: TinyGo may not support all these parameters - they are best-effort
		// MinInterval and MaxInterval would be ideal but may not be available
	}

	device, err := c.adapter.Connect(targetDevice.Address, connParams)
	if err != nil {
		return fmt.Errorf("failed to connect to device %s: %w", targetDevice.Address.String(), err)
	}

	c.mutex.Lock()
	c.device = &device
	c.mutex.Unlock()

	log.Printf("Connected to device %s - discovering MediaDash service...", targetDevice.Address.String())

	// Discover services
	services, err := device.DiscoverServices([]bluetooth.UUID{serviceUUID})
	if err != nil {
		device.Disconnect()
		return fmt.Errorf("failed to discover services on device %s: %w", targetDevice.Address.String(), err)
	}

	if len(services) == 0 {
		device.Disconnect()
		return fmt.Errorf("MediaDash service (%s) not found on device %s - this device is not a MediaDash server",
			serviceUUID.String(), targetDevice.Address.String())
	}

	log.Printf("✓ Successfully discovered MediaDash service: %s", serviceUUID.String())
	service := services[0]
	c.service = &service

	// Setup characteristics
	if err := c.setupCharacteristics(); err != nil {
		device.Disconnect()
		return fmt.Errorf("failed to setup characteristics: %w", err)
	}

	// Update connection state
	c.mutex.Lock()
	c.connected = true
	c.reconnectAttempts = 0
	c.reconnectDelay = 1 * time.Second
	c.connectionStartTime = time.Now()
	c.lastHealthCheck = time.Now()
	c.consecutiveErrors = 0
	c.lastError = nil
	atomic.StoreInt32(&c.connectionHealthy, 1)
	c.mutex.Unlock()

	// Initialize activity tracking timestamps
	c.activityMutex.Lock()
	now := time.Now()
	c.lastSendActivity = now
	c.lastReceiveActivity = now
	c.activityMutex.Unlock()

	log.Printf("✓ Successfully connected to MediaDash device: %s (%s)",
		targetDevice.Address.String(), targetDevice.LocalName())
	log.Printf("✓ All MediaDash characteristics configured and notifications enabled")

	// Update Redis with connection status
	deviceName := targetDevice.LocalName()
	if deviceName == "" {
		deviceName = "MediaDash Device"
	}
	if err := c.redisStore.SetBLEConnectionStatus(true, deviceName); err != nil {
		log.Printf("Warning: Failed to update Redis connection status: %v", err)
	}

	// Start monitoring connection
	go c.monitorConnection(ctx)

	return nil
}

// tryFallbackDeviceSelection attempts to connect to potential devices to verify services
func (c *Client) tryFallbackDeviceSelection(devices []bluetooth.ScanResult, targetServiceUUID bluetooth.UUID) *bluetooth.ScanResult {
	log.Println("Trying fallback device selection - connecting to verify services...")

	// Sort devices by preference (stronger RSSI and known names first)
	potentialDevices := make([]bluetooth.ScanResult, 0)

	for _, device := range devices {
		localName := device.LocalName()

		// Skip known incompatible devices
		if localName == "LLIZARD" {
			log.Printf("  Skipping LLIZARD device (different project): %s", device.Address.String())
			continue
		}

		// Only try devices that might be relevant (skip obvious non-matches)
		if localName == "MediaDash" || localName == "MediaDash-Server" ||
			localName == "Android" || localName == "NocturneCompanion" ||
			localName == "MediaDash-Companion" || localName == "" {
			log.Printf("  Adding potential device: %s (%s) RSSI: %d",
				device.Address.String(), localName, device.RSSI)
			potentialDevices = append(potentialDevices, device)
		}
	}

	log.Printf("Found %d potential devices to verify", len(potentialDevices))

	// Try to connect to each potential device and verify services
	for i, device := range potentialDevices {
		if i >= 3 { // Limit to first 3 devices to avoid excessive connection attempts
			break
		}

		log.Printf("Verifying device %d/%d: %s (%s)", i+1, len(potentialDevices),
			device.Address.String(), device.LocalName())

		if c.verifyDeviceServices(device, targetServiceUUID) {
			log.Printf("  ✓ Device has MediaDash service - selecting for connection")
			return &device
		}

		log.Printf("  ✗ Device does not have MediaDash service")
	}

	log.Println("No suitable devices found in fallback verification")
	return nil
}

// verifyDeviceServices connects briefly to a device to check if it has the target service
func (c *Client) verifyDeviceServices(scanResult bluetooth.ScanResult, targetServiceUUID bluetooth.UUID) bool {
	// Create a temporary connection to verify services
	tempDevice, err := c.adapter.Connect(scanResult.Address, bluetooth.ConnectionParams{
		ConnectionTimeout: bluetooth.NewDuration(10 * time.Second),
	})
	if err != nil {
		log.Printf("    Failed to connect for verification: %v", err)
		return false
	}
	defer tempDevice.Disconnect()

	// Try to discover the specific service
	services, err := tempDevice.DiscoverServices([]bluetooth.UUID{targetServiceUUID})
	if err != nil {
		log.Printf("    Failed to discover services: %v", err)
		return false
	}

	hasService := len(services) > 0
	if hasService {
		log.Printf("    ✓ Found MediaDash service UUID: %s", targetServiceUUID.String())
	} else {
		log.Printf("    ✗ MediaDash service not found")
	}

	return hasService
}

// rateLimitedWrite performs a BLE write operation with rate limiting to prevent ATT error 0x0e
func (c *Client) rateLimitedWrite(char *bluetooth.DeviceCharacteristic, data []byte) (int, error) {
	if char == nil {
		return 0, fmt.Errorf("characteristic is nil")
	}

	// Wait for rate limit token
	<-c.rateLimiter

	c.writeMutex.Lock()
	minInterval := time.Duration(c.cfg.Ble.RateLimiting.WriteIntervalMs) * time.Millisecond
	timeSinceLastWrite := time.Since(c.lastWriteTime)

	if timeSinceLastWrite < minInterval {
		sleepTime := minInterval - timeSinceLastWrite
		log.Printf("Rate limiting BLE write: sleeping for %v", sleepTime)
		time.Sleep(sleepTime)
	}

	c.lastWriteTime = time.Now()
	c.writeMutex.Unlock()

	// Perform the write operation
	result, err := char.WriteWithoutResponse(data)

	// Update send activity tracking
	c.updateSendActivity()

	// Return the token for next operation
	go func() {
		time.Sleep(time.Duration(c.cfg.Ble.RateLimiting.WriteIntervalMs) * time.Millisecond)
		c.rateLimiter <- struct{}{}
	}()

	if err != nil {
		log.Printf("Rate-limited BLE write failed: %v", err)
		c.trackError("ble_write_rate_limited", err)

		// Check if this is a fatal connection error - trigger immediate reconnection
		if c.isFatalConnectionError(err) {
			log.Printf("FATAL BLE connection error detected during write - triggering reconnection")
			go c.handleDisconnection()
		}
	} else {
		log.Printf("Rate-limited BLE write successful (%d bytes)", len(data))
	}

	return result, err
}

// updateSendActivity updates the last send activity timestamp
func (c *Client) updateSendActivity() {
	c.activityMutex.Lock()
	c.lastSendActivity = time.Now()
	c.activityMutex.Unlock()
}

// updateReceiveActivity updates the last receive activity timestamp
func (c *Client) updateReceiveActivity() {
	c.activityMutex.Lock()
	c.lastReceiveActivity = time.Now()
	c.activityMutex.Unlock()
}

// getLastActivity returns the most recent activity timestamp (send or receive)
func (c *Client) getLastActivity() time.Time {
	c.activityMutex.RLock()
	defer c.activityMutex.RUnlock()

	if c.lastSendActivity.After(c.lastReceiveActivity) {
		return c.lastSendActivity
	}
	return c.lastReceiveActivity
}

// shouldPerformHealthCheck determines if a connection health check is needed
// based on recent activity and configuration
func (c *Client) shouldPerformHealthCheck() bool {
	activityTimeout := time.Duration(c.cfg.Ble.ConnectionMonitoring.ActivityTimeoutMinutes) * time.Minute
	timeSinceLastActivity := time.Since(c.getLastActivity())

	// Only perform health check if no activity for the configured timeout period
	return timeSinceLastActivity >= activityTimeout
}

// trackError tracks specific error patterns for analysis
func (c *Client) trackError(errorType string, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.lastError = err
	c.attErrorCount[errorType]++

	// Log error to persistent file
	LogError(errorType, err)

	// Check for specific ATT error 0x0e (Unlikely Error/Insufficient Resources)
	if err != nil && strings.Contains(err.Error(), "ATT error: 0x0e") {
		c.attErrorCount["att_0x0e"]++
		log.Printf("ATT Error 0x0e detected (count: %d) - Android server resource exhaustion likely", c.attErrorCount["att_0x0e"])

		// If we see multiple ATT 0x0e errors, suggest specific recovery
		if c.attErrorCount["att_0x0e"] >= 3 {
			log.Printf("Multiple ATT 0x0e errors detected - implementing recovery strategy")
			log.Printf("Recovery suggestions:")
			log.Printf("  1. Android companion app should restart BLE advertising")
			log.Printf("  2. Consider reducing notification frequency")
			log.Printf("  3. Check Android battery optimization settings")
		}
	}

	// Log error pattern analysis
	if c.attErrorCount[errorType] > 1 && c.attErrorCount[errorType]%5 == 0 {
		log.Printf("Recurring error pattern detected: %s (count: %d)", errorType, c.attErrorCount[errorType])
	}
}

// trackMediaStateNotification tracks a media state notification (received or dropped)
func (c *Client) trackMediaStateNotification(dropped bool) {
	if dropped {
		atomic.AddInt64(&c.notificationMetrics.mediaStateDropped, 1)
		c.notificationMetrics.mutex.Lock()
		c.notificationMetrics.mediaStateLastDrop = time.Now()
		c.notificationMetrics.mutex.Unlock()
		c.checkDropRateAlert("mediaState")
	}
	atomic.AddInt64(&c.notificationMetrics.mediaStateReceived, 1)
}

// trackAlbumArtNotification tracks an album art notification (received or dropped)
func (c *Client) trackAlbumArtNotification(dropped bool) {
	if dropped {
		atomic.AddInt64(&c.notificationMetrics.albumArtDropped, 1)
		c.notificationMetrics.mutex.Lock()
		c.notificationMetrics.albumArtLastDrop = time.Now()
		c.notificationMetrics.mutex.Unlock()
		c.checkDropRateAlert("albumArt")
	}
	atomic.AddInt64(&c.notificationMetrics.albumArtReceived, 1)
}

// checkDropRateAlert checks if drop rate exceeds threshold and logs an alert
func (c *Client) checkDropRateAlert(notificationType string) {
	c.notificationMetrics.mutex.Lock()
	defer c.notificationMetrics.mutex.Unlock()

	// Check cooldown to avoid alert spam
	if time.Since(c.notificationMetrics.lastAlertTime) < c.notificationMetrics.alertCooldown {
		return
	}

	var received, dropped int64
	var dropRate float64

	switch notificationType {
	case "mediaState":
		received = atomic.LoadInt64(&c.notificationMetrics.mediaStateReceived)
		dropped = atomic.LoadInt64(&c.notificationMetrics.mediaStateDropped)
	case "albumArt":
		received = atomic.LoadInt64(&c.notificationMetrics.albumArtReceived)
		dropped = atomic.LoadInt64(&c.notificationMetrics.albumArtDropped)
	}

	if received > 0 {
		dropRate = float64(dropped) / float64(received) * 100
	}

	if dropRate > c.notificationMetrics.dropAlertThreshold {
		log.Printf("ALERT: High %s notification drop rate: %.2f%% (%d/%d dropped)",
			notificationType, dropRate, dropped, received)
		log.Printf("  Consider checking: BLE connection quality, processing speed, buffer sizes")
		c.notificationMetrics.lastAlertTime = time.Now()
	}
}

// getNotificationMetrics returns current notification metrics for diagnostics
func (c *Client) getNotificationMetrics() map[string]interface{} {
	c.notificationMetrics.mutex.RLock()
	mediaStateLastDrop := c.notificationMetrics.mediaStateLastDrop
	albumArtLastDrop := c.notificationMetrics.albumArtLastDrop
	c.notificationMetrics.mutex.RUnlock()

	mediaStateReceived := atomic.LoadInt64(&c.notificationMetrics.mediaStateReceived)
	mediaStateDropped := atomic.LoadInt64(&c.notificationMetrics.mediaStateDropped)
	albumArtReceived := atomic.LoadInt64(&c.notificationMetrics.albumArtReceived)
	albumArtDropped := atomic.LoadInt64(&c.notificationMetrics.albumArtDropped)

	// Calculate drop rates
	var mediaStateDropRate, albumArtDropRate float64
	if mediaStateReceived > 0 {
		mediaStateDropRate = float64(mediaStateDropped) / float64(mediaStateReceived) * 100
	}
	if albumArtReceived > 0 {
		albumArtDropRate = float64(albumArtDropped) / float64(albumArtReceived) * 100
	}

	metrics := map[string]interface{}{
		"mediaState": map[string]interface{}{
			"received":     mediaStateReceived,
			"dropped":      mediaStateDropped,
			"dropRate":     fmt.Sprintf("%.2f%%", mediaStateDropRate),
			"lastDropTime": mediaStateLastDrop.Format(time.RFC3339),
			"healthy":      mediaStateDropRate < c.notificationMetrics.dropAlertThreshold,
		},
		"albumArt": map[string]interface{}{
			"received":     albumArtReceived,
			"dropped":      albumArtDropped,
			"dropRate":     fmt.Sprintf("%.2f%%", albumArtDropRate),
			"lastDropTime": albumArtLastDrop.Format(time.RFC3339),
			"healthy":      albumArtDropRate < c.notificationMetrics.dropAlertThreshold,
		},
		"alertThreshold": fmt.Sprintf("%.1f%%", c.notificationMetrics.dropAlertThreshold),
	}

	return metrics
}

// resetNotificationMetrics resets all notification counters (useful for diagnostics)
func (c *Client) resetNotificationMetrics() {
	atomic.StoreInt64(&c.notificationMetrics.mediaStateReceived, 0)
	atomic.StoreInt64(&c.notificationMetrics.mediaStateDropped, 0)
	atomic.StoreInt64(&c.notificationMetrics.albumArtReceived, 0)
	atomic.StoreInt64(&c.notificationMetrics.albumArtDropped, 0)

	c.notificationMetrics.mutex.Lock()
	c.notificationMetrics.mediaStateLastDrop = time.Time{}
	c.notificationMetrics.albumArtLastDrop = time.Time{}
	c.notificationMetrics.lastAlertTime = time.Time{}
	c.notificationMetrics.mutex.Unlock()

	log.Printf("Notification metrics reset")
}

// setupCharacteristics discovers and configures BLE characteristics
func (c *Client) setupCharacteristics() error {
	log.Println("Discovering MediaDash BLE characteristics...")

	// Characteristic UUIDs
	charUUIDs := map[string]string{
		"mediaState":      c.cfg.Ble.MediaStateCharacteristicUUID,
		"playbackControl": c.cfg.Ble.PlaybackControlCharacteristicUUID,
		"albumArtRequest": c.cfg.Ble.AlbumArtRequestCharacteristicUUID, // Used for error recovery
		"albumArtData":    c.cfg.Ble.AlbumArtDataCharacteristicUUID,
		"podcastInfo":     c.cfg.Ble.PodcastInfoCharacteristicUUID,
		"lyricsRequest":   c.cfg.Ble.LyricsRequestCharacteristicUUID,
		"lyricsData":      c.cfg.Ble.LyricsDataCharacteristicUUID,
		"settings":        c.cfg.Ble.SettingsCharacteristicUUID,
		"timeSync":        c.cfg.Ble.TimeSyncCharacteristicUUID,
	}

	// Parse UUIDs and discover characteristics
	uuids := make([]bluetooth.UUID, 0, len(charUUIDs))
	for name, uuidStr := range charUUIDs {
		uuid, err := bluetooth.ParseUUID(uuidStr)
		if err != nil {
			return fmt.Errorf("invalid characteristic UUID for %s (%s): %w", name, uuidStr, err)
		}
		uuids = append(uuids, uuid)
		log.Printf("Looking for characteristic: %s (%s)", name, uuidStr)
	}

	chars, err := c.service.DiscoverCharacteristics(uuids)
	if err != nil {
		return fmt.Errorf("failed to discover characteristics: %w", err)
	}

	log.Printf("Discovered %d characteristics from service", len(chars))

	// Map characteristics by UUID
	for _, char := range chars {
		for name, uuidStr := range charUUIDs {
			uuid, _ := bluetooth.ParseUUID(uuidStr)
			if char.UUID() == uuid {
				c.characteristics[name] = &char
				log.Printf("✓ Found characteristic: %s (%s)", name, uuidStr)
				break
			}
		}
	}

	// Verify required characteristics were found (albumArtRequest is optional)
	requiredChars := []string{"mediaState", "playbackControl", "albumArtData"}
	missing := make([]string, 0)

	for _, name := range requiredChars {
		if c.characteristics[name] == nil {
			uuidStr := charUUIDs[name]
			missing = append(missing, fmt.Sprintf("%s (%s)", name, uuidStr))
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("Required MediaDash characteristics not found: %v", missing)
	}

	// Log optional characteristics
	if c.characteristics["albumArtRequest"] == nil {
		log.Printf("Album art request characteristic not available - using proactive mode only")
	} else {
		log.Printf("✓ Album art request characteristic available for error recovery")
	}

	if c.characteristics["podcastInfo"] == nil {
		log.Printf("Podcast info characteristic not available - podcast support disabled")
	} else {
		log.Printf("✓ Podcast info characteristic available")
	}

	// Enable notifications for media state
	log.Println("Enabling notifications for media state characteristic...")
	if err := c.characteristics["mediaState"].EnableNotifications(c.handleMediaStateNotification); err != nil {
		return fmt.Errorf("failed to enable media state notifications: %w", err)
	}
	log.Println("✓ Media state notifications enabled")

	// Enable notifications for album art data
	log.Println("Enabling notifications for album art data characteristic...")
	if err := c.characteristics["albumArtData"].EnableNotifications(c.handleAlbumArtData); err != nil {
		return fmt.Errorf("failed to enable album art notifications: %w", err)
	}
	log.Println("✓ Album art data notifications enabled")

	// Enable notifications for podcast info (optional)
	if c.characteristics["podcastInfo"] != nil {
		log.Println("Enabling notifications for podcast info characteristic...")
		if err := c.characteristics["podcastInfo"].EnableNotifications(c.handlePodcastInfoNotification); err != nil {
			return fmt.Errorf("failed to enable podcast info notifications: %w", err)
		}
		log.Println("✓ Podcast info notifications enabled")
	}

	// Enable notifications for lyrics data (optional)
	if c.characteristics["lyricsData"] != nil {
		debug.LogLyrics("Lyrics data characteristic found (UUID: %s)", c.cfg.Ble.LyricsDataCharacteristicUUID)
		debug.LogLyrics("Enabling BLE notifications for lyrics data...")
		if err := c.characteristics["lyricsData"].EnableNotifications(c.handleLyricsDataNotification); err != nil {
			log.Printf("[LYRICS] ERROR: Failed to enable lyrics data notifications: %v", err)
			return fmt.Errorf("failed to enable lyrics data notifications: %w", err)
		}
		debug.LogLyrics("Successfully enabled lyrics data notifications")
	} else {
		debug.LogLyrics("WARNING: Lyrics data characteristic (UUID: %s) not available on server", c.cfg.Ble.LyricsDataCharacteristicUUID)
		debug.LogLyrics("Lyrics support is disabled - Android app may not support lyrics feature")
	}

	// Check if lyrics request characteristic is available
	if c.characteristics["lyricsRequest"] != nil {
		debug.LogLyrics("Lyrics request characteristic found (UUID: %s) - can request lyrics from Android", c.cfg.Ble.LyricsRequestCharacteristicUUID)
	} else {
		debug.LogLyrics("WARNING: Lyrics request characteristic (UUID: %s) not available - cannot request lyrics", c.cfg.Ble.LyricsRequestCharacteristicUUID)
	}

	// Enable notifications for settings (optional)
	if c.characteristics["settings"] != nil {
		log.Println("Enabling notifications for settings characteristic...")
		if err := c.characteristics["settings"].EnableNotifications(c.handleSettingsNotification); err != nil {
			return fmt.Errorf("failed to enable settings notifications: %w", err)
		}
		log.Println("✓ Settings notifications enabled")
	} else {
		log.Printf("Settings characteristic not available - settings sync disabled")
	}

	// Enable notifications for time sync (optional)
	if c.characteristics["timeSync"] != nil {
		log.Println("Enabling notifications for time sync characteristic...")
		if err := c.characteristics["timeSync"].EnableNotifications(c.handleTimeSyncNotification); err != nil {
			return fmt.Errorf("failed to enable time sync notifications: %w", err)
		}
		log.Println("✓ Time sync notifications enabled")
	} else {
		log.Printf("Time sync characteristic not available - time sync disabled")
	}

	return nil
}

// handleMediaStateNotification processes media state updates with flow control
func (c *Client) handleMediaStateNotification(buf []byte) {
	// Update receive activity tracking
	c.updateReceiveActivity()

	// Implement flow control to prevent notification overflow
	// Uses dedicated media state channel to prevent album art transfers from blocking playback updates
	select {
	case c.mediaStateFlowControl <- struct{}{}:
		defer func() { <-c.mediaStateFlowControl }()
		c.trackMediaStateNotification(false) // Track successful receipt
	default:
		// Drop notification if flow control buffer is full
		c.trackMediaStateNotification(true) // Track dropped notification
		log.Printf("Warning: Media state notification dropped due to flow control (buffer: %d/%d)",
			len(c.mediaStateFlowControl), cap(c.mediaStateFlowControl))
		return
	}

	// Log raw JSON data for debugging protocol issues
	rawJSON := string(buf)
	log.Printf("Raw BLE notification JSON: %s", rawJSON)

	var update MediaStateUpdate
	if err := json.Unmarshal(buf, &update); err != nil {
		log.Printf("Failed to parse media state update: %v", err)
		log.Printf("Raw JSON that failed to parse: %s", rawJSON)
		c.trackError("json_unmarshal", err)
		return
	}

	// Determine actual playing state from both fields
	actuallyPlaying := update.IsPlaying || (update.PlaybackState == "playing")
	
	log.Printf("Parsed MediaStateUpdate: Title='%s', Artist='%s', Album='%s', Duration=%d, Position=%d, Volume=%d, AlbumArtHash='%s', PlaybackState='%s', IsPlaying=%v, ActuallyPlaying=%v",
		update.TrackTitle, update.Artist, update.Album, update.Duration, update.Position, update.Volume, update.AlbumArtHash, update.PlaybackState, update.IsPlaying, actuallyPlaying)

	// Check if this is a meaningful state change
	hasChanged, changedFields := c.hasMediaStateChanged(&update)
	if hasChanged {
		c.logComprehensiveMediaState(&update, changedFields)

		// Update cached state
		c.updateCachedMediaState(&update)

		// Store in Redis with change detection - use the correctly determined playing state
		redisState := &redis.MediaState{
			IsPlaying:    actuallyPlaying,
			TrackTitle:   update.TrackTitle,
			Artist:       update.Artist,
			Album:        update.Album,
			Duration:     update.Duration,
			Position:     update.Position,
			Volume:       update.Volume,
			AlbumArtHash: update.AlbumArtHash,
			MediaChannel: update.MediaChannel,
		}

		if err := c.redisStore.StoreMediaState(redisState); err != nil {
			log.Printf("Failed to store media state: %v", err)
		}

		// Update time tracker with new position, duration, and playing state
		c.timeTrackerMutex.RLock()
		if c.timeTracker != nil {
			c.timeTracker.UpdatePosition(update.Position, actuallyPlaying)
			c.timeTracker.UpdateDuration(update.Duration)
		}
		c.timeTrackerMutex.RUnlock()
	} else {
		// For debugging excessive notifications - only log every 10th duplicate
		// This helps identify notification frequency without spam
		c.logDuplicateNotification(&update)
	}

	// Handle album art changes separately
	c.handleAlbumArtHashChange(update.AlbumArtHash, update.Artist, update.Album)

	// Handle lyrics requests on track/artist changes
	if hasChanged && (strings.Contains(changedFields, "track") || strings.Contains(changedFields, "artist") || changedFields == "initial") {
		c.handleTrackChange(update.Artist, update.TrackTitle)
	}
}

// handleAlbumArtData processes album art chunk notifications with flow control
func (c *Client) handleAlbumArtData(buf []byte) {
	// Update receive activity tracking
	c.updateReceiveActivity()

	// Implement flow control for album art notifications
	// Uses dedicated album art channel with large buffer for full transfer capacity
	select {
	case c.albumArtFlowControl <- struct{}{}:
		defer func() { <-c.albumArtFlowControl }()
		c.trackAlbumArtNotification(false) // Track successful receipt
	default:
		// Drop notification if flow control buffer is full - this indicates serious backpressure
		c.trackAlbumArtNotification(true) // Track dropped notification
		log.Printf("ERROR: Album art chunk dropped due to flow control (buffer: %d/%d) - transfer may fail",
			len(c.albumArtFlowControl), cap(c.albumArtFlowControl))
		return
	}

	// Use binary protocol (more efficient - no base64, no JSON)
	// Binary format: 16-byte header + raw image data
	if err := c.albumHandler.ProcessBinaryChunk(buf); err != nil {
		log.Printf("BLE_AA_BINARY: Failed to process binary chunk: %v", err)
		c.trackError("album_art_binary", err)
	}
}

// Podcast info chunk reassembly state
var (
	podcastInfoChunks      = make(map[int][]byte)
	podcastInfoTotalChunks = 0
	podcastInfoMutex       sync.Mutex
)

// handlePodcastInfoNotification processes podcast information updates from Android
// Data may arrive in chunks with 2-byte header: [chunkIndex, totalChunks, ...data...]
// Podcast response types (matches Android GattServerManager header byte)
const (
	PodcastResponseTypeLegacy        = 0 // Legacy 2-byte header format
	PodcastResponseTypeList          = 1 // PodcastListResponse (A-Z channel list)
	PodcastResponseTypeRecent        = 2 // RecentEpisodesResponse (recent episodes)
	PodcastResponseTypeEpisodes      = 3 // PodcastEpisodesResponse (paginated episodes)
	MediaChannelsResponseType        = 4 // MediaChannelsResponse (list of media channel apps)
)

// State for each response type's chunk reassembly
var (
	podcastListChunks          = make(map[int][]byte)
	podcastListTotalChunks     = 0
	podcastRecentChunks        = make(map[int][]byte)
	podcastRecentTotalChunks   = 0
	podcastEpisodesChunks      = make(map[int][]byte)
	podcastEpisodesTotalChunks = 0
	mediaChannelsChunks        = make(map[int][]byte)
	mediaChannelsTotalChunks   = 0
)

func (c *Client) handlePodcastInfoNotification(buf []byte) {
	// Update receive activity tracking
	c.updateReceiveActivity()

	if len(buf) < 3 {
		log.Printf("Podcast info data too short: %d bytes", len(buf))
		return
	}

	// Try to determine header format:
	// New format: [type][chunkIndex][totalChunks][data...] (3-byte header, type > 0)
	// Legacy format: [chunkIndex][totalChunks][data...] (2-byte header)
	responseType := int(buf[0])
	var chunkIndex, totalChunks int
	var chunkData []byte

	if responseType >= 1 && responseType <= 4 {
		// New 3-byte header format
		chunkIndex = int(buf[1])
		totalChunks = int(buf[2])
		chunkData = buf[3:]
		log.Printf("📦 Received podcast chunk: type=%d (%s), chunk %d/%d (%d bytes)",
			responseType, podcastResponseTypeName(responseType), chunkIndex+1, totalChunks, len(chunkData))
	} else {
		// Legacy 2-byte header format
		responseType = PodcastResponseTypeLegacy
		chunkIndex = int(buf[0])
		totalChunks = int(buf[1])
		chunkData = buf[2:]
		log.Printf("📦 Received legacy podcast chunk: %d/%d (%d bytes)", chunkIndex+1, totalChunks, len(chunkData))
	}

	// Handle single-chunk case
	if totalChunks == 1 {
		c.processPodcastResponse(responseType, chunkData)
		return
	}

	// Multi-chunk reassembly - use type-specific chunk storage
	podcastInfoMutex.Lock()
	defer podcastInfoMutex.Unlock()

	chunks, totalPtr := c.getPodcastChunkStorage(responseType)

	// Reset if we're starting a new transfer for this type
	if chunkIndex == 0 || totalChunks != *totalPtr {
		c.clearPodcastChunkStorage(responseType)
		*totalPtr = totalChunks
		chunks, _ = c.getPodcastChunkStorage(responseType) // Get fresh reference
	}

	// Store this chunk
	chunks[chunkIndex] = make([]byte, len(chunkData))
	copy(chunks[chunkIndex], chunkData)

	// Check if we have all chunks for this type
	if len(chunks) == totalChunks {
		// Reassemble
		var fullData []byte
		for i := 0; i < totalChunks; i++ {
			if chunk, ok := chunks[i]; ok {
				fullData = append(fullData, chunk...)
			} else {
				log.Printf("Missing podcast chunk %d for type %d, cannot reassemble", i, responseType)
				return
			}
		}

		log.Printf("✅ Reassembled podcast response type=%d (%s): %d bytes from %d chunks",
			responseType, podcastResponseTypeName(responseType), len(fullData), totalChunks)

		// Clear state for this type
		c.clearPodcastChunkStorage(responseType)

		// Process complete JSON
		c.processPodcastResponse(responseType, fullData)
	}
}

func podcastResponseTypeName(t int) string {
	switch t {
	case PodcastResponseTypeLegacy:
		return "legacy"
	case PodcastResponseTypeList:
		return "podcast_list"
	case PodcastResponseTypeRecent:
		return "recent_episodes"
	case PodcastResponseTypeEpisodes:
		return "podcast_episodes"
	case MediaChannelsResponseType:
		return "media_channels"
	default:
		return "unknown"
	}
}

func (c *Client) getPodcastChunkStorage(responseType int) (map[int][]byte, *int) {
	switch responseType {
	case PodcastResponseTypeList:
		return podcastListChunks, &podcastListTotalChunks
	case PodcastResponseTypeRecent:
		return podcastRecentChunks, &podcastRecentTotalChunks
	case PodcastResponseTypeEpisodes:
		return podcastEpisodesChunks, &podcastEpisodesTotalChunks
	case MediaChannelsResponseType:
		return mediaChannelsChunks, &mediaChannelsTotalChunks
	default:
		return podcastInfoChunks, &podcastInfoTotalChunks
	}
}

func (c *Client) clearPodcastChunkStorage(responseType int) {
	switch responseType {
	case PodcastResponseTypeList:
		podcastListChunks = make(map[int][]byte)
		podcastListTotalChunks = 0
	case PodcastResponseTypeRecent:
		podcastRecentChunks = make(map[int][]byte)
		podcastRecentTotalChunks = 0
	case PodcastResponseTypeEpisodes:
		podcastEpisodesChunks = make(map[int][]byte)
		podcastEpisodesTotalChunks = 0
	case MediaChannelsResponseType:
		mediaChannelsChunks = make(map[int][]byte)
		mediaChannelsTotalChunks = 0
	default:
		podcastInfoChunks = make(map[int][]byte)
		podcastInfoTotalChunks = 0
	}
}

// processPodcastResponse routes the reassembled JSON to the appropriate handler
func (c *Client) processPodcastResponse(responseType int, data []byte) {
	switch responseType {
	case PodcastResponseTypeList:
		c.processPodcastListJSON(data)
	case PodcastResponseTypeRecent:
		c.processRecentEpisodesJSON(data)
	case PodcastResponseTypeEpisodes:
		c.processPodcastEpisodesJSON(data)
	case MediaChannelsResponseType:
		c.processMediaChannelsBinary(data)
	default:
		// Legacy format - try multiple formats
		c.processPodcastInfoJSON(data)
	}
}

// processPodcastListJSON handles CompactPodcastListResponse (A-Z channel list)
func (c *Client) processPodcastListJSON(data []byte) {
	log.Printf("📋 Processing podcast list JSON (%d bytes)", len(data))

	var response redis.CompactPodcastListResponse
	if err := json.Unmarshal(data, &response); err != nil {
		log.Printf("❌ Failed to parse podcast list: %v", err)
		log.Printf("Raw JSON: %s", string(data))
		c.trackError("podcast_list_unmarshal", err)
		return
	}

	log.Printf("✅ Parsed podcast list: %d channels", len(response.Podcasts))
	for i, p := range response.Podcasts {
		if i < 3 {
			log.Printf("   - %s (%d episodes)", p.Name, p.Count)
		}
	}
	if len(response.Podcasts) > 3 {
		log.Printf("   ... and %d more", len(response.Podcasts)-3)
	}

	// Store in Redis
	if err := c.redisStore.StorePodcastList(&response); err != nil {
		log.Printf("❌ Failed to store podcast list: %v", err)
		c.trackError("podcast_list_store", err)
		return
	}

	log.Printf("✅ Stored podcast list in Redis: %d channels", len(response.Podcasts))
}

// processRecentEpisodesJSON handles CompactRecentEpisodesResponse
func (c *Client) processRecentEpisodesJSON(data []byte) {
	log.Printf("🕐 Processing recent episodes JSON (%d bytes)", len(data))

	var response redis.CompactRecentEpisodesResponse
	if err := json.Unmarshal(data, &response); err != nil {
		log.Printf("❌ Failed to parse recent episodes: %v", err)
		log.Printf("Raw JSON: %s", string(data))
		c.trackError("recent_episodes_unmarshal", err)
		return
	}

	log.Printf("✅ Parsed recent episodes: %d episodes (total: %d)", len(response.Episodes), response.Total)
	for i, ep := range response.Episodes {
		if i < 3 {
			log.Printf("   - %s: %s", ep.Channel, ep.Title)
		}
	}
	if len(response.Episodes) > 3 {
		log.Printf("   ... and %d more", len(response.Episodes)-3)
	}

	// Store in Redis
	if err := c.redisStore.StoreRecentEpisodes(&response); err != nil {
		log.Printf("❌ Failed to store recent episodes: %v", err)
		c.trackError("recent_episodes_store", err)
		return
	}

	log.Printf("✅ Stored recent episodes in Redis: %d episodes", len(response.Episodes))
}

// processPodcastEpisodesJSON handles CompactPodcastEpisodesResponse (paginated episodes for one podcast)
func (c *Client) processPodcastEpisodesJSON(data []byte) {
	log.Printf("📑 Processing podcast episodes JSON (%d bytes)", len(data))

	var response redis.CompactPodcastEpisodesResponse
	if err := json.Unmarshal(data, &response); err != nil {
		log.Printf("❌ Failed to parse podcast episodes: %v", err)
		log.Printf("Raw JSON: %s", string(data))
		c.trackError("podcast_episodes_unmarshal", err)
		return
	}

	log.Printf("✅ Parsed podcast episodes for '%s': %d episodes (offset=%d, total=%d, hasMore=%v)",
		response.Name, len(response.Episodes), response.Offset, response.Total, response.More)
	for i, ep := range response.Episodes {
		if i < 3 {
			log.Printf("   - %s", ep.Title)
		}
	}
	if len(response.Episodes) > 3 {
		log.Printf("   ... and %d more", len(response.Episodes)-3)
	}

	// Store in Redis
	if err := c.redisStore.StorePodcastEpisodes(&response); err != nil {
		log.Printf("❌ Failed to store podcast episodes: %v", err)
		c.trackError("podcast_episodes_store", err)
		return
	}

	log.Printf("✅ Stored podcast episodes in Redis for '%s'", response.Name)
}

// processMediaChannelsBinary parses binary media channels list from Android
// Binary format:
//   - 2 bytes: uint16 big-endian channel count
//   - For each channel:
//     - 1 byte: name length
//     - N bytes: UTF-8 name string
func (c *Client) processMediaChannelsBinary(data []byte) {
	log.Printf("📺 Processing media channels binary (%d bytes)", len(data))

	if len(data) < 2 {
		log.Printf("❌ Media channels data too short: %d bytes", len(data))
		return
	}

	// Read channel count (big-endian uint16)
	channelCount := int(data[0])<<8 | int(data[1])
	log.Printf("   Channel count: %d", channelCount)

	channels := make([]string, 0, channelCount)
	offset := 2

	for i := 0; i < channelCount && offset < len(data); i++ {
		// Read name length
		nameLen := int(data[offset])
		offset++

		if offset+nameLen > len(data) {
			log.Printf("❌ Media channel %d name truncated (need %d bytes, have %d)", i, nameLen, len(data)-offset)
			break
		}

		// Read name
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		channels = append(channels, name)
	}

	// Print the channel list
	log.Printf("═══════════════════════════════════════════════════════")
	log.Printf("📺 MEDIA CHANNELS: Received %d channels from Android", len(channels))
	log.Printf("═══════════════════════════════════════════════════════")
	for i, ch := range channels {
		log.Printf("   %2d. %s", i+1, ch)
	}
	log.Printf("═══════════════════════════════════════════════════════")

	// Store in Redis
	if err := c.redisStore.StoreMediaChannels(channels); err != nil {
		log.Printf("❌ Failed to store media channels: %v", err)
		c.trackError("media_channels_store", err)
		return
	}

	log.Printf("✅ Stored %d media channels in Redis", len(channels))
}

// processPodcastInfoJSON parses and stores podcast info JSON (legacy format)
func (c *Client) processPodcastInfoJSON(data []byte) {
	rawJSON := string(data)
	log.Printf("Processing podcast info JSON (%d bytes)", len(data))

	// Try parsing as new multi-podcast format first
	var libraryInfo redis.PodcastInfoResponse
	if err := json.Unmarshal(data, &libraryInfo); err == nil && len(libraryInfo.Podcasts) > 0 {
		log.Printf("Parsed PodcastLibrary: %d podcasts", len(libraryInfo.Podcasts))
		for _, p := range libraryInfo.Podcasts {
			log.Printf("  - %s by %s (%d episodes)", p.Title, p.Author, len(p.Episodes))
		}

		// Store podcast library in Redis
		if err := c.redisStore.StorePodcastLibrary(&libraryInfo); err != nil {
			log.Printf("Failed to store podcast library: %v", err)
			c.trackError("podcast_library_store", err)
			return
		}

		log.Printf("Successfully stored podcast library: %d podcasts", len(libraryInfo.Podcasts))
		return
	}

	// Fall back to legacy single-podcast format
	var info redis.PodcastInfo
	if err := json.Unmarshal(data, &info); err != nil {
		log.Printf("Failed to parse podcast info update: %v", err)
		log.Printf("Raw JSON that failed to parse: %s", rawJSON)
		c.trackError("podcast_info_unmarshal", err)
		return
	}

	log.Printf("Parsed legacy PodcastInfo: Show='%s', Episode='%s', Author='%s', EpisodeCount=%d, CurrentIndex=%d, Episodes=%d",
		info.ShowName, info.EpisodeTitle, info.Author, info.EpisodeCount, info.CurrentIndex, len(info.Episodes))

	// Store podcast info in Redis
	if err := c.redisStore.StorePodcastInfo(&info); err != nil {
		log.Printf("Failed to store podcast info: %v", err)
		c.trackError("podcast_info_store", err)
		return
	}

	log.Printf("Successfully stored podcast info: %s - %s (%d episodes)", info.ShowName, info.EpisodeTitle, len(info.Episodes))
}

// lyricsPacketBuffer holds BLE packets for a lyrics chunk being reassembled
type lyricsPacketBuffer struct {
	packets    map[int][]byte // packetIndex -> data
	totalCount int
	chunkIndex int
}

// lyricsReassemblyBuffer tracks incomplete lyrics chunks
var lyricsReassemblyBuffer = make(map[int]*lyricsPacketBuffer) // chunkIndex -> buffer
var lyricsBufferMutex sync.Mutex                               // protects lyricsReassemblyBuffer

// handleLyricsDataNotification processes lyrics data from Android
// Packets have a 3-byte header: [lyricsChunkIndex, blePacketIndex, totalBlePackets]
func (c *Client) handleLyricsDataNotification(buf []byte) {
	// Update receive activity tracking
	c.updateReceiveActivity()

	debug.LogLyrics("BLE notification received: %d bytes", len(buf))

	if len(buf) < 4 {
		debug.LogLyrics("ERROR: Data too short: %d bytes (minimum 4 required)", len(buf))
		return
	}

	// Extract header
	lyricsChunkIndex := int(buf[0])
	blePacketIndex := int(buf[1])
	totalBlePackets := int(buf[2])
	jsonFragment := buf[3:]

	debug.LogLyrics("Packet header: chunkIdx=%d, packetIdx=%d/%d, payload=%d bytes",
		lyricsChunkIndex, blePacketIndex+1, totalBlePackets, len(jsonFragment))

	// Lock mutex for all map operations
	lyricsBufferMutex.Lock()

	// Initialize buffer for this chunk if needed
	if lyricsReassemblyBuffer[lyricsChunkIndex] == nil {
		lyricsReassemblyBuffer[lyricsChunkIndex] = &lyricsPacketBuffer{
			packets:    make(map[int][]byte),
			totalCount: totalBlePackets,
			chunkIndex: lyricsChunkIndex,
		}
	}

	buffer := lyricsReassemblyBuffer[lyricsChunkIndex]
	buffer.packets[blePacketIndex] = jsonFragment

	debug.LogLyrics("Buffered packet %d/%d for chunk %d (have %d packets)",
		blePacketIndex+1, totalBlePackets, lyricsChunkIndex, len(buffer.packets))

	// Check if all packets received for this chunk
	if len(buffer.packets) < totalBlePackets {
		lyricsBufferMutex.Unlock()
		return // Still waiting for more packets
	}

	// Reassemble the full JSON
	var fullJson []byte
	for i := 0; i < totalBlePackets; i++ {
		if packet, ok := buffer.packets[i]; ok {
			fullJson = append(fullJson, packet...)
		} else {
			log.Printf("[LYRICS] ERROR: Missing packet %d for chunk %d", i, lyricsChunkIndex)
			delete(lyricsReassemblyBuffer, lyricsChunkIndex)
			lyricsBufferMutex.Unlock()
			return
		}
	}

	// Clear buffer for this chunk
	delete(lyricsReassemblyBuffer, lyricsChunkIndex)
	lyricsBufferMutex.Unlock()

	debug.LogLyrics("Reassembled full JSON for chunk %d: %d bytes", lyricsChunkIndex, len(fullJson))
	debug.LogLyricsVerbose("Full JSON: %s", string(fullJson))

	// Parse JSON lyrics chunk
	var chunk redis.CompactLyricsChunk
	if err := json.Unmarshal(fullJson, &chunk); err != nil {
		log.Printf("[LYRICS] ERROR: Failed to parse lyrics chunk JSON: %v", err)
		debug.LogLyricsVerbose("Raw JSON that failed: %s", string(fullJson))
		c.trackError("lyrics_unmarshal", err)
		return
	}

	debug.LogLyrics("Parsed chunk successfully:")
	debug.LogLyrics("  Hash: %s", chunk.Hash)
	debug.LogLyrics("  Synced: %v", chunk.Synced)
	debug.LogLyrics("  TotalLines: %d", chunk.TotalLines)
	debug.LogLyrics("  ChunkIndex: %d/%d", chunk.ChunkIndex+1, chunk.MaxChunks)
	debug.LogLyrics("  Lines in this chunk: %d", len(chunk.Lines))

	// Check for clear signal (empty lyrics)
	if chunk.TotalLines == 0 && chunk.MaxChunks == 1 {
		debug.LogLyrics("Clear signal received - no lyrics available for current track")
		if err := c.redisStore.ClearLyrics(); err != nil {
			log.Printf("[LYRICS] ERROR: Failed to clear lyrics from Redis: %v", err)
			c.trackError("lyrics_clear", err)
		} else {
			debug.LogLyrics("Successfully cleared lyrics from Redis")
		}
		return
	}

	// Store the lyrics chunk
	debug.LogLyrics("Storing chunk %d/%d in Redis...", chunk.ChunkIndex+1, chunk.MaxChunks)
	if err := c.redisStore.StoreLyricsChunk(&chunk); err != nil {
		log.Printf("[LYRICS] ERROR: Failed to store lyrics chunk: %v", err)
		c.trackError("lyrics_store", err)
		return
	}

	debug.LogLyrics("Successfully stored chunk %d/%d for hash %s", chunk.ChunkIndex+1, chunk.MaxChunks, chunk.Hash)

	if chunk.ChunkIndex == chunk.MaxChunks-1 {
		debug.LogLyrics("All chunks received - lyrics complete for hash %s (total lines: %d, synced: %v)",
			chunk.Hash, chunk.TotalLines, chunk.Synced)
	}
}

// handleSettingsNotification processes settings updates from Android
func (c *Client) handleSettingsNotification(buf []byte) {
	// Update receive activity tracking
	c.updateReceiveActivity()

	if len(buf) < 5 {
		log.Printf("Settings data too short: %d bytes", len(buf))
		return
	}

	// Parse JSON settings
	var settings map[string]interface{}
	if err := json.Unmarshal(buf, &settings); err != nil {
		log.Printf("Failed to parse settings: %v", err)
		c.trackError("settings_unmarshal", err)
		return
	}

	log.Printf("⚙️ Received settings update: %v", settings)

	// Handle lyricsEnabled setting
	if lyricsEnabled, ok := settings["lyricsEnabled"]; ok {
		debug.LogLyrics("Settings notification contains lyricsEnabled: %v (type: %T)", lyricsEnabled, lyricsEnabled)
		enabled := false
		switch v := lyricsEnabled.(type) {
		case bool:
			enabled = v
			debug.LogLyrics("Parsed lyricsEnabled as bool: %v", enabled)
		case string:
			enabled = v == "true" || v == "1"
			debug.LogLyrics("Parsed lyricsEnabled from string '%s': %v", v, enabled)
		default:
			debug.LogLyrics("WARNING: Unexpected type for lyricsEnabled: %T", lyricsEnabled)
		}

		debug.LogLyrics("Updating lyrics enabled state in Redis to: %v", enabled)
		if err := c.redisStore.SetLyricsEnabled(enabled); err != nil {
			log.Printf("[LYRICS] ERROR: Failed to store lyrics enabled setting: %v", err)
			c.trackError("settings_lyrics_enabled", err)
		} else {
			debug.LogLyrics("Successfully updated lyrics enabled to: %v", enabled)
		}
	}
}

// handleTimeSyncNotification processes time sync updates from Android
// Sets the system time based on Unix timestamp received from the phone
func (c *Client) handleTimeSyncNotification(buf []byte) {
	// Update receive activity tracking
	c.updateReceiveActivity()

	if len(buf) < 5 {
		log.Printf("[TIME_SYNC] Data too short: %d bytes", len(buf))
		return
	}

	// Parse Unix timestamp (decimal string)
	timestampStr := strings.TrimSpace(string(buf))
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		log.Printf("[TIME_SYNC] Failed to parse timestamp '%s': %v", timestampStr, err)
		return
	}

	// Convert to time.Time for logging
	syncTime := time.Unix(timestamp, 0)
	log.Printf("[TIME_SYNC] Received time from Android: %s (Unix: %d)", syncTime.Format(time.RFC3339), timestamp)

	// Set system time using the date command
	// Format: date -s "@<timestamp>" sets time from Unix timestamp
	cmd := fmt.Sprintf("date -s \"@%d\"", timestamp)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		log.Printf("[TIME_SYNC] Failed to set system time: %v (output: %s)", err, string(output))
		c.trackError("time_sync_set", err)
		return
	}

	log.Printf("[TIME_SYNC] ✓ System time synchronized to: %s", syncTime.Format("2006-01-02 15:04:05 MST"))
}

// requestLyrics requests lyrics from the Android server
func (c *Client) requestLyrics(artist, track string) error {
	debug.LogLyrics("Request initiated for artist='%s', track='%s'", artist, track)

	c.mutex.RLock()
	char := c.characteristics["lyricsRequest"]
	connected := c.connected
	c.mutex.RUnlock()

	if !connected {
		debug.LogLyrics("ERROR: Cannot request lyrics - not connected to BLE server")
		return fmt.Errorf("not connected to BLE server")
	}

	if char == nil {
		debug.LogLyrics("ERROR: Lyrics request characteristic (UUID: 0000a0d6-...) not available on server")
		debug.LogLyrics("ERROR: This means the Android app does not support lyrics or the characteristic was not discovered")
		return fmt.Errorf("lyrics not supported")
	}

	// Create request command
	requestCmd := map[string]string{
		"action": "get",
		"artist": artist,
		"track":  track,
	}

	data, err := json.Marshal(requestCmd)
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to marshal lyrics request JSON: %v", err)
		return fmt.Errorf("failed to marshal lyrics request: %w", err)
	}

	debug.LogLyrics("Sending BLE request: %s", string(data))
	debug.LogLyrics("Request payload size: %d bytes", len(data))

	// Send request with rate limiting
	if _, err := c.rateLimitedWrite(char, data); err != nil {
		log.Printf("[LYRICS] ERROR: BLE write failed: %v", err)
		c.trackError("lyrics_request", err)
		return fmt.Errorf("failed to send lyrics request: %w", err)
	}

	debug.LogLyrics("Request sent successfully - waiting for lyrics data notification")
	return nil
}

// handleTrackChange checks if track/artist changed and requests lyrics if enabled
func (c *Client) handleTrackChange(artist, track string) {
	debug.LogLyrics("handleTrackChange called with artist='%s', track='%s'", artist, track)

	// Skip if empty metadata
	if artist == "" || track == "" {
		debug.LogLyrics("Skipping - empty metadata (artist='%s', track='%s')", artist, track)
		return
	}

	// Check if this is actually a new track
	c.lyricsMutex.RLock()
	lastArtist := c.lastLyricsArtist
	lastTrack := c.lastLyricsTrack
	sameTrack := lastArtist == artist && lastTrack == track
	c.lyricsMutex.RUnlock()

	if sameTrack {
		debug.LogLyrics("Same track as last request - no new lyrics request needed")
		return
	}

	debug.LogLyrics("Track changed: '%s - %s' -> '%s - %s'", lastArtist, lastTrack, artist, track)

	// Update last requested track
	c.lyricsMutex.Lock()
	c.lastLyricsArtist = artist
	c.lastLyricsTrack = track
	c.lyricsMutex.Unlock()

	// Check if lyrics are enabled in SDK config (via Redis)
	debug.LogLyrics("Checking if lyrics feature is enabled in Redis...")
	enabled, err := c.redisStore.GetLyricsEnabled()
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to check lyrics enabled setting: %v", err)
		return
	}

	debug.LogLyrics("Lyrics feature enabled: %v", enabled)

	if !enabled {
		debug.LogLyrics("Feature disabled in settings - skipping lyrics request for: %s - %s", artist, track)
		return
	}

	// Request lyrics from Android
	debug.LogLyrics("Initiating lyrics request for new track: %s - %s", artist, track)
	if err := c.requestLyrics(artist, track); err != nil {
		log.Printf("[LYRICS] ERROR: Failed to request lyrics: %v", err)
		c.trackError("lyrics_request_track_change", err)
	} else {
		debug.LogLyrics("Lyrics request queued successfully for: %s - %s", artist, track)
	}
}

// requestAlbumArt requests album art from the Android server by hash (full request)
// This is the primary mechanism for obtaining album art when not cached locally
func (c *Client) requestAlbumArt(hash string) error {
	return c.requestAlbumArtWithChunks(hash, nil)
}

// requestAlbumArtWithChunks requests album art with optional selective chunk retry
// If chunks is nil or empty, requests all chunks (full request)
// If chunks contains indices, requests only those specific chunks (selective retry)
func (c *Client) requestAlbumArtWithChunks(hash string, chunks []uint16) error {
	c.mutex.RLock()
	char := c.characteristics["albumArtRequest"]
	connected := c.connected
	c.mutex.RUnlock()

	if !connected {
		return fmt.Errorf("not connected to BLE server")
	}

	if char == nil {
		log.Printf("Album art request characteristic not available - server should send proactively")
		return nil // Not an error - Android sends proactively
	}

	// Create request command
	var requestCmd interface{}
	if len(chunks) == 0 {
		// Full request - just hash
		log.Printf("BLE_AA_ADVANCED: Requesting full album art for hash: %s", hash)
		requestCmd = map[string]string{
			"hash": hash,
		}
	} else {
		// Selective retry - hash + specific chunks
		log.Printf("BLE_AA_ADVANCED: Requesting %d specific chunks for hash: %s", len(chunks), hash)

		// Convert uint16 to int for JSON marshaling
		chunkInts := make([]int, len(chunks))
		for i, c := range chunks {
			chunkInts[i] = int(c)
		}

		requestCmd = map[string]interface{}{
			"hash":   hash,
			"chunks": chunkInts,
		}
	}

	data, err := json.Marshal(requestCmd)
	if err != nil {
		return fmt.Errorf("failed to marshal album art request: %w", err)
	}

	// Log the request payload for debugging
	if len(chunks) > 0 {
		log.Printf("BLE_AA_ADVANCED: Selective retry request payload: %s", string(data))
	}

	// Send request with rate limiting
	if _, err := c.rateLimitedWrite(char, data); err != nil {
		log.Printf("BLE_AA_ADVANCED: Failed to send album art request for hash %s: %v", hash, err)
		c.trackError("album_art_request", err)
		return fmt.Errorf("failed to send album art request: %w", err)
	}

	if len(chunks) == 0 {
		log.Printf("BLE_AA_ADVANCED: Full album art request sent successfully for hash: %s", hash)
	} else {
		log.Printf("BLE_AA_ADVANCED: Selective chunk request sent successfully for hash: %s (%d chunks)", hash, len(chunks))
	}
	return nil
}

// processCommands processes playback commands from Redis queue with enhanced monitoring
func (c *Client) processCommands(ctx context.Context) {
	// Use optimized polling for maximum responsiveness
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	// Command queue health monitoring
	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()

	log.Println("Enhanced command processor started - monitoring Redis queue: system:playback_cmd_q")

	for {
		select {
		case <-ctx.Done():
			log.Println("Command processor shutting down due to context cancellation")
			return
		case <-c.stopChan:
			log.Println("Command processor shutting down due to stop signal")
			return
		case <-ticker.C:
			if err := c.processNextCommand(); err != nil {
				// Only log actual processing errors, not "no commands" conditions
				if err.Error() != "no commands queued" {
					log.Printf("Failed to process command: %v", err)
					c.trackError("command_processing", err)
				}
			}
		case <-healthTicker.C:
			c.reportCommandProcessingHealth()
		}
	}
}

// processNextCommand dequeues and processes the next playback command with comprehensive error handling
func (c *Client) processNextCommand() error {
	// Dequeue command from Redis
	cmd, err := c.redisStore.DequeuePlaybackCommand()
	if err != nil {
		return fmt.Errorf("failed to dequeue command from Redis: %w", err)
	}

	if cmd == nil {
		return fmt.Errorf("no commands queued") // This is expected when queue is empty
	}

	// Validate command before processing
	if err := c.validatePlaybackCommand(cmd); err != nil {
		log.Printf("Invalid command rejected: %v", err)
		c.updateCommandStats(false)
		return fmt.Errorf("command validation failed: %w", err)
	}

	// Handle request_lyrics specially - uses lyrics characteristic, not playbackControl
	if cmd.Action == "request_lyrics" {
		debug.LogLyrics("Processing request_lyrics command: artist=%s, track=%s", cmd.Artist, cmd.Track)
		c.requestLyrics(cmd.Artist, cmd.Track)
		c.updateCommandStats(true)
		log.Printf("✅ LYRICS: Request sent for: %s - %s", cmd.Artist, cmd.Track)
		return nil
	}

	// Check BLE connection status
	c.mutex.RLock()
	char := c.characteristics["playbackControl"]
	connected := c.connected
	c.mutex.RUnlock()

	if !connected || char == nil {
		// Queue for retry instead of re-queuing immediately
		c.queueForRetry(cmd, "BLE not connected or characteristic unavailable")
		return fmt.Errorf("BLE not available - queued for retry")
	}

	// Convert and validate BLE command
	bleCmd, err := c.convertToBLECommand(cmd)
	if err != nil {
		log.Printf("Command conversion failed: %v", err)
		c.updateCommandStats(false)
		return fmt.Errorf("command conversion failed: %w", err)
	}

	// Marshal command to JSON
	data, err := json.Marshal(bleCmd)
	if err != nil {
		log.Printf("Command marshaling failed: %v", err)
		c.updateCommandStats(false)
		return fmt.Errorf("failed to marshal BLE command: %w", err)
	}

	// Log command details before sending
	cmdAge := time.Since(time.Unix(cmd.Timestamp, 0))
	if cmdAge > 365*24*time.Hour {
		log.Printf("Processing command: %s (value: %d, age: %v - system time jump detected)",
			cmd.Action, cmd.Value, cmdAge)
	} else {
		log.Printf("Processing command: %s (value: %d, age: %v)",
			cmd.Action, cmd.Value, cmdAge)
	}

	// Send command with rate limiting and error handling
	if _, err := c.rateLimitedWrite(char, data); err != nil {
		// Track the error for pattern analysis
		c.trackError("ble_write", err)

		// Determine if we should retry based on error type
		if c.shouldRetryCommand(err) {
			c.queueForRetry(cmd, fmt.Sprintf("BLE write failed: %v", err))
			log.Printf("Command queued for retry due to: %v", err)
		} else {
			log.Printf("Command permanently failed (non-retryable): %v", err)
			c.updateCommandStats(false)
		}

		return fmt.Errorf("failed to send BLE command: %w", err)
	}

	// Command sent successfully
	// Enhanced logging for podcast commands
	switch cmd.Action {
	case "play_episode":
		log.Printf("✅ PODCAST: Episode play command sent successfully")
		log.Printf("   → episodeHash: %s", cmd.EpisodeHash)
		log.Printf("   → Waiting for Android to start playback...")
	case "play_podcast_episode":
		log.Printf("✅ PODCAST: Episode play command sent successfully (DEPRECATED)")
		log.Printf("   → Waiting for Android to start playback...")
	case "request_podcast_list":
		log.Printf("✅ PODCAST: Channel list request sent successfully")
		log.Printf("   → Waiting for Android response via BLE notification...")
	case "request_recent_episodes":
		log.Printf("✅ PODCAST: Recent episodes request sent successfully")
		log.Printf("   → Waiting for Android response via BLE notification...")
	case "request_podcast_episodes":
		log.Printf("✅ PODCAST: Podcast episodes request sent successfully")
		log.Printf("   → Waiting for Android response via BLE notification...")
	default:
		log.Printf("✓ Successfully sent command: %s", cmd.Action)
	}
	c.updateCommandStats(true)
	return nil
}

// validatePlaybackCommand validates the incoming playback command structure
func (c *Client) validatePlaybackCommand(cmd *redis.PlaybackCommand) error {
	if cmd == nil {
		return fmt.Errorf("command is nil")
	}

	if cmd.Action == "" {
		return fmt.Errorf("command action is empty")
	}

	// Validate known command types
	validActions := map[string]bool{
		"play":                     true,
		"pause":                    true,
		"next":                     true,
		"previous":                 true,
		"seek":                     true,
		"volume":                   true,
		"stop":                     true,
		"toggle":                   true, // toggle play/pause
		"request_podcast_info":     true, // legacy: get all podcasts with episodes
		"play_episode":             true, // play episode by hash (CRC32 of feedUrl+pubDate+duration)
		"play_podcast_episode":     true, // DEPRECATED: play by index, use play_episode instead
		"request_podcast_list":     true, // get podcast channel names (A-Z list)
		"request_recent_episodes":  true, // get recent episodes across all podcasts
		"request_podcast_episodes": true, // get episodes for specific podcast (paginated)
		"request_lyrics":           true, // request lyrics for artist/track
		"request_media_channels":   true, // get list of media channel apps (Spotify, YouTube, etc.)
		"select_media_channel":     true, // select which media channel app to control
	}

	if !validActions[cmd.Action] {
		return fmt.Errorf("unknown command action: %s", cmd.Action)
	}

	// Validate value ranges for specific commands
	switch cmd.Action {
	case "volume":
		if cmd.Value < 0 || cmd.Value > 100 {
			return fmt.Errorf("volume value out of range (0-100): %d", cmd.Value)
		}
	case "seek":
		if cmd.Value < 0 {
			return fmt.Errorf("seek value cannot be negative: %d", cmd.Value)
		}
	case "play_episode":
		if cmd.EpisodeHash == "" {
			return fmt.Errorf("play_episode requires episodeHash")
		}
	case "play_podcast_episode":
		// DEPRECATED: use play_episode instead
		if cmd.PodcastId == "" {
			return fmt.Errorf("play_podcast_episode requires podcastId")
		}
		if cmd.EpisodeIndex < 0 {
			return fmt.Errorf("play_podcast_episode requires non-negative episodeIndex: %d", cmd.EpisodeIndex)
		}
	case "request_podcast_episodes":
		if cmd.PodcastId == "" {
			return fmt.Errorf("request_podcast_episodes requires podcastId")
		}
		if cmd.Offset < 0 {
			return fmt.Errorf("request_podcast_episodes offset cannot be negative: %d", cmd.Offset)
		}
		if cmd.Limit < 0 {
			return fmt.Errorf("request_podcast_episodes limit cannot be negative: %d", cmd.Limit)
		}
	case "request_lyrics":
		if cmd.Artist == "" || cmd.Track == "" {
			return fmt.Errorf("request_lyrics requires artist and track")
		}
	}

	// Note: Timestamp is available for logging/debugging but not used for validation
	// Commands are processed immediately without age checks to avoid issues with
	// system time synchronization problems or clock drift
	return nil
}

// convertToBLECommand converts Redis command to BLE command format with enhanced mapping
func (c *Client) convertToBLECommand(cmd *redis.PlaybackCommand) (*PlaybackCommand, error) {
	if cmd == nil {
		return nil, fmt.Errorf("cannot convert nil command")
	}

	bleCmd := &PlaybackCommand{
		Action:       cmd.Action,
		Value:        cmd.Value,
		PodcastId:    cmd.PodcastId,
		EpisodeHash:  cmd.EpisodeHash,
		EpisodeIndex: cmd.EpisodeIndex,
		Offset:       cmd.Offset,
		Limit:        cmd.Limit,
		Channel:      cmd.Channel,
	}

	// Map certain Redis commands to BLE equivalents
	switch cmd.Action {
	case "toggle":
		// Convert toggle to play/pause based on current state if available
		// For now, just pass through as-is and let Android handle it
		bleCmd.Action = "toggle"
	case "seek":
		// Ensure seek position is valid
		if cmd.Value < 0 {
			return nil, fmt.Errorf("seek position cannot be negative")
		}
		// Convert seconds (from UI/Redis) to milliseconds (Android MediaController expects ms)
		bleCmd.Value = cmd.Value * 1000
	case "volume":
		// Ensure volume is within bounds
		if cmd.Value < 0 {
			bleCmd.Value = 0
		} else if cmd.Value > 100 {
			bleCmd.Value = 100
		}
	case "play_episode":
		// New hash-based episode playback
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("🎧 PODCAST: Play episode command (hash-based)")
		log.Printf("   → episodeHash: %s", cmd.EpisodeHash)
		log.Printf("   → Target: Android MediaDash via BLE")
	case "play_podcast_episode":
		// DEPRECATED: Pass through podcast fields as-is, Android handles the lookup
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("🎧 PODCAST: Play episode command (DEPRECATED - use play_episode)")
		log.Printf("   → podcastId: %s", cmd.PodcastId)
		log.Printf("   → episodeIndex: %d", cmd.EpisodeIndex)
		log.Printf("   → Target: Android MediaDash via BLE")
	case "request_podcast_list":
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("📋 PODCAST: Request channel list (A-Z)")
		log.Printf("   → Target: Android MediaDash via BLE")
		log.Printf("   → Response: Will be stored in podcast:list Redis key")
	case "request_recent_episodes":
		limit := cmd.Limit
		if limit <= 0 {
			limit = 30
		}
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("🕐 PODCAST: Request recent episodes")
		log.Printf("   → limit: %d", limit)
		log.Printf("   → Target: Android MediaDash via BLE")
		log.Printf("   → Response: Will be stored in podcast:recent Redis key")
	case "request_podcast_episodes":
		// Pass through pagination fields, apply default limit if not set
		if bleCmd.Limit <= 0 {
			bleCmd.Limit = 15 // Default page size
		}
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("📑 PODCAST: Request episodes for podcast")
		log.Printf("   → podcastId: %s", cmd.PodcastId)
		log.Printf("   → offset: %d", cmd.Offset)
		log.Printf("   → limit: %d", bleCmd.Limit)
		log.Printf("   → Target: Android MediaDash via BLE")
		log.Printf("   → Response: Will be stored in podcast:episodes:%s Redis key", cmd.PodcastId)
	case "request_media_channels":
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("📺 MEDIA: Request media channel apps list")
		log.Printf("   → Target: Android MediaDash via BLE")
		log.Printf("   → Response: Binary format, will be stored in media:channels Redis key")
	case "select_media_channel":
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("🎛️ MEDIA: Select media channel to control")
		log.Printf("   → Channel: %s", cmd.Channel)
		log.Printf("   → Target: Android MediaDash via BLE")
		// Store the selected channel in Redis for local access
		if cmd.Channel != "" {
			c.redisStore.StoreControlledChannel(cmd.Channel)
		}
	}

	log.Printf("Converted command: %s -> BLE{Action: %s, Value: %d, PodcastId: %s, EpisodeIndex: %d, Offset: %d, Limit: %d, Channel: %s}",
		cmd.Action, bleCmd.Action, bleCmd.Value, bleCmd.PodcastId, bleCmd.EpisodeIndex, bleCmd.Offset, bleCmd.Limit, bleCmd.Channel)

	return bleCmd, nil
}

// shouldRetryCommand determines if a command should be retried based on the error type
func (c *Client) shouldRetryCommand(err error) bool {
	if err == nil {
		return false
	}

	// NEVER retry fatal connection errors - reconnection is already triggered
	// Retrying these would just spam errors until reconnection happens
	if c.isFatalConnectionError(err) {
		log.Printf("Fatal connection error - NOT retrying (reconnection in progress)")
		return false
	}

	errorStr := strings.ToLower(err.Error())

	// Don't retry on validation or marshaling errors
	nonRetryableErrors := []string{
		"marshal",
		"validation",
		"invalid",
		"parse",
		"json",
	}

	for _, nonRetryableError := range nonRetryableErrors {
		if strings.Contains(errorStr, nonRetryableError) {
			return false
		}
	}

	// Retry on transient connection/communication errors
	retryableErrors := []string{
		"att error",
		"timeout",
		"resource",
		"operation failed",
	}

	for _, retryableError := range retryableErrors {
		if strings.Contains(errorStr, retryableError) {
			return true
		}
	}

	// Default to retrying unknown errors
	return true
}

// queueForRetry adds a command to the retry queue
func (c *Client) queueForRetry(cmd *redis.PlaybackCommand, reason string) {
	c.retryQueueMutex.Lock()
	defer c.retryQueueMutex.Unlock()

	// Check if this command is already in the retry queue
	for i, existing := range c.retryQueue {
		if existing.command.Action == cmd.Action &&
			existing.command.Value == cmd.Value &&
			existing.command.Timestamp == cmd.Timestamp {
			// Update existing entry
			c.retryQueue[i].attempts++
			c.retryQueue[i].nextRetry = time.Now().Add(c.retryDelay * time.Duration(c.retryQueue[i].attempts))
			c.retryQueue[i].reason = reason
			log.Printf("Updated retry entry for command: %s (attempt %d)", cmd.Action, c.retryQueue[i].attempts)
			return
		}
	}

	// Add new retry entry
	retryEntry := retry{
		command:   cmd,
		attempts:  1,
		nextRetry: time.Now().Add(c.retryDelay),
		reason:    reason,
	}

	c.retryQueue = append(c.retryQueue, retryEntry)
	log.Printf("Queued command for retry: %s (reason: %s)", cmd.Action, reason)
}

// processRetryQueue processes commands in the retry queue
func (c *Client) processRetryQueue(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("Command retry processor started")

	for {
		select {
		case <-ctx.Done():
			log.Println("Retry processor shutting down due to context cancellation")
			return
		case <-c.stopChan:
			log.Println("Retry processor shutting down due to stop signal")
			return
		case <-ticker.C:
			c.processRetries()
		}
	}
}

// processRetries processes commands that are ready for retry
func (c *Client) processRetries() {
	c.retryQueueMutex.Lock()
	defer c.retryQueueMutex.Unlock()

	now := time.Now()
	var remainingRetries []retry

	for _, retryEntry := range c.retryQueue {
		if now.Before(retryEntry.nextRetry) {
			// Not ready for retry yet
			remainingRetries = append(remainingRetries, retryEntry)
			continue
		}

		if retryEntry.attempts >= c.maxRetryAttempts {
			// Maximum retry attempts reached
			log.Printf("Command permanently failed after %d attempts: %s (reason: %s)",
				retryEntry.attempts, retryEntry.command.Action, retryEntry.reason)
			c.updateCommandStats(false)
			continue
		}

		// Try to send the command again
		log.Printf("Retrying command: %s (attempt %d/%d, reason: %s)",
			retryEntry.command.Action, retryEntry.attempts+1, c.maxRetryAttempts, retryEntry.reason)

		// Check BLE connection
		c.mutex.RLock()
		char := c.characteristics["playbackControl"]
		connected := c.connected
		c.mutex.RUnlock()

		if !connected || char == nil {
			// Still not connected, update retry time and keep in queue
			retryEntry.attempts++
			retryEntry.nextRetry = now.Add(c.retryDelay * time.Duration(retryEntry.attempts))
			retryEntry.reason = "BLE still not connected"
			remainingRetries = append(remainingRetries, retryEntry)
			continue
		}

		// Convert and send command
		bleCmd, err := c.convertToBLECommand(retryEntry.command)
		if err != nil {
			log.Printf("Retry command conversion failed: %v", err)
			c.updateCommandStats(false)
			continue
		}

		data, err := json.Marshal(bleCmd)
		if err != nil {
			log.Printf("Retry command marshaling failed: %v", err)
			c.updateCommandStats(false)
			continue
		}

		if _, err := c.rateLimitedWrite(char, data); err != nil {
			// Retry failed, update retry time and keep in queue
			retryEntry.attempts++
			retryEntry.nextRetry = now.Add(c.retryDelay * time.Duration(retryEntry.attempts))
			retryEntry.reason = fmt.Sprintf("Retry failed: %v", err)
			remainingRetries = append(remainingRetries, retryEntry)
			c.trackError("ble_write_retry", err)
			log.Printf("Command retry failed: %v", err)
		} else {
			// Retry succeeded
			log.Printf("✓ Command retry succeeded: %s", retryEntry.command.Action)
			c.updateCommandStats(true)
		}
	}

	c.retryQueue = remainingRetries
}

// processAlbumArtRequests polls Redis for album art requests from the UI and forwards to Android
func (c *Client) processAlbumArtRequests(ctx context.Context) {
	// Poll every 100ms - less frequent than commands since album art requests are rare
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Track last processed request to avoid duplicate processing
	var lastProcessedHash string
	var lastProcessedTime time.Time

	log.Println("Album art request processor started - monitoring Redis key: mediadash:albumart:request")

	for {
		select {
		case <-ctx.Done():
			log.Println("Album art request processor shutting down due to context cancellation")
			return
		case <-c.stopChan:
			log.Println("Album art request processor shutting down due to stop signal")
			return
		case <-ticker.C:
			c.processNextAlbumArtRequest(&lastProcessedHash, &lastProcessedTime)
		}
	}
}

// processNextAlbumArtRequest checks for and processes album art requests from Redis
func (c *Client) processNextAlbumArtRequest(lastProcessedHash *string, lastProcessedTime *time.Time) {
	// Get album art request from Redis
	request, err := c.redisStore.GetAlbumArtRequest()
	if err != nil {
		log.Printf("Failed to get album art request from Redis: %v", err)
		return
	}

	if request == nil {
		return // No request pending
	}

	// Skip if we've already processed this exact request recently (within 5 seconds)
	if request.Hash == *lastProcessedHash && time.Since(*lastProcessedTime) < 5*time.Second {
		return
	}

	log.Printf("Album art request received from UI: hash=%s, timestamp=%d", request.Hash, request.Timestamp)

	// Check if album art already exists on disk before requesting from Android
	artFilePath := filepath.Join(c.cfg.AlbumArt.CacheDirectory, request.Hash+".webp")
	if _, err := os.Stat(artFilePath); err == nil {
		// File exists - update Redis path and skip BLE request
		log.Printf("Album art already cached at %s - skipping BLE request", artFilePath)
		if err := c.redisStore.SetAlbumArtFilePath(artFilePath); err != nil {
			log.Printf("Warning: Failed to update Redis album art path: %v", err)
		}
		// Clear the request since we've handled it
		c.redisStore.ClearAlbumArtRequest()
		*lastProcessedHash = request.Hash
		*lastProcessedTime = time.Now()
		return
	}

	// Check BLE connection
	c.mutex.RLock()
	connected := c.connected
	char := c.characteristics["albumArtRequest"]
	c.mutex.RUnlock()

	if !connected {
		log.Printf("Cannot forward album art request - BLE not connected")
		return
	}

	if char == nil {
		log.Printf("Album art request characteristic not available on Android device")
		// Clear the request since we can't process it
		c.redisStore.ClearAlbumArtRequest()
		return
	}

	// Forward request to Android via BLE
	if err := c.requestAlbumArt(request.Hash); err != nil {
		log.Printf("Failed to forward album art request to Android: %v", err)
		c.trackError("album_art_request_forward", err)
		return
	}

	// Successfully forwarded - update tracking and clear Redis request
	*lastProcessedHash = request.Hash
	*lastProcessedTime = time.Now()

	if err := c.redisStore.ClearAlbumArtRequest(); err != nil {
		log.Printf("Warning: Failed to clear album art request from Redis: %v", err)
	}

	log.Printf("✓ Successfully forwarded album art request to Android: hash=%s", request.Hash)
}

// updateCommandStats updates command processing statistics
func (c *Client) updateCommandStats(success bool) {
	c.commandProcessingStats.mutex.Lock()
	defer c.commandProcessingStats.mutex.Unlock()

	c.commandProcessingStats.lastCommandTime = time.Now()

	if success {
		c.commandProcessingStats.commandsProcessed++
	} else {
		c.commandProcessingStats.commandsFailed++
	}
}

// reportCommandProcessingHealth reports the health of command processing
func (c *Client) reportCommandProcessingHealth() {
	c.commandProcessingStats.mutex.RLock()
	processed := c.commandProcessingStats.commandsProcessed
	failed := c.commandProcessingStats.commandsFailed
	lastCommand := c.commandProcessingStats.lastCommandTime
	c.commandProcessingStats.mutex.RUnlock()

	c.retryQueueMutex.Lock()
	retryQueueSize := len(c.retryQueue)
	c.retryQueueMutex.Unlock()

	timeSinceLastCommand := time.Since(lastCommand)
	successRate := float64(0)
	if processed+failed > 0 {
		successRate = float64(processed) / float64(processed+failed) * 100
	}

	log.Printf("=== Command Processing Health ===")
	log.Printf("  Commands processed: %d", processed)
	log.Printf("  Commands failed: %d", failed)
	log.Printf("  Success rate: %.1f%%", successRate)
	log.Printf("  Retry queue size: %d", retryQueueSize)

	if !lastCommand.IsZero() {
		log.Printf("  Time since last command: %v", timeSinceLastCommand.Truncate(time.Second))

		if timeSinceLastCommand > 2*time.Minute {
			log.Printf("  Status: No recent command activity")
		} else {
			log.Printf("  Status: Active")
		}
	} else {
		log.Printf("  Status: No commands processed yet")
	}
	log.Printf("================================")
}

// monitorConnection monitors the BLE connection and triggers reconnection if needed
// Uses a combination of activity-based and forced health checks
func (c *Client) monitorConnection(ctx context.Context) {
	// Activity-based check interval
	monitorInterval := time.Duration(c.cfg.Ble.ConnectionMonitoring.HealthCheckIntervalMinutes) * time.Minute
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	// Forced health check every 5 minutes regardless of activity
	// This catches silent disconnections that activity-based checking might miss
	forcedCheckInterval := 5 * time.Minute
	forcedCheckTicker := time.NewTicker(forcedCheckInterval)
	defer forcedCheckTicker.Stop()

	log.Printf("Starting connection monitoring (activity check: %v, forced check: %v, activity timeout: %v)",
		monitorInterval, forcedCheckInterval,
		time.Duration(c.cfg.Ble.ConnectionMonitoring.ActivityTimeoutMinutes)*time.Minute)

	disconnectionLogged := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-forcedCheckTicker.C:
			// Forced health check - runs regardless of activity to catch silent disconnections
			c.mutex.RLock()
			device := c.device
			connected := c.connected
			connectionAge := time.Since(c.connectionStartTime)
			c.mutex.RUnlock()

			if connected && device != nil {
				log.Printf("Performing forced health check (every %v)", forcedCheckInterval)
				if c.testConnectionHealth() {
					atomic.StoreInt32(&c.connectionHealthy, 1)
					disconnectionLogged = false
					log.Printf("Forced health check passed - connection verified after %v",
						connectionAge.Truncate(time.Second))
				} else {
					atomic.StoreInt32(&c.connectionHealthy, 0)
					if !disconnectionLogged {
						log.Printf("Forced health check FAILED - connection dead after %v, triggering reconnection",
							connectionAge.Truncate(time.Second))
						c.handleDisconnection()
						disconnectionLogged = true
					}
				}
			}
		case <-ticker.C:
			c.mutex.RLock()
			device := c.device
			connected := c.connected
			connectionAge := time.Since(c.connectionStartTime)
			c.mutex.RUnlock()

			if connected && device != nil {
				// Only perform activity-based health check if no recent activity
				if c.shouldPerformHealthCheck() {
					lastActivity := c.getLastActivity()
					timeSinceActivity := time.Since(lastActivity)

					log.Printf("Performing health check: no activity for %v (threshold: %v)",
						timeSinceActivity.Truncate(time.Second),
						time.Duration(c.cfg.Ble.ConnectionMonitoring.ActivityTimeoutMinutes)*time.Minute)

					if c.testConnectionHealth() {
						atomic.StoreInt32(&c.connectionHealthy, 1)
						disconnectionLogged = false
						log.Printf("Health check passed - connection healthy after %v",
							connectionAge.Truncate(time.Second))
					} else {
						atomic.StoreInt32(&c.connectionHealthy, 0)
						if !disconnectionLogged {
							log.Printf("Health check failed after %v (no activity for %v)",
								connectionAge.Truncate(time.Second), timeSinceActivity.Truncate(time.Second))
							c.handleDisconnection()
							disconnectionLogged = true
						}
					}
				} else {
					// Recent activity detected - skip activity-based health check
					timeSinceActivity := time.Since(c.getLastActivity())
					log.Printf("Skipping activity check: recent activity detected (%v ago)",
						timeSinceActivity.Truncate(time.Second))
					atomic.StoreInt32(&c.connectionHealthy, 1)
					disconnectionLogged = false

					// Log connection stability for long connections
					if connectionAge > 60*time.Second && int(connectionAge.Minutes())%10 == 0 {
						log.Printf("Connection stable for %v (active: %v ago)",
							connectionAge.Truncate(time.Second), timeSinceActivity.Truncate(time.Second))
					}
				}
			}
		}
	}
}

// testConnectionHealth performs an active connection health check by attempting a read operation
func (c *Client) testConnectionHealth() bool {
	c.mutex.RLock()
	device := c.device
	connected := c.connected
	c.mutex.RUnlock()

	if !connected || device == nil {
		log.Printf("Health check: not connected or device is nil")
		return false
	}

	// Actually try to read from the characteristic to verify the connection is alive
	// This is the only way to detect stale D-Bus connections
	if char := c.characteristics["mediaState"]; char != nil {
		// Attempt a read operation - this will fail if the connection is dead
		_, err := char.Read(make([]byte, 1))
		if err != nil {
			// Check if this is a fatal connection error
			if c.isFatalConnectionError(err) {
				log.Printf("Health check: FATAL connection error detected: %v", err)
				return false
			}
			// Other errors (like read not supported) are OK - connection is still alive
			log.Printf("Health check: read error (non-fatal): %v", err)
		}
		return true
	}

	log.Printf("Health check: mediaState characteristic not found")
	return false
}

// isFatalConnectionError checks if an error indicates the BLE connection is dead
func (c *Client) isFatalConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()

	// D-Bus errors indicating the connection/object no longer exists
	fatalPatterns := []string{
		"doesn't exist",
		"not connected",
		"No such object",
		"connection lost",
		"device not connected",
		"org.freedesktop.DBus.Error.UnknownObject",
		"org.freedesktop.DBus.Error.ServiceUnknown",
		"org.bluez.Error.NotConnected",
		"org.bluez.Error.Failed",
	}

	for _, pattern := range fatalPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// handleDisconnection handles disconnection events with error analysis
func (c *Client) handleDisconnection() {
	c.mutex.Lock()
	connectionDuration := time.Since(c.connectionStartTime)
	wasConnected := c.connected

	// Avoid duplicate disconnection handling
	if !wasConnected && c.device == nil {
		c.mutex.Unlock()
		log.Printf("handleDisconnection called but already disconnected - skipping")
		return
	}

	c.connected = false
	if c.device != nil {
		c.device.Disconnect()
		c.device = nil
	}
	c.mutex.Unlock()

	if wasConnected {
		log.Printf("Device disconnected after %v", connectionDuration.Truncate(time.Second))

		// Log disconnection to persistent file
		LogDisconnect(fmt.Sprintf("connection duration: %v, consecutive errors: %d", connectionDuration.Truncate(time.Second), c.consecutiveErrors))

		// Analyze disconnection pattern
		if connectionDuration < 15*time.Second {
			log.Printf("Short-lived connection detected (< 15s) - possible ATT error or resource issue")
			c.consecutiveErrors++
			LogConnectionIssue("short-lived connection detected (<15s)", nil)
		} else {
			c.consecutiveErrors = 0 // Reset on successful long connection
		}

		// Update Redis with disconnection status
		if err := c.redisStore.SetBLEConnectionStatus(false, ""); err != nil {
			log.Printf("Warning: Failed to update Redis disconnection status: %v", err)
			LogConnectionIssue("failed to update Redis disconnection status", err)
		}

		// Clear retry queue - old commands are stale after disconnection
		c.clearRetryQueue()
	}

	atomic.StoreInt32(&c.connectionHealthy, 0)
	c.scheduleReconnect()
}

// clearRetryQueue clears all pending retries (called on disconnection)
func (c *Client) clearRetryQueue() {
	c.retryQueueMutex.Lock()
	defer c.retryQueueMutex.Unlock()

	if len(c.retryQueue) > 0 {
		log.Printf("Clearing %d pending retry commands due to disconnection", len(c.retryQueue))
		c.retryQueue = nil
	}
}

// scheduleReconnect schedules a reconnection attempt with intelligent backoff
func (c *Client) scheduleReconnect() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.reconnectAttempts++

	// Intelligent delay calculation based on error patterns
	var delay time.Duration
	if c.consecutiveErrors >= 3 {
		// Multiple quick disconnects - likely ATT error pattern
		delay = 30 * time.Second // Longer delay for ATT errors
		log.Printf("Detected rapid disconnect pattern (%d consecutive), using extended delay", c.consecutiveErrors)
	} else {
		// Normal exponential backoff
		delay = c.reconnectDelay * time.Duration(1<<uint(c.reconnectAttempts-1))
		if delay > c.maxReconnectDelay {
			delay = c.maxReconnectDelay
		}
	}

	c.reconnectDelay = delay

	log.Printf("Scheduling reconnection attempt %d in %v", c.reconnectAttempts, delay)

	go func() {
		time.Sleep(delay)
		c.triggerReconnect()
	}()
}

// triggerReconnect triggers a reconnection attempt
func (c *Client) triggerReconnect() {
	select {
	case c.reconnectChan <- struct{}{}:
	default:
		// Channel is full, reconnection already pending
	}
}

// TriggerExternalReconnect allows external code (like main.go) to request a reconnection
// This is used when the UI sends a reconnect request via Redis
func (c *Client) TriggerExternalReconnect() {
	c.mutex.RLock()
	wasConnected := c.connected
	c.mutex.RUnlock()

	if wasConnected {
		log.Printf("External reconnect requested - disconnecting current connection first")
		// handleDisconnection will disconnect and schedule reconnection
		c.handleDisconnection()
	} else {
		log.Printf("External reconnect requested - initiating connection attempt")
		// Reset reconnection parameters for immediate retry
		c.reconnectAttempts = 0
		c.reconnectDelay = 1 * time.Second
		c.consecutiveErrors = 0
		// Trigger the reconnection
		c.triggerReconnect()
	}
}

// periodicCleanup performs periodic cleanup tasks
func (c *Client) periodicCleanup(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			// Clean up stale album art transfers
			c.albumHandler.CleanupStaleTransfers(5 * time.Minute)
		}
	}
}

// hasMediaStateChanged compares the new update with the last known state with optimized detection
func (c *Client) hasMediaStateChanged(update *MediaStateUpdate) (bool, string) {
	c.mediaStateMutex.RLock()
	last := c.lastMediaState
	c.mediaStateMutex.RUnlock()

	if last == nil {
		return true, "initial"
	}

	var changes []string

	// Priority changes - always trigger updates for critical state changes
	actuallyPlayingNow := update.IsPlaying || (update.PlaybackState == "playing")
	actuallyPlayingLast := last.IsPlaying || (last.PlaybackState == "playing")
	
	if actuallyPlayingLast != actuallyPlayingNow {
		changes = append(changes, "playback")
	}
	
	// Also check for PlaybackState changes specifically (Android might send different combinations)
	if last.PlaybackState != update.PlaybackState {
		changes = append(changes, "playback_state")
	}
	
	// Track metadata changes
	if last.TrackTitle != update.TrackTitle {
		changes = append(changes, "track")
	}
	if last.Artist != update.Artist {
		changes = append(changes, "artist")
	}
	if last.Album != update.Album {
		changes = append(changes, "album")
	}
	if last.Volume != update.Volume {
		changes = append(changes, "volume")
	}
	if last.Duration != update.Duration {
		changes = append(changes, "duration")
	}
	if last.AlbumArtHash != update.AlbumArtHash {
		changes = append(changes, "albumart")
	}

	// Optimized position change detection - more sensitive to user interactions
	positionDiff := update.Position - last.Position
	if positionDiff < 0 || positionDiff > 1000 { // Reduced threshold for better responsiveness
		changes = append(changes, "position")
	}

	hasChanges := len(changes) > 0
	changedFields := strings.Join(changes, ", ")

	return hasChanges, changedFields
}

// updateCachedMediaState updates the cached media state
func (c *Client) updateCachedMediaState(update *MediaStateUpdate) {
	// Determine actual playing state from both fields
	actuallyPlaying := update.IsPlaying || (update.PlaybackState == "playing")
	
	c.mediaStateMutex.Lock()
	c.lastMediaState = &MediaStateUpdate{
		IsPlaying:     actuallyPlaying,
		PlaybackState: update.PlaybackState,
		TrackTitle:    update.TrackTitle,
		Artist:        update.Artist,
		Album:         update.Album,
		Duration:      update.Duration,
		Position:      update.Position,
		Volume:        update.Volume,
		AlbumArtHash:  update.AlbumArtHash,
	}
	c.mediaStateMutex.Unlock()
}

// logComprehensiveMediaState logs comprehensive track information in a readable format
func (c *Client) logComprehensiveMediaState(update *MediaStateUpdate, changedFields string) {
	log.Printf("=== Media State Update ===")
	log.Printf("  Track:     %s", update.TrackTitle)
	log.Printf("  Artist:    %s", update.Artist)
	log.Printf("  Album:     %s", update.Album)

	// Format duration and position as human-readable time
	durationStr := c.formatDuration(update.Duration)
	positionStr := c.formatDuration(update.Position)
	log.Printf("  Duration:  %s (%d ms)", durationStr, update.Duration)
	log.Printf("  Position:  %s (%d ms)", positionStr, update.Position)

	// Calculate and display progress percentage
	var progressPercent float64
	if update.Duration > 0 {
		progressPercent = (float64(update.Position) / float64(update.Duration)) * 100
	}
	log.Printf("  Progress:  %.1f%%", progressPercent)

	// Playback state with clear indication - use actual playing state
	actuallyPlaying := update.IsPlaying || (update.PlaybackState == "playing")
	playStateStr := "PAUSED"
	if actuallyPlaying {
		playStateStr = "PLAYING"
	}
	if update.PlaybackState != "" {
		playStateStr += fmt.Sprintf(" (%s)", update.PlaybackState)
	}
	log.Printf("  State:     %s", playStateStr)
	log.Printf("  Volume:    %d%%", update.Volume)

	// Album art information
	if update.AlbumArtHash != "" {
		log.Printf("  Album Art: %s (hash)", update.AlbumArtHash)
	} else {
		log.Printf("  Album Art: <none>")
	}

	// Show what fields changed
	log.Printf("  Changed:   [%s]", changedFields)

	// Show metadata completeness summary
	completeness := c.getMetadataCompleteness(update)
	log.Printf("  Metadata:  %s", completeness)
	log.Printf("=============================")
}

// getMetadataCompleteness provides a summary of what metadata fields are populated
func (c *Client) getMetadataCompleteness(update *MediaStateUpdate) string {
	var present []string
	var missing []string

	if update.TrackTitle != "" {
		present = append(present, "Title")
	} else {
		missing = append(missing, "Title")
	}

	if update.Artist != "" {
		present = append(present, "Artist")
	} else {
		missing = append(missing, "Artist")
	}

	if update.Album != "" {
		present = append(present, "Album")
	} else {
		missing = append(missing, "Album")
	}

	if update.Duration > 0 {
		present = append(present, "Duration")
	} else {
		missing = append(missing, "Duration")
	}

	if update.AlbumArtHash != "" {
		present = append(present, "AlbumArt")
	} else {
		missing = append(missing, "AlbumArt")
	}

	// Always present: IsPlaying, Position, Volume
	present = append(present, "State", "Position", "Volume")

	result := fmt.Sprintf("Present: [%s]", strings.Join(present, ", "))
	if len(missing) > 0 {
		result += fmt.Sprintf(" | Missing: [%s]", strings.Join(missing, ", "))
	}

	return result
}

// formatDuration converts milliseconds to a human-readable time format (MM:SS or HH:MM:SS)
func (c *Client) formatDuration(milliseconds int64) string {
	if milliseconds < 0 {
		return "00:00"
	}

	seconds := milliseconds / 1000
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60

	if hours > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, secs)
	}
	return fmt.Sprintf("%02d:%02d", minutes, secs)
}

// logDuplicateNotification logs duplicate notifications at a reduced rate with comprehensive info
func (c *Client) logDuplicateNotification(update *MediaStateUpdate) {
	// Counter-based rate limiting - only log every 10th duplicate
	c.duplicateCounterMutex.Lock()
	c.duplicateCounter++
	counter := c.duplicateCounter
	c.duplicateCounterMutex.Unlock()

	if counter%10 == 0 {
		positionStr := c.formatDuration(update.Position)
		durationStr := c.formatDuration(update.Duration)
		playStateStr := "PAUSED"
		if update.IsPlaying {
			playStateStr = "PLAYING"
		}

		log.Printf("=== Duplicate Notification #%d (no significant changes) ===", counter)
		log.Printf("  Track: %s | Artist: %s | Album: %s", update.TrackTitle, update.Artist, update.Album)
		log.Printf("  State: %s | Position: %s/%s | Volume: %d%%",
			playStateStr, positionStr, durationStr, update.Volume)
		if update.AlbumArtHash != "" {
			log.Printf("  Album Art: %s", update.AlbumArtHash)
		}
		log.Printf("=======================================================")
	}
}

// handleAlbumArtHashChange handles album art hash changes separately with validation
// If Android sends an empty hash but artist/album are available, computes the hash locally
func (c *Client) handleAlbumArtHashChange(newHash string, artist, album string) {
	// If Android didn't send a hash but we have artist and album, compute it ourselves
	// This ensures we can proactively request album art for new tracks
	effectiveHash := newHash
	hashWasComputed := false
	if effectiveHash == "" && artist != "" && album != "" {
		effectiveHash = albumart.GenerateAlbumArtHash(artist, album)
		hashWasComputed = true
		log.Printf("BLE_AA_ADVANCED: Android sent empty hash, computed from artist|album: %s (artist='%s', album='%s')", effectiveHash, artist, album)
	}

	c.albumArtHashMutex.Lock()
	lastHash := c.lastAlbumArtHash
	c.lastAlbumArtHash = effectiveHash
	c.albumArtHashMutex.Unlock()

	if effectiveHash != "" && effectiveHash != lastHash {
		// Cancel any stale requests for old hashes before processing new one
		if c.albumHandler != nil {
			c.albumHandler.CancelStaleRequests(effectiveHash)
		}

		// Validate the hash matches the expected artist/album combination (only if Android provided a hash)
		if !hashWasComputed && artist != "" && album != "" {
			expectedHash := albumart.GenerateAlbumArtHash(artist, album)
			if effectiveHash == expectedHash {
				log.Printf("BLE_AA_ADVANCED: Album art hash changed: %s -> %s (✓ validated for %s|%s)", lastHash, effectiveHash, artist, album)
			} else {
				log.Printf("BLE_AA_ADVANCED: Album art hash changed: %s -> %s (⚠️ mismatch: expected %s for %s|%s)",
					lastHash, effectiveHash, expectedHash, artist, album)
			}
		} else if !hashWasComputed {
			log.Printf("BLE_AA_ADVANCED: Album art hash changed: %s -> %s (waiting for proactive send)", lastHash, effectiveHash)
		} else {
			log.Printf("BLE_AA_ADVANCED: Album art hash changed (computed): %s -> %s for %s|%s", lastHash, effectiveHash, artist, album)
		}

		// Check if we already have this album art cached
		cachedData, err := c.albumHandler.GetCachedAlbumArt(effectiveHash)
		if err != nil {
			log.Printf("BLE_AA_ADVANCED: Error checking album art cache for hash %s: %v", effectiveHash, err)
		} else if cachedData != nil {
			log.Printf("BLE_AA_ADVANCED: Album art already cached for hash %s (%d bytes) - updating Redis path", effectiveHash, len(cachedData))
			artFilePath := filepath.Join(c.cfg.AlbumArt.CacheDirectory, effectiveHash+".webp")
			if err := c.redisStore.SetAlbumArtFilePath(artFilePath); err != nil {
				log.Printf("BLE_AA_ADVANCED: Failed to update Redis album art path: %v", err)
			} else {
				log.Printf("BLE_AA_ADVANCED: Successfully updated Redis album art path: %s", artFilePath)
			}
		} else {
			log.Printf("BLE_AA_ADVANCED: Album art not cached for hash %s - requesting from Android", effectiveHash)
			// Request album art from Android server
			if err := c.requestAlbumArt(effectiveHash); err != nil {
				log.Printf("BLE_AA_ADVANCED: Failed to request album art for hash %s: %v", effectiveHash, err)
			} else {
				log.Printf("BLE_AA_ADVANCED: Successfully sent album art request for hash %s", effectiveHash)
			}
		}
	} else if effectiveHash == "" && lastHash != "" {
		// Album art removed (no hash and no artist/album to compute from)
		log.Printf("BLE_AA_ADVANCED: Album art removed (was: %s)", lastHash)
		if err := c.redisStore.ClearAlbumArtFilePath(); err != nil {
			log.Printf("BLE_AA_ADVANCED: Failed to clear Redis album art path: %v", err)
		} else {
			log.Printf("BLE_AA_ADVANCED: Successfully cleared Redis album art path")
		}
	}
	// If effectiveHash == lastHash, no change needed - don't log or clear
}

// IsConnected returns the current connection status
func (c *Client) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.connected
}

// GetConnectionStatus returns detailed connection status
func (c *Client) GetConnectionStatus() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var connectionAge time.Duration
	if !c.connectionStartTime.IsZero() {
		connectionAge = time.Since(c.connectionStartTime)
	}

	status := map[string]interface{}{
		"connected":         c.connected,
		"reconnectAttempts": c.reconnectAttempts,
		"reconnectDelay":    c.reconnectDelay.String(),
		"connectionAge":     connectionAge.String(),
		"consecutiveErrors": c.consecutiveErrors,
		"healthy":           atomic.LoadInt32(&c.connectionHealthy) == 1,
	}

	if c.lastError != nil {
		status["lastError"] = c.lastError.Error()
	}

	if len(c.attErrorCount) > 0 {
		status["errorCounts"] = c.attErrorCount
	}

	return status
}

// GetDiagnosticInfo returns comprehensive diagnostic information
func (c *Client) GetDiagnosticInfo() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	c.activityMutex.RLock()
	lastSendActivity := c.lastSendActivity
	lastReceiveActivity := c.lastReceiveActivity
	c.activityMutex.RUnlock()

	// Get command processing statistics
	c.commandProcessingStats.mutex.RLock()
	commandsProcessed := c.commandProcessingStats.commandsProcessed
	commandsFailed := c.commandProcessingStats.commandsFailed
	lastCommandTime := c.commandProcessingStats.lastCommandTime
	c.commandProcessingStats.mutex.RUnlock()

	// Get retry queue statistics
	c.retryQueueMutex.Lock()
	retryQueueSize := len(c.retryQueue)
	retryQueueEntries := make([]map[string]interface{}, len(c.retryQueue))
	for i, retry := range c.retryQueue {
		retryQueueEntries[i] = map[string]interface{}{
			"command":   retry.command.Action,
			"attempts":  retry.attempts,
			"nextRetry": retry.nextRetry,
			"reason":    retry.reason,
		}
	}
	c.retryQueueMutex.Unlock()

	// Calculate command success rate
	var successRate float64
	if commandsProcessed+commandsFailed > 0 {
		successRate = float64(commandsProcessed) / float64(commandsProcessed+commandsFailed) * 100
	}

	diagnostics := map[string]interface{}{
		"connection": c.GetConnectionStatus(),
		"characteristics": map[string]bool{
			"mediaState":      c.characteristics["mediaState"] != nil,
			"playbackControl": c.characteristics["playbackControl"] != nil,
			"albumArtRequest": c.characteristics["albumArtRequest"] != nil, // Optional for recovery
			"albumArtData":    c.characteristics["albumArtData"] != nil,
		},
		"flowControl": map[string]interface{}{
			"mediaState": map[string]interface{}{
				"bufferSize":      cap(c.mediaStateFlowControl),
				"currentlyQueued": len(c.mediaStateFlowControl),
				"utilization":     fmt.Sprintf("%.1f%%", float64(len(c.mediaStateFlowControl))/float64(cap(c.mediaStateFlowControl))*100),
			},
			"albumArt": map[string]interface{}{
				"bufferSize":      cap(c.albumArtFlowControl),
				"currentlyQueued": len(c.albumArtFlowControl),
				"utilization":     fmt.Sprintf("%.1f%%", float64(len(c.albumArtFlowControl))/float64(cap(c.albumArtFlowControl))*100),
			},
		},
		"notificationMetrics": c.getNotificationMetrics(),
		"rateLimiting": map[string]interface{}{
			"writeIntervalMs":    c.cfg.Ble.RateLimiting.WriteIntervalMs,
			"tokensAvailable":    len(c.rateLimiter),
			"lastWriteTime":      c.lastWriteTime,
			"timeSinceLastWrite": time.Since(c.lastWriteTime).String(),
		},
		"activityTracking": map[string]interface{}{
			"lastSendActivity":         lastSendActivity,
			"lastReceiveActivity":      lastReceiveActivity,
			"timeSinceLastSend":        time.Since(lastSendActivity).String(),
			"timeSinceLastReceive":     time.Since(lastReceiveActivity).String(),
			"shouldPerformHealthCheck": c.shouldPerformHealthCheck(),
		},
		"commandProcessing": map[string]interface{}{
			"commandsProcessed":    commandsProcessed,
			"commandsFailed":       commandsFailed,
			"successRate":          successRate,
			"lastCommandTime":      lastCommandTime,
			"timeSinceLastCommand": time.Since(lastCommandTime).String(),
			"maxRetryAttempts":     c.maxRetryAttempts,
			"retryDelay":           c.retryDelay.String(),
			"retryQueue": map[string]interface{}{
				"size":    retryQueueSize,
				"entries": retryQueueEntries,
			},
		},
		"albumArt": map[string]interface{}{
			"retryStatus":      c.albumHandler.GetRetryStatus(),
			"validationStatus": c.albumHandler.GetValidationStatus(),
			"migrationStatus":  c.albumHandler.GetMigrationStatus(),
			"transferStatus":   c.getAlbumArtTransferStatus(),
		},
	}

	// Add ATT error 0x0e specific diagnostics
	if attErrorCount, exists := c.attErrorCount["att_0x0e"]; exists && attErrorCount > 0 {
		diagnostics["att0x0eAnalysis"] = map[string]interface{}{
			"count":       attErrorCount,
			"description": "ATT Error 0x0e indicates Android server resource exhaustion",
			"likely_causes": []string{
				"Android BLE server notification buffer overflow",
				"Connection parameter mismatch causing resource exhaustion",
				"Android battery optimization interfering with BLE operations",
				"Too frequent characteristic notifications from server",
			},
			"recommendations": []string{
				"Restart Android companion app",
				"Check Android battery optimization whitelist",
				"Reduce BLE notification frequency if possible",
				"Consider longer connection intervals",
			},
		}
	}

	return diagnostics
}

// getAlbumArtTransferStatus returns current album art transfer status
func (c *Client) getAlbumArtTransferStatus() map[string]interface{} {
	// This would typically get status from the album handler
	// For now, return basic status
	status := map[string]interface{}{
		"enabled": true,
	}

	// Add any active transfer information if available
	c.albumArtHashMutex.RLock()
	lastHash := c.lastAlbumArtHash
	c.albumArtHashMutex.RUnlock()

	if lastHash != "" {
		status["lastRequestedHash"] = lastHash
	}

	return status
}
