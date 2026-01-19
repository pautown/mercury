package settings

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// Settings represents application configuration settings
type Settings struct {
	BLE struct {
		ScanTimeout       int  `json:"scanTimeout"`       // seconds
		ConnectionTimeout int  `json:"connectionTimeout"` // seconds
		EnableAutoConnect bool `json:"enableAutoConnect"`
		MaxReconnectDelay int  `json:"maxReconnectDelay"` // seconds
	} `json:"ble"`

	Redis struct {
		CommandTimeout int `json:"commandTimeout"` // seconds
		MaxRetries     int `json:"maxRetries"`
	} `json:"redis"`

	AlbumArt struct {
		MaxCacheSize    int64 `json:"maxCacheSize"`    // bytes
		CacheExpiration int   `json:"cacheExpiration"` // hours
		ChunkTimeout    int   `json:"chunkTimeout"`    // seconds
		MaxTransferSize int64 `json:"maxTransferSize"` // bytes
	} `json:"albumArt"`

	Logging struct {
		Level      string `json:"level"` // debug, info, warn, error
		EnableFile bool   `json:"enableFile"`
		FilePath   string `json:"filePath"`
	} `json:"logging"`
}

// Handler manages application settings
type Handler struct {
	settings *Settings
	mutex    sync.RWMutex
	onChange func(*Settings)
}

// NewHandler creates a new settings handler with defaults
func NewHandler(onChange func(*Settings)) *Handler {
	handler := &Handler{
		settings: getDefaultSettings(),
		onChange: onChange,
	}

	return handler
}

// getDefaultSettings returns default application settings
func getDefaultSettings() *Settings {
	return &Settings{
		BLE: struct {
			ScanTimeout       int  `json:"scanTimeout"`
			ConnectionTimeout int  `json:"connectionTimeout"`
			EnableAutoConnect bool `json:"enableAutoConnect"`
			MaxReconnectDelay int  `json:"maxReconnectDelay"`
		}{
			ScanTimeout:       30,
			ConnectionTimeout: 10,
			EnableAutoConnect: true,
			MaxReconnectDelay: 60,
		},

		Redis: struct {
			CommandTimeout int `json:"commandTimeout"`
			MaxRetries     int `json:"maxRetries"`
		}{
			CommandTimeout: 5,
			MaxRetries:     3,
		},

		AlbumArt: struct {
			MaxCacheSize    int64 `json:"maxCacheSize"`
			CacheExpiration int   `json:"cacheExpiration"`
			ChunkTimeout    int   `json:"chunkTimeout"`
			MaxTransferSize int64 `json:"maxTransferSize"`
		}{
			MaxCacheSize:    100 * 1024 * 1024, // 100MB
			CacheExpiration: 24,                // 24 hours
			ChunkTimeout:    30,                // 30 seconds
			MaxTransferSize: 5 * 1024 * 1024,   // 5MB
		},

		Logging: struct {
			Level      string `json:"level"`
			EnableFile bool   `json:"enableFile"`
			FilePath   string `json:"filePath"`
		}{
			Level:      "info",
			EnableFile: false,
			FilePath:   "/tmp/mediadash-client.log",
		},
	}
}

// GetSettings returns a copy of the current settings
func (h *Handler) GetSettings() *Settings {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	// Return a deep copy to prevent external modification
	data, _ := json.Marshal(h.settings)
	var copy Settings
	json.Unmarshal(data, &copy)

	return &copy
}

// UpdateSettings updates the settings with new values
func (h *Handler) UpdateSettings(newSettings *Settings) error {
	// Validate settings
	if err := h.validateSettings(newSettings); err != nil {
		return fmt.Errorf("invalid settings: %w", err)
	}

	h.mutex.Lock()
	h.settings = newSettings
	h.mutex.Unlock()

	log.Println("Settings updated")

	// Notify of changes
	if h.onChange != nil {
		go h.onChange(newSettings)
	}

	return nil
}

// validateSettings validates settings values
func (h *Handler) validateSettings(settings *Settings) error {
	// Validate BLE settings
	if settings.BLE.ScanTimeout < 5 || settings.BLE.ScanTimeout > 300 {
		return fmt.Errorf("scan timeout must be between 5 and 300 seconds")
	}

	if settings.BLE.ConnectionTimeout < 5 || settings.BLE.ConnectionTimeout > 60 {
		return fmt.Errorf("connection timeout must be between 5 and 60 seconds")
	}

	if settings.BLE.MaxReconnectDelay < 10 || settings.BLE.MaxReconnectDelay > 600 {
		return fmt.Errorf("max reconnect delay must be between 10 and 600 seconds")
	}

	// Validate Redis settings
	if settings.Redis.CommandTimeout < 1 || settings.Redis.CommandTimeout > 30 {
		return fmt.Errorf("command timeout must be between 1 and 30 seconds")
	}

	if settings.Redis.MaxRetries < 1 || settings.Redis.MaxRetries > 10 {
		return fmt.Errorf("max retries must be between 1 and 10")
	}

	// Validate album art settings
	if settings.AlbumArt.MaxCacheSize < 10*1024*1024 || settings.AlbumArt.MaxCacheSize > 1024*1024*1024 {
		return fmt.Errorf("max cache size must be between 10MB and 1GB")
	}

	if settings.AlbumArt.CacheExpiration < 1 || settings.AlbumArt.CacheExpiration > 168 {
		return fmt.Errorf("cache expiration must be between 1 and 168 hours")
	}

	if settings.AlbumArt.ChunkTimeout < 5 || settings.AlbumArt.ChunkTimeout > 300 {
		return fmt.Errorf("chunk timeout must be between 5 and 300 seconds")
	}

	if settings.AlbumArt.MaxTransferSize < 1024*1024 || settings.AlbumArt.MaxTransferSize > 50*1024*1024 {
		return fmt.Errorf("max transfer size must be between 1MB and 50MB")
	}

	// Validate logging settings
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[settings.Logging.Level] {
		return fmt.Errorf("invalid logging level: %s", settings.Logging.Level)
	}

	return nil
}

// LoadFromJSON loads settings from JSON data
func (h *Handler) LoadFromJSON(data []byte) error {
	var settings Settings
	if err := json.Unmarshal(data, &settings); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	return h.UpdateSettings(&settings)
}

// ToJSON returns settings as JSON
func (h *Handler) ToJSON() ([]byte, error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	data, err := json.MarshalIndent(h.settings, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal settings: %w", err)
	}

	return data, nil
}

// GetBLEScanTimeout returns the BLE scan timeout
func (h *Handler) GetBLEScanTimeout() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.BLE.ScanTimeout
}

// GetBLEConnectionTimeout returns the BLE connection timeout
func (h *Handler) GetBLEConnectionTimeout() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.BLE.ConnectionTimeout
}

// IsAutoConnectEnabled returns whether auto-connect is enabled
func (h *Handler) IsAutoConnectEnabled() bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.BLE.EnableAutoConnect
}

// GetMaxReconnectDelay returns the maximum reconnection delay
func (h *Handler) GetMaxReconnectDelay() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.BLE.MaxReconnectDelay
}

// GetRedisCommandTimeout returns the Redis command timeout
func (h *Handler) GetRedisCommandTimeout() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.Redis.CommandTimeout
}

// GetRedisMaxRetries returns the maximum Redis retries
func (h *Handler) GetRedisMaxRetries() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.Redis.MaxRetries
}

// GetAlbumArtMaxCacheSize returns the maximum album art cache size
func (h *Handler) GetAlbumArtMaxCacheSize() int64 {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.AlbumArt.MaxCacheSize
}

// GetAlbumArtCacheExpiration returns the album art cache expiration time
func (h *Handler) GetAlbumArtCacheExpiration() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.AlbumArt.CacheExpiration
}

// GetAlbumArtChunkTimeout returns the album art chunk timeout
func (h *Handler) GetAlbumArtChunkTimeout() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.AlbumArt.ChunkTimeout
}

// GetAlbumArtMaxTransferSize returns the maximum album art transfer size
func (h *Handler) GetAlbumArtMaxTransferSize() int64 {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.AlbumArt.MaxTransferSize
}

// GetLoggingLevel returns the current logging level
func (h *Handler) GetLoggingLevel() string {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.Logging.Level
}

// IsFileLoggingEnabled returns whether file logging is enabled
func (h *Handler) IsFileLoggingEnabled() bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.Logging.EnableFile
}

// GetLogFilePath returns the log file path
func (h *Handler) GetLogFilePath() string {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.settings.Logging.FilePath
}
