package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed config.json
var configFile []byte

// Config represents the structure of the config.json file.
type Config struct {
	Ble struct {
		ServiceUUID                       string `json:"serviceUUID"`
		MediaStateCharacteristicUUID      string `json:"mediaStateCharacteristicUUID"`
		PlaybackControlCharacteristicUUID string `json:"playbackControlCharacteristicUUID"`
		AlbumArtRequestCharacteristicUUID string `json:"albumArtRequestCharacteristicUUID"`
		AlbumArtDataCharacteristicUUID    string `json:"albumArtDataCharacteristicUUID"`
		PodcastInfoCharacteristicUUID     string `json:"podcastInfoCharacteristicUUID"`
		LyricsRequestCharacteristicUUID   string `json:"lyricsRequestCharacteristicUUID"`
		LyricsDataCharacteristicUUID      string `json:"lyricsDataCharacteristicUUID"`
		SettingsCharacteristicUUID        string `json:"settingsCharacteristicUUID"`
		RateLimiting                      struct {
			WriteIntervalMs int    `json:"writeIntervalMs"`
			Description     string `json:"description"`
		} `json:"rateLimiting"`
		ConnectionMonitoring struct {
			HealthCheckIntervalMinutes int    `json:"healthCheckIntervalMinutes"`
			ActivityTimeoutMinutes     int    `json:"activityTimeoutMinutes"`
			Description                string `json:"description"`
		} `json:"connectionMonitoring"`
	} `json:"ble"`
	Redis struct {
		Address string            `json:"address"`
		KeyMap  map[string]string `json:"keyMap"`
	} `json:"redis"`
	AlbumArt struct {
		CacheDirectory string `json:"cacheDirectory"`
	} `json:"albumArt"`
}

// Load parses the embedded config.json file into the Config struct.
func Load() (*Config, error) {
	var cfg Config
	err := json.Unmarshal(configFile, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse embedded config.json: %w", err)
	}
	return &cfg, nil
}
