package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"mediadash/client/internal/config"
	"mediadash/client/internal/debug"
)

// Store represents a Redis client wrapper for MediaDash operations
type Store struct {
	client *redis.Client
	keyMap map[string]string
	ctx    context.Context
}

// MediaState represents the media playback state
type MediaState struct {
	IsPlaying    bool   `json:"isPlaying"`
	TrackTitle   string `json:"trackTitle"`
	Artist       string `json:"artist"`
	Album        string `json:"album"`
	Duration     int64  `json:"duration"`
	Position     int64  `json:"position"`
	Volume       int    `json:"volume"`
	Timestamp    int64  `json:"timestamp"`
	AlbumArtHash string `json:"albumArtHash,omitempty"`
}

// PlaybackCommand represents a playback control command
type PlaybackCommand struct {
	Action    string `json:"action"` // play, pause, next, previous, seek, volume, play_episode, request_podcast_list, request_lyrics, etc.
	Value     int64  `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp"`
	// Podcast-specific fields for play_episode and request_podcast_episodes commands
	PodcastId    string `json:"podcastId,omitempty"`    // Podcast hash (for request_podcast_episodes)
	EpisodeHash  string `json:"episodeHash,omitempty"`  // Episode hash (CRC32 of feedUrl+pubDate+duration) for play_episode
	EpisodeIndex int    `json:"episodeIndex,omitempty"` // DEPRECATED: use EpisodeHash instead
	// Pagination fields for request_podcast_episodes
	Offset int `json:"offset,omitempty"`
	Limit  int `json:"limit,omitempty"`
	// Lyrics-specific fields for request_lyrics command
	Artist string `json:"artist,omitempty"` // Artist name for lyrics lookup
	Track  string `json:"track,omitempty"`  // Track name for lyrics lookup
}

// AlbumArtRequest represents a request for album art data
type AlbumArtRequest struct {
	Hash      string `json:"hash"`
	Timestamp int64  `json:"timestamp"`
}

// PodcastRequest represents a request for podcast information
type PodcastRequest struct {
	Action    string `json:"action"`
	Timestamp int64  `json:"timestamp"`
}

// ============================================================================
// Lyrics Types
// ============================================================================

// LyricsLine represents a single line of lyrics with timestamp
type LyricsLine struct {
	Timestamp int64  `json:"t"` // timestamp in ms (0 if unsynced)
	Text      string `json:"l"` // lyrics text
}

// LyricsData represents the complete lyrics data stored in Redis
type LyricsData struct {
	Hash       string       `json:"hash"`        // CRC32 hash of "artist|track"
	TrackTitle string       `json:"trackTitle"`
	Artist     string       `json:"artist"`
	Synced     bool         `json:"synced"`      // true if has timestamps
	Lines      []LyricsLine `json:"lines"`
	Source     string       `json:"source,omitempty"` // e.g., "lrclib"
}

// CompactLyricsChunk represents a BLE chunk of lyrics (uses short field names)
type CompactLyricsChunk struct {
	Hash       string               `json:"h"` // hash
	Synced     bool                 `json:"s"` // synced (has timestamps)
	TotalLines int                  `json:"n"` // total line count
	ChunkIndex int                  `json:"c"` // chunk index (0-based)
	MaxChunks  int                  `json:"m"` // max chunks (total)
	Lines      []CompactLyricsLine  `json:"l"` // lyrics lines in this chunk
}

// CompactLyricsLine represents a lyrics line in BLE format
type CompactLyricsLine struct {
	Timestamp int64  `json:"t"` // timestamp in ms
	Text      string `json:"l"` // text
}

// PodcastInfoResponse represents the full podcast library response
type PodcastInfoResponse struct {
	Podcasts         []PodcastShowInfo    `json:"podcasts"`
	CurrentlyPlaying *CurrentlyPlayingInfo `json:"currentlyPlaying,omitempty"`
}

// PodcastShowInfo represents a single podcast show with its episodes
type PodcastShowInfo struct {
	ID           string           `json:"id"`
	Title        string           `json:"title"`
	Author       string           `json:"author"`
	Description  string           `json:"description"`
	ImageURL     string           `json:"imageUrl"`
	EpisodeCount int              `json:"episodeCount"`
	Episodes     []PodcastEpisode `json:"episodes"`
}

// CurrentlyPlayingInfo represents the currently playing episode
type CurrentlyPlayingInfo struct {
	PodcastID    string `json:"podcastId"`
	EpisodeTitle string `json:"episodeTitle"`
	EpisodeIndex int    `json:"episodeIndex"`
}

// PodcastEpisode represents a single podcast episode
type PodcastEpisode struct {
	Title       string `json:"title"`
	Duration    int64  `json:"duration"`
	PublishDate string `json:"publishDate"`
	PubDate     int64  `json:"pubDate,omitempty"` // Unix timestamp in milliseconds
}

// ============================================================================
// Compact BLE Response Types (optimized for minimal BLE bandwidth)
// Uses short JSON field names to reduce payload size by ~55%
// ============================================================================

// CompactPodcastListResponse is the BLE response for request_podcast_list
// JSON: {"p":[...],"np":{...}}
type CompactPodcastListResponse struct {
	Podcasts   []CompactPodcast   `json:"p"`
	NowPlaying *CompactNowPlaying `json:"np,omitempty"`
}

// CompactPodcast represents a podcast channel with minimal data
// JSON: {"h":"abc12345","n":"Podcast Name","c":50}
type CompactPodcast struct {
	Hash  string `json:"h"` // Podcast ID hash
	Name  string `json:"n"` // Podcast title
	Count int    `json:"c"` // Episode count
}

// CompactRecentEpisodesResponse is the BLE response for request_recent_episodes
// JSON: {"e":[...],"t":100}
type CompactRecentEpisodesResponse struct {
	Episodes []CompactEpisode `json:"e"`
	Total    int              `json:"t"`
}

// CompactEpisode represents an episode with minimal data (for recent episodes)
// JSON: {"h":"abc12345","p":"def67890","c":"Channel Name","t":"Episode Title","d":3600,"u":1704499200,"i":0}
type CompactEpisode struct {
	Hash        string `json:"h"`           // Episode hash (CRC32 of feedUrl+pubDate+duration)
	PodcastHash string `json:"p,omitempty"` // Podcast hash (for backward compat with C plugin)
	Channel     string `json:"c"`           // Podcast/channel title
	Title       string `json:"t"`           // Episode title
	Duration    int    `json:"d"`           // Duration in SECONDS (not ms)
	PubDate     int64  `json:"u"`           // Unix timestamp in SECONDS for sorting
	Index       int    `json:"i"`           // Episode index (for backward compat with C plugin)
}

// CompactPodcastEpisodesResponse is the BLE response for request_podcast_episodes
// JSON: {"h":"abc","n":"Podcast","t":50,"o":0,"m":true,"e":[...]}
type CompactPodcastEpisodesResponse struct {
	PodcastHash string                 `json:"h"` // Podcast ID hash
	Name        string                 `json:"n"` // Podcast title
	Total       int                    `json:"t"` // Total episodes
	Offset      int                    `json:"o"` // Current offset
	More        bool                   `json:"m"` // Has more episodes
	Episodes    []CompactEpisodeSimple `json:"e"`
}

// CompactEpisodeSimple is an even smaller episode when podcast context is known
// JSON: {"h":"a1b2c3d4","t":"Episode Title","d":3600,"u":1704499200}
type CompactEpisodeSimple struct {
	Hash     string `json:"h"` // Episode hash (CRC32 of feedUrl+pubDate+duration)
	Title    string `json:"t"` // Episode title
	Duration int    `json:"d"` // Duration in SECONDS
	PubDate  int64  `json:"u"` // Unix timestamp in SECONDS for sorting
}

// CompactNowPlaying represents currently playing info
// JSON: {"h":"abc12345","e":"def67890","t":"Episode Title"}
type CompactNowPlaying struct {
	PodcastHash string `json:"h"` // Podcast ID hash
	EpisodeHash string `json:"e"` // Episode hash (CRC32 of feedUrl+pubDate+duration)
	Title       string `json:"t"` // Episode title
}

// ============================================================================
// Full-Format Redis Types (for C plugin compatibility)
// The C plugin expects full field names, so we convert compact->full when storing
// ============================================================================

// RedisPodcastListResponse is stored in Redis for the C plugin to read
// Key: podcast:list
type RedisPodcastListResponse struct {
	Podcasts []RedisPodcastChannel `json:"podcasts"`
}

// RedisPodcastChannel has full field names for C plugin parsing
type RedisPodcastChannel struct {
	ID           string `json:"id"`           // Podcast ID (hash)
	Title        string `json:"title"`        // Podcast name
	Author       string `json:"author"`       // Podcast author (empty if not available)
	EpisodeCount int    `json:"episodeCount"` // Number of episodes
}

// RedisRecentEpisodesResponse is stored in Redis for the C plugin to read
// Key: podcast:recent_episodes
type RedisRecentEpisodesResponse struct {
	Episodes   []RedisRecentEpisode `json:"episodes"`
	TotalCount int                  `json:"totalCount"`
}

// RedisRecentEpisode has full field names for C plugin parsing
type RedisRecentEpisode struct {
	PodcastID    string `json:"podcastId"`    // Podcast hash (for play_podcast_episode - DEPRECATED)
	EpisodeHash  string `json:"episodeHash"`  // Episode hash (CRC32 of feedUrl+pubDate+duration) for play_episode
	PodcastTitle string `json:"podcastTitle"` // Podcast name
	Title        string `json:"title"`        // Episode title
	Duration     int64  `json:"duration"`     // Duration in MILLISECONDS (C plugin expects ms)
	PublishDate  string `json:"publishDate"`  // Human readable date
	PubDate      int64  `json:"pubDate"`      // Unix timestamp ms
	EpisodeIndex int    `json:"episodeIndex"` // Episode index (DEPRECATED - for backwards compat)
}

// RedisPodcastEpisodesResponse is stored in Redis for the C plugin to read
// Key: podcast:episodes:<podcastId>
type RedisPodcastEpisodesResponse struct {
	PodcastID     string              `json:"podcastId"`
	PodcastTitle  string              `json:"podcastTitle"`
	TotalEpisodes int                 `json:"totalEpisodes"`
	Offset        int                 `json:"offset"`
	HasMore       bool                `json:"hasMore"`
	Episodes      []RedisPodcastEpisode `json:"episodes"`
}

// RedisPodcastEpisode has full field names for C plugin parsing
type RedisPodcastEpisode struct {
	EpisodeHash string `json:"episodeHash"` // Episode hash (CRC32 of feedUrl+pubDate+duration) for playback
	Title       string `json:"title"`
	Duration    int64  `json:"duration"`    // Duration in MILLISECONDS
	PublishDate string `json:"publishDate"` // Human readable date
	PubDate     int64  `json:"pubDate"`     // Unix timestamp ms
}

// ============================================================================
// Compact -> Full Format Converters
// ============================================================================

// ToRedisFormat converts compact BLE format to full Redis format
func (c *CompactPodcastListResponse) ToRedisFormat() *RedisPodcastListResponse {
	channels := make([]RedisPodcastChannel, len(c.Podcasts))
	for i, p := range c.Podcasts {
		channels[i] = RedisPodcastChannel{
			ID:           p.Hash,
			Title:        p.Name,
			Author:       "", // Not available in compact format
			EpisodeCount: p.Count,
		}
	}
	return &RedisPodcastListResponse{Podcasts: channels}
}

// ToRedisFormat converts compact BLE format to full Redis format
func (c *CompactRecentEpisodesResponse) ToRedisFormat() *RedisRecentEpisodesResponse {
	episodes := make([]RedisRecentEpisode, len(c.Episodes))
	for i, e := range c.Episodes {
		episodes[i] = RedisRecentEpisode{
			PodcastID:    e.PodcastHash,            // Podcast hash (for backward compat)
			EpisodeHash:  e.Hash,                   // Episode hash for playback
			PodcastTitle: e.Channel,
			Title:        e.Title,
			Duration:     int64(e.Duration) * 1000, // Convert seconds -> ms
			PublishDate:  "",                       // Not available in compact format
			PubDate:      e.PubDate * 1000,         // Convert seconds -> ms
			EpisodeIndex: e.Index,                  // Episode index (for backward compat)
		}
	}
	return &RedisRecentEpisodesResponse{
		Episodes:   episodes,
		TotalCount: c.Total,
	}
}

// ToRedisFormat converts compact BLE format to full Redis format
func (c *CompactPodcastEpisodesResponse) ToRedisFormat() *RedisPodcastEpisodesResponse {
	episodes := make([]RedisPodcastEpisode, len(c.Episodes))
	for i, e := range c.Episodes {
		episodes[i] = RedisPodcastEpisode{
			EpisodeHash: e.Hash,                   // Episode hash for playback
			Title:       e.Title,
			Duration:    int64(e.Duration) * 1000, // Convert seconds -> ms
			PublishDate: "",                       // Not available in compact format
			PubDate:     e.PubDate * 1000,         // Convert seconds -> ms
		}
	}
	return &RedisPodcastEpisodesResponse{
		PodcastID:     c.PodcastHash,
		PodcastTitle:  c.Name,
		TotalEpisodes: c.Total,
		Offset:        c.Offset,
		HasMore:       c.More,
		Episodes:      episodes,
	}
}

// ============================================================================
// Legacy Types (deprecated)
// ============================================================================

// Legacy PodcastInfo for backwards compatibility (deprecated)
type PodcastInfo struct {
	ShowName           string           `json:"showName"`
	EpisodeTitle       string           `json:"episodeTitle"`
	EpisodeDescription string           `json:"episodeDescription"`
	Author             string           `json:"author"`
	EpisodeCount       int              `json:"episodeCount"`
	CurrentIndex       int              `json:"currentIndex"`
	Episodes           []PodcastEpisode `json:"episodes"`
}

// NewStore creates a new Redis store client
func NewStore(cfg config.Config) (*Store, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Address,
		Password:     "",
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		MinIdleConns: 5,
	})

	ctx := context.Background()

	// Test connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Store{
		client: rdb,
		keyMap: cfg.Redis.KeyMap,
		ctx:    ctx,
	}, nil
}

// Close closes the Redis connection
func (s *Store) Close() error {
	return s.client.Close()
}

// StoreMediaState stores the current media state with intelligent logging
func (s *Store) StoreMediaState(state *MediaState) error {
	state.Timestamp = time.Now().Unix()

	// Store individual keys as expected by LVGL component
	pipe := s.client.Pipeline()

	// Set individual media keys
	if trackKey, exists := s.keyMap["trackTitle"]; exists {
		pipe.Set(s.ctx, trackKey, state.TrackTitle, 0)
	}
	if artistKey, exists := s.keyMap["artistName"]; exists {
		pipe.Set(s.ctx, artistKey, state.Artist, 0)
	}
	if albumKey, exists := s.keyMap["albumName"]; exists {
		pipe.Set(s.ctx, albumKey, state.Album, 0)
	}
	if playingKey, exists := s.keyMap["isPlaying"]; exists {
		pipe.Set(s.ctx, playingKey, fmt.Sprintf("%v", state.IsPlaying), 0)
	}
	if durationKey, exists := s.keyMap["durationMs"]; exists {
		// Convert milliseconds to seconds for UI display (MM:SS format)
		durationSeconds := state.Duration / 1000
		// Only update duration if it's a valid positive value to prevent 0 overwrites
		if durationSeconds > 0 {
			pipe.Set(s.ctx, durationKey, fmt.Sprintf("%d", durationSeconds), 0)
		}
	}
	if progressKey, exists := s.keyMap["progressMs"]; exists {
		// Convert milliseconds to seconds for UI display (MM:SS format)
		progressSeconds := state.Position / 1000
		pipe.Set(s.ctx, progressKey, fmt.Sprintf("%d", progressSeconds), 0)
	}

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to store media state: %w", err)
	}

	// Only log successful storage - the BLE handler now controls when this is called
	// so we avoid duplicate "stored" messages for unchanged data
	log.Printf("Stored media state: %s - %s (%v)", state.Artist, state.TrackTitle, state.IsPlaying)
	return nil
}

// GetMediaState retrieves the current media state
func (s *Store) GetMediaState() (*MediaState, error) {
	state := &MediaState{}

	// Get individual keys as expected by LVGL component
	pipe := s.client.Pipeline()

	var trackCmd, artistCmd, albumCmd, playingCmd, durationCmd, progressCmd *redis.StringCmd

	if trackKey, exists := s.keyMap["trackTitle"]; exists {
		trackCmd = pipe.Get(s.ctx, trackKey)
	}
	if artistKey, exists := s.keyMap["artistName"]; exists {
		artistCmd = pipe.Get(s.ctx, artistKey)
	}
	if albumKey, exists := s.keyMap["albumName"]; exists {
		albumCmd = pipe.Get(s.ctx, albumKey)
	}
	if playingKey, exists := s.keyMap["isPlaying"]; exists {
		playingCmd = pipe.Get(s.ctx, playingKey)
	}
	if durationKey, exists := s.keyMap["durationMs"]; exists {
		durationCmd = pipe.Get(s.ctx, durationKey)
	}
	if progressKey, exists := s.keyMap["progressMs"]; exists {
		progressCmd = pipe.Get(s.ctx, progressKey)
	}

	_, err := pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get media state: %w", err)
	}

	// Extract values from commands
	if trackCmd != nil {
		state.TrackTitle, _ = trackCmd.Result()
	}
	if artistCmd != nil {
		state.Artist, _ = artistCmd.Result()
	}
	if albumCmd != nil {
		state.Album, _ = albumCmd.Result()
	}
	if playingCmd != nil {
		playingStr, _ := playingCmd.Result()
		state.IsPlaying = playingStr == "true"
	}
	if durationCmd != nil {
		durationStr, _ := durationCmd.Result()
		var durationSeconds int64
		fmt.Sscanf(durationStr, "%d", &durationSeconds)
		// Convert seconds back to milliseconds for internal MediaState consistency
		state.Duration = durationSeconds * 1000
	}
	if progressCmd != nil {
		progressStr, _ := progressCmd.Result()
		var progressSeconds int64
		fmt.Sscanf(progressStr, "%d", &progressSeconds)
		// Convert seconds back to milliseconds for internal MediaState consistency
		state.Position = progressSeconds * 1000
	}

	return state, nil
}

// QueuePlaybackCommand queues a playback control command
func (s *Store) QueuePlaybackCommand(cmd *PlaybackCommand) error {
	cmd.Timestamp = time.Now().Unix()

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal playback command: %w", err)
	}

	key := s.keyMap["playbackCommandQueue"]
	err = s.client.LPush(s.ctx, key, data).Err()
	if err != nil {
		return fmt.Errorf("failed to queue playback command: %w", err)
	}

	log.Printf("Queued playback command: %s", cmd.Action)
	return nil
}

// DequeuePlaybackCommand dequeues the next playback control command
func (s *Store) DequeuePlaybackCommand() (*PlaybackCommand, error) {
	key := s.keyMap["playbackCommandQueue"]

	data, err := s.client.BRPop(s.ctx, 1*time.Second, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No commands queued
		}
		return nil, fmt.Errorf("failed to dequeue playback command: %w", err)
	}

	if len(data) < 2 {
		return nil, fmt.Errorf("invalid command data received")
	}

	var cmd PlaybackCommand
	err = json.Unmarshal([]byte(data[1]), &cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal playback command: %w", err)
	}

	return &cmd, nil
}

// StoreAlbumArtRequest stores an album art request
func (s *Store) StoreAlbumArtRequest(request *AlbumArtRequest) error {
	request.Timestamp = time.Now().Unix()

	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal album art request: %w", err)
	}

	key := s.keyMap["albumArtRequest"]
	err = s.client.Set(s.ctx, key, data, 5*time.Minute).Err() // Expire after 5 minutes
	if err != nil {
		return fmt.Errorf("failed to store album art request: %w", err)
	}

	log.Printf("Stored album art request for hash: %s", request.Hash)
	return nil
}

// GetAlbumArtRequest retrieves the current album art request
func (s *Store) GetAlbumArtRequest() (*AlbumArtRequest, error) {
	key := s.keyMap["albumArtRequest"]

	data, err := s.client.Get(s.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No request stored
		}
		return nil, fmt.Errorf("failed to get album art request: %w", err)
	}

	var request AlbumArtRequest
	err = json.Unmarshal([]byte(data), &request)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal album art request: %w", err)
	}

	return &request, nil
}

// ClearAlbumArtRequest removes the album art request after processing
func (s *Store) ClearAlbumArtRequest() error {
	key := s.keyMap["albumArtRequest"]
	err := s.client.Del(s.ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to clear album art request: %w", err)
	}
	return nil
}

// StoreAlbumArtCache stores album art data in cache with enhanced validation
func (s *Store) StoreAlbumArtCache(hash string, data []byte) error {
	if len(hash) == 0 {
		return fmt.Errorf("hash cannot be empty")
	}

	if len(data) == 0 {
		return fmt.Errorf("data cannot be empty")
	}

	// Validate hash format (Android CRC32 decimal string, 8-10 digits)
	for _, c := range hash {
		if c < '0' || c > '9' {
			return fmt.Errorf("invalid hash format: expected decimal string, got '%c'", c)
		}
	}
	
	if len(hash) < 8 || len(hash) > 10 {
		return fmt.Errorf("invalid hash format: expected 8-10 decimal digits, got %d", len(hash))
	}

	key := fmt.Sprintf("%s:%s", s.keyMap["albumArtCache"], hash)

	// Use pipeline for atomic operation with metadata
	pipe := s.client.Pipeline()

	// Store the actual data
	pipe.Set(s.ctx, key, data, 24*time.Hour)

	// Store metadata
	metaKey := key + ":meta"
	metadata := map[string]interface{}{
		"size":      len(data),
		"hash":      hash,
		"timestamp": time.Now().Unix(),
		"version":   "v2", // Version for migration tracking
	}

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	pipe.Set(s.ctx, metaKey, metaJSON, 24*time.Hour)

	// Execute pipeline
	_, err = pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to store album art cache: %w", err)
	}

	log.Printf("Cached album art for hash: %s (%d bytes) with metadata", hash, len(data))
	return nil
}

// GetAlbumArtCache retrieves album art data from cache with validation
func (s *Store) GetAlbumArtCache(hash string) ([]byte, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("hash cannot be empty")
	}

	// Validate hash format (Android CRC32 decimal string, 8-10 digits)
	for _, c := range hash {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("invalid hash format: expected decimal string, got '%c'", c)
		}
	}
	
	if len(hash) < 8 || len(hash) > 10 {
		return nil, fmt.Errorf("invalid hash format: expected 8-10 decimal digits, got %d", len(hash))
	}

	key := fmt.Sprintf("%s:%s", s.keyMap["albumArtCache"], hash)
	metaKey := key + ":meta"

	// Use pipeline to get both data and metadata
	pipe := s.client.Pipeline()
	dataCmd := pipe.Get(s.ctx, key)
	metaCmd := pipe.Get(s.ctx, metaKey)

	_, err := pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get album art cache: %w", err)
	}

	// Get data
	data, err := dataCmd.Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Not cached
		}
		return nil, fmt.Errorf("failed to get album art data: %w", err)
	}

	// Get and validate metadata if available
	metaData, err := metaCmd.Result()
	if err != nil && err != redis.Nil {
		log.Printf("Warning: Failed to get album art metadata for hash %s: %v", hash, err)
		// Continue without metadata validation for backward compatibility
	} else if err == nil {
		// Validate metadata
		var metadata map[string]interface{}
		if err := json.Unmarshal([]byte(metaData), &metadata); err != nil {
			log.Printf("Warning: Failed to parse album art metadata for hash %s: %v", hash, err)
		} else {
			// Validate metadata consistency
			if metaHash, ok := metadata["hash"].(string); ok && metaHash != hash {
				log.Printf("Warning: Hash mismatch in metadata for %s: expected %s, got %s",
					hash, hash, metaHash)
				// Could remove invalid cache entry here
			}

			if metaSize, ok := metadata["size"].(float64); ok && int(metaSize) != len(data) {
				log.Printf("Warning: Size mismatch in metadata for %s: expected %d, got %d",
					hash, int(metaSize), len(data))
			}
		}
	}

	return []byte(data), nil
}

// PublishEvent publishes an event to subscribers
func (s *Store) PublishEvent(channel string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	err = s.client.Publish(s.ctx, channel, jsonData).Err()
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

// SubscribeToEvents subscribes to events on specified channels
func (s *Store) SubscribeToEvents(channels ...string) *redis.PubSub {
	return s.client.Subscribe(s.ctx, channels...)
}

// SetBLEConnectionStatus updates the BLE connection status in Redis
func (s *Store) SetBLEConnectionStatus(connected bool, deviceName string) error {
	pipe := s.client.Pipeline()

	// Set BLE connection status
	if bleConnectedKey, exists := s.keyMap["bleConnected"]; exists {
		pipe.Set(s.ctx, bleConnectedKey, fmt.Sprintf("%v", connected), 0)
	}

	// Set BLE device name
	if bleNameKey, exists := s.keyMap["bleName"]; exists {
		if connected && deviceName != "" {
			pipe.Set(s.ctx, bleNameKey, deviceName, 0)
		} else {
			pipe.Set(s.ctx, bleNameKey, "Not Connected", 0)
		}
	}

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to update BLE connection status: %w", err)
	}

	log.Printf("Updated BLE connection status: connected=%v, device=%s", connected, deviceName)
	return nil
}

// GetBLEConnectionStatus retrieves the current BLE connection status
func (s *Store) GetBLEConnectionStatus() (connected bool, deviceName string, err error) {
	pipe := s.client.Pipeline()

	var connectedCmd *redis.StringCmd
	var nameCmd *redis.StringCmd

	if bleConnectedKey, exists := s.keyMap["bleConnected"]; exists {
		connectedCmd = pipe.Get(s.ctx, bleConnectedKey)
	}

	if bleNameKey, exists := s.keyMap["bleName"]; exists {
		nameCmd = pipe.Get(s.ctx, bleNameKey)
	}

	_, err = pipe.Exec(s.ctx)
	if err != nil && err != redis.Nil {
		return false, "", fmt.Errorf("failed to get BLE connection status: %w", err)
	}

	// Parse connection status
	if connectedCmd != nil {
		connectedStr, err := connectedCmd.Result()
		if err == nil {
			connected = connectedStr == "true"
		}
	}

	// Parse device name
	if nameCmd != nil {
		deviceName, _ = nameCmd.Result()
		if deviceName == "" {
			deviceName = "Unknown Device"
		}
	}

	return connected, deviceName, nil
}

// SetAlbumArtFilePath updates the album art file path in Redis
func (s *Store) SetAlbumArtFilePath(filePath string) error {
	if albumArtPathKey, exists := s.keyMap["albumArtPath"]; exists {
		err := s.client.Set(s.ctx, albumArtPathKey, filePath, 0).Err()
		if err != nil {
			return fmt.Errorf("failed to set album art file path: %w", err)
		}
		log.Printf("Updated Redis album art path: %s", filePath)
		return nil
	}
	return fmt.Errorf("albumArtPath key not found in configuration")
}

// ClearAlbumArtFilePath clears the album art file path in Redis
func (s *Store) ClearAlbumArtFilePath() error {
	if albumArtPathKey, exists := s.keyMap["albumArtPath"]; exists {
		err := s.client.Del(s.ctx, albumArtPathKey).Err()
		if err != nil {
			return fmt.Errorf("failed to clear album art file path: %w", err)
		}
		// Note: Logging moved to BLE client to avoid duplicate messages
		// when no actual album art hash change occurred
		return nil
	}
	return fmt.Errorf("albumArtPath key not found in configuration")
}

// GetAlbumArtFilePath retrieves the album art file path from Redis
func (s *Store) GetAlbumArtFilePath() (string, error) {
	if albumArtPathKey, exists := s.keyMap["albumArtPath"]; exists {
		filePath, err := s.client.Get(s.ctx, albumArtPathKey).Result()
		if err != nil {
			if err == redis.Nil {
				return "", nil // No path set
			}
			return "", fmt.Errorf("failed to get album art file path: %w", err)
		}
		return filePath, nil
	}
	return "", fmt.Errorf("albumArtPath key not found in configuration")
}

// CleanupAlbumArtCache removes expired or invalid album art cache entries
func (s *Store) CleanupAlbumArtCache() error {
	cachePattern := s.keyMap["albumArtCache"] + ":*"

	// Get all cache keys
	keys, err := s.client.Keys(s.ctx, cachePattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get cache keys: %w", err)
	}

	cleanedCount := 0
	errorCount := 0

	for _, key := range keys {
		// Skip metadata keys
		if strings.HasSuffix(key, ":meta") {
			continue
		}

		// Check if key still exists and is valid
		ttl, err := s.client.TTL(s.ctx, key).Result()
		if err != nil {
			log.Printf("Warning: Failed to get TTL for key %s: %v", key, err)
			errorCount++
			continue
		}

		// If key is expired or about to expire, clean it up
		if ttl <= 0 || ttl < time.Minute {
			metaKey := key + ":meta"

			// Delete both data and metadata
			pipe := s.client.Pipeline()
			pipe.Del(s.ctx, key)
			pipe.Del(s.ctx, metaKey)

			_, err := pipe.Exec(s.ctx)
			if err != nil {
				log.Printf("Warning: Failed to delete expired cache entry %s: %v", key, err)
				errorCount++
			} else {
				cleanedCount++
			}
		}
	}

	if cleanedCount > 0 || errorCount > 0 {
		log.Printf("Album art cache cleanup completed: %d cleaned, %d errors", cleanedCount, errorCount)
	}

	return nil
}

// GetAlbumArtCacheStats returns statistics about album art cache usage
func (s *Store) GetAlbumArtCacheStats() (map[string]interface{}, error) {
	cachePattern := s.keyMap["albumArtCache"] + ":*"

	keys, err := s.client.Keys(s.ctx, cachePattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cache keys: %w", err)
	}

	dataKeys := 0
	metaKeys := 0
	totalSize := int64(0)

	for _, key := range keys {
		if strings.HasSuffix(key, ":meta") {
			metaKeys++
		} else {
			dataKeys++

			// Get size
			size, err := s.client.StrLen(s.ctx, key).Result()
			if err != nil {
				log.Printf("Warning: Failed to get size for key %s: %v", key, err)
			} else {
				totalSize += size
			}
		}
	}

	stats := map[string]interface{}{
		"dataEntries":    dataKeys,
		"metaEntries":    metaKeys,
		"totalSizeBytes": totalSize,
		"totalSizeMB":    float64(totalSize) / (1024 * 1024),
	}

	return stats, nil
}

// ValidateAlbumArtCacheIntegrity validates the integrity of cached album art
func (s *Store) ValidateAlbumArtCacheIntegrity() error {
	cachePattern := s.keyMap["albumArtCache"] + ":*"

	keys, err := s.client.Keys(s.ctx, cachePattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get cache keys: %w", err)
	}

	validCount := 0
	invalidCount := 0

	for _, key := range keys {
		if strings.HasSuffix(key, ":meta") {
			continue
		}

		// Extract hash from key
		parts := strings.Split(key, ":")
		if len(parts) < 2 {
			log.Printf("Warning: Invalid cache key format: %s", key)
			invalidCount++
			continue
		}
		hash := parts[len(parts)-1]

		// Validate hash format (Android CRC32 decimal string, 8-10 digits)
		isValidHash := true
		if len(hash) < 8 || len(hash) > 10 {
			isValidHash = false
		} else {
			for _, c := range hash {
				if c < '0' || c > '9' {
					isValidHash = false
					break
				}
			}
		}
		
		if !isValidHash {
			log.Printf("Warning: Invalid hash format in key %s: %s (expected 8-10 decimal digits)", key, hash)
			invalidCount++

			// Remove invalid entry
			metaKey := key + ":meta"
			pipe := s.client.Pipeline()
			pipe.Del(s.ctx, key)
			pipe.Del(s.ctx, metaKey)
			pipe.Exec(s.ctx)
			continue
		}

		validCount++
	}

	log.Printf("Album art cache integrity check: %d valid, %d invalid (removed)", validCount, invalidCount)

	if invalidCount > 0 {
		return fmt.Errorf("found and removed %d invalid cache entries", invalidCount)
	}

	return nil
}

// SetKey sets a specific key-value pair in Redis
func (s *Store) SetKey(key, value string) error {
	err := s.client.Set(s.ctx, key, value, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set Redis key %s: %w", key, err)
	}
	return nil
}

// UpdatePositionOnly updates only the position/progress in Redis while preserving duration
// This is critical for TimeTracker to avoid resetting duration to 0 during position updates
func (s *Store) UpdatePositionOnly(positionSeconds int64) error {
	// Only update the progress key, don't touch duration
	if progressKey, exists := s.keyMap["progressMs"]; exists {
		err := s.client.Set(s.ctx, progressKey, fmt.Sprintf("%d", positionSeconds), 0).Err()
		if err != nil {
			return fmt.Errorf("failed to update position in Redis: %w", err)
		}
		return nil
	}
	return fmt.Errorf("progressMs key not found in configuration")
}

// Ping tests the Redis connection
func (s *Store) Ping() error {
	return s.client.Ping(s.ctx).Err()
}

// GetAllCachedAlbumArtHashes returns all album art hashes in Redis cache
func (s *Store) GetAllCachedAlbumArtHashes() ([]string, error) {
	cachePattern := s.keyMap["albumArtCache"] + ":*"

	keys, err := s.client.Keys(s.ctx, cachePattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cache keys: %w", err)
	}

	hashes := make([]string, 0, len(keys))
	cachePrefix := s.keyMap["albumArtCache"] + ":"

	for _, key := range keys {
		// Skip metadata keys
		if strings.HasSuffix(key, ":meta") {
			continue
		}

		// Extract hash from key
		if strings.HasPrefix(key, cachePrefix) {
			hash := strings.TrimPrefix(key, cachePrefix)
			hashes = append(hashes, hash)
		}
	}

	return hashes, nil
}

// DeleteAlbumArtCache removes a specific album art cache entry from Redis
func (s *Store) DeleteAlbumArtCache(hash string) error {
	key := fmt.Sprintf("%s:%s", s.keyMap["albumArtCache"], hash)
	metaKey := key + ":meta"

	pipe := s.client.Pipeline()
	pipe.Del(s.ctx, key)
	pipe.Del(s.ctx, metaKey)

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete album art cache for hash %s: %w", hash, err)
	}

	log.Printf("Deleted album art cache entry for hash: %s", hash)
	return nil
}

// BLEStatus represents the BLE client status information
type BLEStatus struct {
	Connected          bool
	DeviceName         string
	DeviceAddress      string
	LastUpdateMs       int64
	CommandsProcessed  int
	CommandsFailed     int
	ConnectionQuality  string
	Scanning           bool
	RSSI               int
}

// PublishBLEStatus publishes BLE client status to Redis
// Keys are prefixed with "ble:status:" to match C client expectations
func (s *Store) PublishBLEStatus(status *BLEStatus) error {
	pipe := s.client.Pipeline()

	// Set all BLE status keys with "ble:status:" prefix
	pipe.Set(s.ctx, "ble:status:connected", fmt.Sprintf("%v", status.Connected), 0)
	pipe.Set(s.ctx, "ble:status:device_name", status.DeviceName, 0)
	pipe.Set(s.ctx, "ble:status:device_address", status.DeviceAddress, 0)
	pipe.Set(s.ctx, "ble:status:last_update", fmt.Sprintf("%d", status.LastUpdateMs), 0)
	pipe.Set(s.ctx, "ble:status:commands_processed", fmt.Sprintf("%d", status.CommandsProcessed), 0)
	pipe.Set(s.ctx, "ble:status:commands_failed", fmt.Sprintf("%d", status.CommandsFailed), 0)
	pipe.Set(s.ctx, "ble:status:connection_quality", status.ConnectionQuality, 0)
	pipe.Set(s.ctx, "ble:status:scanning", fmt.Sprintf("%v", status.Scanning), 0)
	pipe.Set(s.ctx, "ble:status:rssi", fmt.Sprintf("%d", status.RSSI), 0)

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to publish BLE status: %w", err)
	}

	return nil
}

// BLE Reconnect Request Key - set by UI to request BLE reconnection
const BLEReconnectRequestKey = "system:ble_reconnect_request"

// GetReconnectRequest checks for a BLE reconnect request from the UI
// Returns the timestamp of the request, or 0 if no request is pending
func (s *Store) GetReconnectRequest() (int64, error) {
	result, err := s.client.Get(s.ctx, BLEReconnectRequestKey).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil // No request pending
		}
		return 0, fmt.Errorf("failed to get reconnect request: %w", err)
	}

	var timestamp int64
	_, err = fmt.Sscanf(result, "%d", &timestamp)
	if err != nil {
		return 0, fmt.Errorf("failed to parse reconnect request timestamp: %w", err)
	}

	return timestamp, nil
}

// ClearReconnectRequest removes the reconnect request after processing
func (s *Store) ClearReconnectRequest() error {
	err := s.client.Del(s.ctx, BLEReconnectRequestKey).Err()
	if err != nil {
		return fmt.Errorf("failed to clear reconnect request: %w", err)
	}
	return nil
}

// DequeuePodcastRequest dequeues the next podcast info request command
func (s *Store) DequeuePodcastRequest() (*PodcastRequest, error) {
	key := s.keyMap["podcastRequestQueue"]

	data, err := s.client.BRPop(s.ctx, 1*time.Second, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No requests queued
		}
		return nil, fmt.Errorf("failed to dequeue podcast request: %w", err)
	}

	if len(data) < 2 {
		return nil, fmt.Errorf("invalid podcast request data received")
	}

	var req PodcastRequest
	err = json.Unmarshal([]byte(data[1]), &req)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal podcast request: %w", err)
	}

	return &req, nil
}

// StorePodcastInfo stores podcast show and episode information in Redis
func (s *Store) StorePodcastInfo(info *PodcastInfo) error {
	// Store individual podcast keys
	pipe := s.client.Pipeline()

	if showNameKey, exists := s.keyMap["podcastShowName"]; exists {
		pipe.Set(s.ctx, showNameKey, info.ShowName, 0)
	}
	if episodeTitleKey, exists := s.keyMap["podcastEpisodeTitle"]; exists {
		pipe.Set(s.ctx, episodeTitleKey, info.EpisodeTitle, 0)
	}
	if episodeDescKey, exists := s.keyMap["podcastEpisodeDescription"]; exists {
		pipe.Set(s.ctx, episodeDescKey, info.EpisodeDescription, 0)
	}
	if authorKey, exists := s.keyMap["podcastAuthor"]; exists {
		pipe.Set(s.ctx, authorKey, info.Author, 0)
	}
	if episodeCountKey, exists := s.keyMap["podcastEpisodeCount"]; exists {
		pipe.Set(s.ctx, episodeCountKey, fmt.Sprintf("%d", info.EpisodeCount), 0)
	}
	if currentIndexKey, exists := s.keyMap["podcastCurrentIndex"]; exists {
		pipe.Set(s.ctx, currentIndexKey, fmt.Sprintf("%d", info.CurrentIndex), 0)
	}

	// Store episode list as JSON array
	if episodeListKey, exists := s.keyMap["podcastEpisodeList"]; exists {
		episodesJSON, err := json.Marshal(info.Episodes)
		if err != nil {
			return fmt.Errorf("failed to marshal podcast episodes: %w", err)
		}
		pipe.Set(s.ctx, episodeListKey, episodesJSON, 0)
	}

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to store podcast info: %w", err)
	}

	log.Printf("Stored podcast info: %s - %s (%d episodes)", info.ShowName, info.EpisodeTitle, info.EpisodeCount)
	return nil
}

// StorePodcastLibrary stores the full podcast library (multiple podcasts)
func (s *Store) StorePodcastLibrary(info *PodcastInfoResponse) error {
	pipe := s.client.Pipeline()

	// Store the full podcast library as JSON
	libraryJSON, err := json.Marshal(info.Podcasts)
	if err != nil {
		return fmt.Errorf("failed to marshal podcast library: %w", err)
	}
	pipe.Set(s.ctx, "podcast:library", libraryJSON, 0)

	// Store podcast count
	pipe.Set(s.ctx, "podcast:count", fmt.Sprintf("%d", len(info.Podcasts)), 0)

	// Store currently playing info if available
	if info.CurrentlyPlaying != nil {
		pipe.Set(s.ctx, "podcast:currently_playing_id", info.CurrentlyPlaying.PodcastID, 0)
		pipe.Set(s.ctx, "podcast:currently_playing_episode", info.CurrentlyPlaying.EpisodeTitle, 0)
		pipe.Set(s.ctx, "podcast:currently_playing_index", fmt.Sprintf("%d", info.CurrentlyPlaying.EpisodeIndex), 0)
	}

	// Also store individual podcast data for each podcast
	for i, podcast := range info.Podcasts {
		prefix := fmt.Sprintf("podcast:%d:", i)
		pipe.Set(s.ctx, prefix+"id", podcast.ID, 0)
		pipe.Set(s.ctx, prefix+"title", podcast.Title, 0)
		pipe.Set(s.ctx, prefix+"author", podcast.Author, 0)
		pipe.Set(s.ctx, prefix+"description", podcast.Description, 0)
		pipe.Set(s.ctx, prefix+"image_url", podcast.ImageURL, 0)
		pipe.Set(s.ctx, prefix+"episode_count", fmt.Sprintf("%d", podcast.EpisodeCount), 0)

		// Store episodes for this podcast
		episodesJSON, err := json.Marshal(podcast.Episodes)
		if err != nil {
			log.Printf("Failed to marshal episodes for podcast %s: %v", podcast.Title, err)
			continue
		}
		pipe.Set(s.ctx, prefix+"episodes", episodesJSON, 0)
	}

	_, err = pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to store podcast library: %w", err)
	}

	log.Printf("Stored podcast library: %d podcasts", len(info.Podcasts))
	return nil
}

// ============================================================================
// New Lazy Loading Storage Functions
// ============================================================================

// StorePodcastList stores the podcast channel list (A-Z) for lazy loading
func (s *Store) StorePodcastList(response *CompactPodcastListResponse) error {
	// Convert compact BLE format to full Redis format for C plugin compatibility
	redisResponse := response.ToRedisFormat()
	jsonData, err := json.Marshal(redisResponse)
	if err != nil {
		return fmt.Errorf("failed to marshal podcast list: %w", err)
	}

	// Store in Redis key that SDK's LlzMediaGetPodcastList reads
	if err := s.client.Set(s.ctx, "podcast:list", jsonData, 0).Err(); err != nil {
		return fmt.Errorf("failed to store podcast list: %w", err)
	}

	// Also update channel count
	if err := s.client.Set(s.ctx, "podcast:channel_count", fmt.Sprintf("%d", len(response.Podcasts)), 0).Err(); err != nil {
		log.Printf("Warning: failed to update podcast channel count: %v", err)
	}

	log.Printf("Stored podcast list: %d channels (%d bytes, full format)", len(response.Podcasts), len(jsonData))
	return nil
}

// StoreRecentEpisodes stores recent episodes across all podcasts
func (s *Store) StoreRecentEpisodes(response *CompactRecentEpisodesResponse) error {
	// Convert compact BLE format to full Redis format for C plugin compatibility
	redisResponse := response.ToRedisFormat()
	jsonData, err := json.Marshal(redisResponse)
	if err != nil {
		return fmt.Errorf("failed to marshal recent episodes: %w", err)
	}

	// Store in Redis key that SDK's LlzMediaGetRecentEpisodes reads
	// NOTE: C plugin expects "podcast:recent_episodes" not "podcast:recent"
	if err := s.client.Set(s.ctx, "podcast:recent_episodes", jsonData, 0).Err(); err != nil {
		return fmt.Errorf("failed to store recent episodes: %w", err)
	}

	log.Printf("Stored recent episodes: %d episodes (%d bytes, full format)", len(response.Episodes), len(jsonData))
	return nil
}

// StorePodcastEpisodes stores paginated episodes for a specific podcast
func (s *Store) StorePodcastEpisodes(response *CompactPodcastEpisodesResponse) error {
	// Convert compact BLE format to full Redis format for C plugin compatibility
	redisResponse := response.ToRedisFormat()
	jsonData, err := json.Marshal(redisResponse)
	if err != nil {
		return fmt.Errorf("failed to marshal podcast episodes: %w", err)
	}

	// Store in Redis key that SDK's LlzMediaGetPodcastEpisodesForId reads
	key := fmt.Sprintf("podcast:episodes:%s", response.PodcastHash)
	if err := s.client.Set(s.ctx, key, jsonData, 0).Err(); err != nil {
		return fmt.Errorf("failed to store podcast episodes: %w", err)
	}

	log.Printf("Stored episodes for '%s': %d episodes at offset %d (%d bytes)",
		response.Name, len(response.Episodes), response.Offset, len(jsonData))
	return nil
}

// ============================================================================
// Lyrics Storage Functions
// ============================================================================

// StoreLyricsChunk stores lyrics data in Redis from a BLE chunk
// Accumulates chunks until the complete lyrics are received
func (s *Store) StoreLyricsChunk(chunk *CompactLyricsChunk) error {
	debug.LogLyrics("StoreLyricsChunk called: hash=%s, chunk=%d/%d, lines=%d, synced=%v",
		chunk.Hash, chunk.ChunkIndex+1, chunk.MaxChunks, len(chunk.Lines), chunk.Synced)

	if chunk.Hash == "" {
		debug.LogLyrics("ERROR: Empty hash provided to StoreLyricsChunk")
		return fmt.Errorf("lyrics hash cannot be empty")
	}

	// If this is the first chunk (index 0), start fresh
	if chunk.ChunkIndex == 0 {
		debug.LogLyrics("First chunk received - clearing any existing partial lyrics for hash %s", chunk.Hash)
		// Clear any existing partial lyrics for this hash
		s.client.Del(s.ctx, fmt.Sprintf("lyrics:chunks:%s", chunk.Hash))
	}

	// Store this chunk in a temporary list
	chunkKey := fmt.Sprintf("lyrics:chunks:%s", chunk.Hash)
	chunkData, err := json.Marshal(chunk)
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to marshal chunk for Redis storage: %v", err)
		return fmt.Errorf("failed to marshal lyrics chunk: %w", err)
	}

	debug.LogLyrics("Storing chunk to Redis key: %s (size: %d bytes)", chunkKey, len(chunkData))
	debug.LogLyricsVerbose("Chunk JSON: %s", string(chunkData))
	s.client.RPush(s.ctx, chunkKey, chunkData)

	// If this is the last chunk, assemble complete lyrics
	if chunk.ChunkIndex == chunk.MaxChunks-1 {
		debug.LogLyrics("Last chunk received - assembling complete lyrics for hash %s", chunk.Hash)
		return s.assembleLyrics(chunk.Hash, chunk.Synced, chunk.TotalLines)
	}

	debug.LogLyrics("Chunk %d/%d stored successfully for hash %s",
		chunk.ChunkIndex+1, chunk.MaxChunks, chunk.Hash)
	return nil
}

// assembleLyrics assembles lyrics from chunks into the final format
func (s *Store) assembleLyrics(hash string, synced bool, totalLines int) error {
	debug.LogLyrics("assembleLyrics: Starting assembly for hash=%s, synced=%v, expectedLines=%d",
		hash, synced, totalLines)

	chunkKey := fmt.Sprintf("lyrics:chunks:%s", hash)

	// Get all chunks
	debug.LogLyrics("Fetching all chunks from Redis key: %s", chunkKey)
	chunksData, err := s.client.LRange(s.ctx, chunkKey, 0, -1).Result()
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to get lyrics chunks from Redis: %v", err)
		return fmt.Errorf("failed to get lyrics chunks: %w", err)
	}

	debug.LogLyrics("Retrieved %d chunk(s) from Redis", len(chunksData))

	// Assemble all lines from chunks
	allLines := make([]LyricsLine, 0, totalLines)
	for i, chunkData := range chunksData {
		var chunk CompactLyricsChunk
		if err := json.Unmarshal([]byte(chunkData), &chunk); err != nil {
			debug.LogLyrics("WARNING: Failed to unmarshal chunk %d: %v", i, err)
			continue
		}
		debug.LogLyrics("Processing chunk %d: %d lines", i, len(chunk.Lines))
		for _, line := range chunk.Lines {
			allLines = append(allLines, LyricsLine{
				Timestamp: line.Timestamp,
				Text:      line.Text,
			})
		}
	}

	debug.LogLyrics("Assembly complete: %d total lines (expected: %d)", len(allLines), totalLines)

	// Log sample of lyrics for verification (verbose mode shows all lines)
	if len(allLines) > 0 {
		debug.LogLyrics("First line: timestamp=%dms, text='%s'", allLines[0].Timestamp, allLines[0].Text)
		if len(allLines) > 1 {
			lastIdx := len(allLines) - 1
			debug.LogLyrics("Last line: timestamp=%dms, text='%s'", allLines[lastIdx].Timestamp, allLines[lastIdx].Text)
		}
		// In verbose mode, log all lines
		for i, line := range allLines {
			debug.LogLyricsVerbose("Line %d: [%dms] %s", i, line.Timestamp, line.Text)
		}
	}

	// Create complete lyrics data
	lyrics := &LyricsData{
		Hash:   hash,
		Synced: synced,
		Lines:  allLines,
	}

	// Store complete lyrics
	debug.LogLyrics("Storing complete lyrics in Redis...")
	if err := s.StoreLyrics(lyrics); err != nil {
		log.Printf("[LYRICS] ERROR: Failed to store complete lyrics: %v", err)
		return err
	}

	// Clean up temporary chunks
	debug.LogLyrics("Cleaning up temporary chunks key: %s", chunkKey)
	s.client.Del(s.ctx, chunkKey)

	debug.LogLyrics("Successfully assembled and stored lyrics: hash=%s, synced=%v, lines=%d", hash, synced, len(allLines))
	return nil
}

// StoreLyrics stores complete lyrics data in Redis
func (s *Store) StoreLyrics(lyrics *LyricsData) error {
	debug.LogLyrics("StoreLyrics: Storing lyrics for hash=%s, synced=%v, lines=%d",
		lyrics.Hash, lyrics.Synced, len(lyrics.Lines))

	pipe := s.client.Pipeline()

	// Store complete lyrics as JSON
	lyricsJSON, err := json.Marshal(lyrics)
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to marshal lyrics to JSON: %v", err)
		return fmt.Errorf("failed to marshal lyrics: %w", err)
	}

	debug.LogLyrics("Lyrics JSON size: %d bytes", len(lyricsJSON))
	debug.LogLyricsVerbose("Full lyrics JSON: %s", string(lyricsJSON))

	// Use lyrics:data key from config or default
	lyricsKey := "lyrics:data"
	if key, exists := s.keyMap["lyricsData"]; exists {
		lyricsKey = key
	}
	debug.LogLyrics("Storing lyrics data to Redis key: %s", lyricsKey)
	pipe.Set(s.ctx, lyricsKey, lyricsJSON, 0)

	// Store current hash for quick lookup
	hashKey := "lyrics:hash"
	if key, exists := s.keyMap["lyricsHash"]; exists {
		hashKey = key
	}
	debug.LogLyrics("Storing lyrics hash to Redis key: %s = %s", hashKey, lyrics.Hash)
	pipe.Set(s.ctx, hashKey, lyrics.Hash, 0)

	// Store synced flag
	syncedKey := "lyrics:synced"
	if key, exists := s.keyMap["lyricsSynced"]; exists {
		syncedKey = key
	}
	debug.LogLyrics("Storing lyrics synced flag to Redis key: %s = %v", syncedKey, lyrics.Synced)
	pipe.Set(s.ctx, syncedKey, fmt.Sprintf("%v", lyrics.Synced), 0)

	debug.LogLyrics("Executing Redis pipeline...")
	_, err = pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("[LYRICS] ERROR: Redis pipeline execution failed: %v", err)
		return fmt.Errorf("failed to store lyrics: %w", err)
	}

	debug.LogLyrics("Successfully stored lyrics: hash=%s, synced=%v, lines=%d", lyrics.Hash, lyrics.Synced, len(lyrics.Lines))
	return nil
}

// GetLyrics retrieves the current lyrics from Redis
func (s *Store) GetLyrics() (*LyricsData, error) {
	debug.LogLyrics("GetLyrics: Retrieving current lyrics from Redis")

	lyricsKey := "lyrics:data"
	if key, exists := s.keyMap["lyricsData"]; exists {
		lyricsKey = key
	}

	debug.LogLyrics("Reading from Redis key: %s", lyricsKey)
	data, err := s.client.Get(s.ctx, lyricsKey).Result()
	if err != nil {
		if err == redis.Nil {
			debug.LogLyrics("No lyrics currently stored in Redis")
			return nil, nil // No lyrics stored
		}
		log.Printf("[LYRICS] ERROR: Failed to get lyrics from Redis: %v", err)
		return nil, fmt.Errorf("failed to get lyrics: %w", err)
	}

	debug.LogLyrics("Retrieved lyrics data: %d bytes", len(data))
	debug.LogLyricsVerbose("Raw lyrics data: %s", data)

	var lyrics LyricsData
	if err := json.Unmarshal([]byte(data), &lyrics); err != nil {
		log.Printf("[LYRICS] ERROR: Failed to unmarshal lyrics JSON: %v", err)
		return nil, fmt.Errorf("failed to unmarshal lyrics: %w", err)
	}

	debug.LogLyrics("Successfully retrieved lyrics: hash=%s, synced=%v, lines=%d",
		lyrics.Hash, lyrics.Synced, len(lyrics.Lines))
	return &lyrics, nil
}

// ClearLyrics removes the current lyrics from Redis
func (s *Store) ClearLyrics() error {
	debug.LogLyrics("ClearLyrics: Removing all lyrics data from Redis")

	pipe := s.client.Pipeline()

	lyricsKey := "lyrics:data"
	if key, exists := s.keyMap["lyricsData"]; exists {
		lyricsKey = key
	}
	debug.LogLyrics("Deleting Redis key: %s", lyricsKey)
	pipe.Del(s.ctx, lyricsKey)

	hashKey := "lyrics:hash"
	if key, exists := s.keyMap["lyricsHash"]; exists {
		hashKey = key
	}
	debug.LogLyrics("Deleting Redis key: %s", hashKey)
	pipe.Del(s.ctx, hashKey)

	syncedKey := "lyrics:synced"
	if key, exists := s.keyMap["lyricsSynced"]; exists {
		syncedKey = key
	}
	debug.LogLyrics("Deleting Redis key: %s", syncedKey)
	pipe.Del(s.ctx, syncedKey)

	debug.LogLyrics("Executing Redis delete pipeline...")
	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to clear lyrics from Redis: %v", err)
		return fmt.Errorf("failed to clear lyrics: %w", err)
	}

	debug.LogLyrics("Successfully cleared all lyrics data from Redis")
	return nil
}

// SetLyricsEnabled stores the lyrics enabled setting in Redis
func (s *Store) SetLyricsEnabled(enabled bool) error {
	debug.LogLyrics("SetLyricsEnabled: Setting lyrics feature to %v", enabled)

	enabledKey := "lyrics:enabled"
	if key, exists := s.keyMap["lyricsEnabled"]; exists {
		enabledKey = key
	}

	value := "false"
	if enabled {
		value = "true"
	}

	debug.LogLyrics("Writing to Redis key: %s = %s", enabledKey, value)
	err := s.client.Set(s.ctx, enabledKey, value, 0).Err()
	if err != nil {
		log.Printf("[LYRICS] ERROR: Failed to set lyrics enabled in Redis: %v", err)
		return fmt.Errorf("failed to set lyrics enabled: %w", err)
	}

	debug.LogLyrics("Successfully set lyrics enabled to: %v", enabled)
	return nil
}

// GetLyricsEnabled retrieves the lyrics enabled setting from Redis
func (s *Store) GetLyricsEnabled() (bool, error) {
	debug.LogLyrics("GetLyricsEnabled: Checking if lyrics feature is enabled")

	enabledKey := "lyrics:enabled"
	if key, exists := s.keyMap["lyricsEnabled"]; exists {
		enabledKey = key
	}

	debug.LogLyrics("Reading from Redis key: %s", enabledKey)
	result, err := s.client.Get(s.ctx, enabledKey).Result()
	if err != nil {
		if err == redis.Nil {
			debug.LogLyrics("Key not found in Redis - defaulting to disabled")
			return false, nil // Default to disabled
		}
		log.Printf("[LYRICS] ERROR: Failed to get lyrics enabled from Redis: %v", err)
		return false, fmt.Errorf("failed to get lyrics enabled: %w", err)
	}

	enabled := result == "true" || result == "1"
	debug.LogLyrics("Lyrics feature enabled: %v (raw value: '%s')", enabled, result)
	return enabled, nil
}

// ============================================================================
// Media Channels Storage Functions
// ============================================================================

// MediaChannelsResponse represents the list of media channel apps
type MediaChannelsResponse struct {
	Channels  []string `json:"channels"`  // List of channel names (e.g., "Spotify", "YouTube Music")
	Count     int      `json:"count"`     // Total channel count
	Timestamp int64    `json:"timestamp"` // When the list was fetched
}

// StoreMediaChannels stores the list of media channel apps in Redis
func (s *Store) StoreMediaChannels(channels []string) error {
	response := MediaChannelsResponse{
		Channels:  channels,
		Count:     len(channels),
		Timestamp: time.Now().Unix(),
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal media channels: %w", err)
	}

	// Store in Redis key that SDK's LlzMediaGetChannels reads
	if err := s.client.Set(s.ctx, "media:channels", jsonData, 0).Err(); err != nil {
		return fmt.Errorf("failed to store media channels: %w", err)
	}

	log.Printf("[MEDIA_CHANNELS] Stored %d channels in Redis (%d bytes)", len(channels), len(jsonData))
	return nil
}

// GetMediaChannels retrieves the list of media channel apps from Redis
func (s *Store) GetMediaChannels() (*MediaChannelsResponse, error) {
	result, err := s.client.Get(s.ctx, "media:channels").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No channels stored yet
		}
		return nil, fmt.Errorf("failed to get media channels: %w", err)
	}

	var response MediaChannelsResponse
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		return nil, fmt.Errorf("failed to parse media channels: %w", err)
	}

	return &response, nil
}
