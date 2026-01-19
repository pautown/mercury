package albumart

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Binary protocol constants
const (
	// BinaryHeaderSize is the fixed header size for binary chunks (16 bytes)
	BinaryHeaderSize = 16
	// MaxChunkDataSize is the maximum raw image data per chunk (512 - 16 = 496)
	MaxChunkDataSize = 496
	// MaxBLENotification is the maximum BLE notification size
	MaxBLENotification = 512
)

// Legacy constants for backward compatibility (JSON protocol)
const (
	// ChunkSize is the legacy chunk size for JSON protocol (deprecated, use MaxChunkDataSize)
	ChunkSize = 300
)

// AlbumArtChunk represents a chunk in the legacy JSON/base64 format (deprecated)
// Use AlbumArtChunkBinary for new code
type AlbumArtChunk struct {
	Hash        string `json:"hash"`        // CRC32 hash as decimal string
	ChunkIndex  uint16 `json:"chunkIndex"`  // 0-based index
	TotalChunks uint16 `json:"totalChunks"` // Total number of chunks
	Data        string `json:"data"`        // Base64-encoded chunk data
	CRC32       uint32 `json:"crc32"`       // CRC32 of raw (not base64) data
}

// GetDecodedData decodes the base64 data (legacy JSON format)
func (c *AlbumArtChunk) GetDecodedData() ([]byte, error) {
	return base64.StdEncoding.DecodeString(c.Data)
}

// AlbumArtChunkBinary represents a chunk of album art data in binary format
// Binary format (16-byte header + raw data):
//
//	Offset  Size   Type     Field
//	------  ----   ----     -----
//	0       4      uint32   hash (CRC32 as uint32, little-endian)
//	4       2      uint16   chunkIndex (little-endian)
//	6       2      uint16   totalChunks (little-endian)
//	8       2      uint16   dataLength (little-endian)
//	10      4      uint32   dataCRC32 (little-endian)
//	14      2      uint16   reserved (0)
//	16+     N      bytes    raw image data (max 496 bytes)
type AlbumArtChunkBinary struct {
	Hash        uint32 // CRC32 of artist|album
	ChunkIndex  uint16 // 0-based index
	TotalChunks uint16 // Total number of chunks
	DataLength  uint16 // Length of data in this chunk
	DataCRC32   uint32 // CRC32 of chunk data
	Reserved    uint16 // Reserved for future use
	Data        []byte // Raw image data
}

// ParseBinaryChunk parses a binary chunk from raw BLE data
func ParseBinaryChunk(data []byte) (*AlbumArtChunkBinary, error) {
	if len(data) < BinaryHeaderSize {
		return nil, fmt.Errorf("data too short: %d bytes (minimum %d)", len(data), BinaryHeaderSize)
	}

	chunk := &AlbumArtChunkBinary{}

	// Parse header (little-endian)
	chunk.Hash = binary.LittleEndian.Uint32(data[0:4])
	chunk.ChunkIndex = binary.LittleEndian.Uint16(data[4:6])
	chunk.TotalChunks = binary.LittleEndian.Uint16(data[6:8])
	chunk.DataLength = binary.LittleEndian.Uint16(data[8:10])
	chunk.DataCRC32 = binary.LittleEndian.Uint32(data[10:14])
	chunk.Reserved = binary.LittleEndian.Uint16(data[14:16])

	// Validate data length
	expectedLen := BinaryHeaderSize + int(chunk.DataLength)
	if len(data) < expectedLen {
		return nil, fmt.Errorf("data too short for declared length: have %d, need %d", len(data), expectedLen)
	}

	// Extract data
	chunk.Data = make([]byte, chunk.DataLength)
	copy(chunk.Data, data[BinaryHeaderSize:expectedLen])

	// Validate CRC32
	actualCRC := crc32.ChecksumIEEE(chunk.Data)
	if actualCRC != chunk.DataCRC32 {
		return nil, fmt.Errorf("CRC32 mismatch: expected %d, got %d", chunk.DataCRC32, actualCRC)
	}

	return chunk, nil
}

// HashString returns the hash as a decimal string (for compatibility with existing code)
func (c *AlbumArtChunkBinary) HashString() string {
	return fmt.Sprintf("%d", c.Hash)
}

// ToInternal converts binary chunk to internal format for processing
func (c *AlbumArtChunkBinary) ToInternal() *AlbumArtChunkInternal {
	return &AlbumArtChunkInternal{
		Hash:        c.HashString(),
		ChunkIndex:  c.ChunkIndex,
		TotalChunks: c.TotalChunks,
		Data:        c.Data,
		CRC32:       c.DataCRC32,
	}
}

// AlbumArtChunkInternal represents decoded chunk data for internal processing
type AlbumArtChunkInternal struct {
	Hash        string
	ChunkIndex  uint16
	TotalChunks uint16
	Data        []byte // Raw bytes
	CRC32       uint32
}

// TransferState represents the state of an album art transfer
type TransferState struct {
	Hash         string
	TotalChunks  uint16
	ReceivedMask []bool
	Data         [][]byte
	StartTime    time.Time
	LastUpdate   time.Time
	Complete     bool
}

// Handler manages album art transfers and caching
type Handler struct {
	cacheDir   string
	transfers  map[string]*TransferState
	mutex      sync.RWMutex
	onComplete func(hash string, data []byte) error // Returns error for cache coherence tracking
	validator  *Validator
	retryMgr   *RetryManager
	migration  *CacheMigration
}

// NewHandler creates a new album art handler with comprehensive validation and retry logic
// The onComplete callback is called synchronously to ensure cache coherence between disk and Redis
func NewHandler(cacheDir string, onComplete func(string, []byte) error) (*Handler, error) {
	// Create cache directory if it doesn't exist
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Initialize validator with secure defaults
	validator := NewValidator(DefaultValidationConfig())

	// Initialize cache migration
	migration := NewCacheMigration(cacheDir)

	handler := &Handler{
		cacheDir:   cacheDir,
		transfers:  make(map[string]*TransferState),
		onComplete: onComplete,
		validator:  validator,
		migration:  migration,
	}

	// Initialize retry manager (will be set by SetRequestFunc)
	// This allows the handler to request retransmission

	// Perform cache migration on startup
	if err := migration.MigrateCache(); err != nil {
		log.Printf("Warning: Cache migration failed: %v", err)
	}

	log.Printf("Album art handler initialized with security validation and retry logic")
	return handler, nil
}

// StartTransfer initializes a new album art transfer
func (h *Handler) StartTransfer(hash string, totalChunks uint16) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Clean up any existing transfer for this hash
	delete(h.transfers, hash)

	// Initialize new transfer state
	h.transfers[hash] = &TransferState{
		Hash:         hash,
		TotalChunks:  totalChunks,
		ReceivedMask: make([]bool, totalChunks),
		Data:         make([][]byte, totalChunks),
		StartTime:    time.Now(),
		LastUpdate:   time.Now(),
		Complete:     false,
	}

	log.Printf("BLE_AA_ADVANCED: Started album art transfer for hash %s (%d chunks)", hash, totalChunks)
}

// ProcessChunk processes an incoming album art chunk with comprehensive validation
func (h *Handler) ProcessChunk(chunk *AlbumArtChunk) error {
	// Decode base64 data first
	decodedData, err := chunk.GetDecodedData()
	if err != nil {
		log.Printf("BLE_AA_ADVANCED: Failed to decode base64 data for hash %s chunk %d: %v", chunk.Hash, chunk.ChunkIndex, err)
		return fmt.Errorf("base64 decode failed: %w", err)
	}

	log.Printf("BLE_AA_ADVANCED: Processing chunk %d/%d for hash %s (%d bytes decoded)",
		chunk.ChunkIndex+1, chunk.TotalChunks, chunk.Hash, len(decodedData))

	// Create internal chunk with decoded data for validation
	internalChunk := &AlbumArtChunkInternal{
		Hash:        chunk.Hash,
		ChunkIndex:  chunk.ChunkIndex,
		TotalChunks: chunk.TotalChunks,
		Data:        decodedData,
		CRC32:       chunk.CRC32,
	}

	// Comprehensive chunk validation
	if err := h.validator.ValidateChunkInternal(internalChunk); err != nil {
		log.Printf("BLE_AA_ADVANCED: Chunk validation failed for hash %s chunk %d: %v", chunk.Hash, chunk.ChunkIndex, err)
		return fmt.Errorf("chunk validation failed: %w", err)
	}

	h.mutex.Lock()
	defer h.mutex.Unlock()

	transfer, exists := h.transfers[chunk.Hash]
	if !exists {
		// Auto-initialize transfer if we receive a chunk without explicit start
		h.transfers[chunk.Hash] = &TransferState{
			Hash:         chunk.Hash,
			TotalChunks:  chunk.TotalChunks,
			ReceivedMask: make([]bool, chunk.TotalChunks),
			Data:         make([][]byte, chunk.TotalChunks),
			StartTime:    time.Now(),
			LastUpdate:   time.Now(),
			Complete:     false,
		}
		transfer = h.transfers[chunk.Hash]
		log.Printf("BLE_AA_ADVANCED: Auto-initialized album art transfer for hash %s (%d chunks expected)",
			chunk.Hash, chunk.TotalChunks)
	}

	// Additional transfer state validation using internal chunk data
	if internalChunk.ChunkIndex >= transfer.TotalChunks {
		return fmt.Errorf("invalid chunk index %d for transfer with %d chunks",
			internalChunk.ChunkIndex, transfer.TotalChunks)
	}

	if internalChunk.TotalChunks != transfer.TotalChunks {
		return fmt.Errorf("chunk total chunks mismatch: expected %d, got %d",
			transfer.TotalChunks, internalChunk.TotalChunks)
	}

	// Store chunk data if not already received (prevents duplicate processing)
	if !transfer.ReceivedMask[internalChunk.ChunkIndex] {
		transfer.ReceivedMask[internalChunk.ChunkIndex] = true
		transfer.Data[internalChunk.ChunkIndex] = make([]byte, len(internalChunk.Data))
		copy(transfer.Data[internalChunk.ChunkIndex], internalChunk.Data)
		transfer.LastUpdate = time.Now()

		// Notify retry manager of chunk reception
		if h.retryMgr != nil {
			h.retryMgr.NotifyChunkReceived(internalChunk.Hash, internalChunk.ChunkIndex, internalChunk.TotalChunks)
		}

		receivedCount := 0
		for _, received := range transfer.ReceivedMask {
			if received {
				receivedCount++
			}
		}

		log.Printf("BLE_AA_ADVANCED: Received and stored chunk %d/%d for hash %s (%d/%d total chunks received)",
			internalChunk.ChunkIndex+1, transfer.TotalChunks, internalChunk.Hash,
			receivedCount, transfer.TotalChunks)
	} else {
		log.Printf("BLE_AA_ADVANCED: Ignoring duplicate chunk %d for hash %s", chunk.ChunkIndex, chunk.Hash)
	}

	// Check if transfer is complete
	complete := true
	for _, received := range transfer.ReceivedMask {
		if !received {
			complete = false
			break
		}
	}

	if complete && !transfer.Complete {
		transfer.Complete = true
		return h.completeTransfer(transfer)
	}

	return nil
}

// ProcessBinaryChunk processes an incoming binary album art chunk
// This is the new efficient binary protocol (no base64, no JSON)
func (h *Handler) ProcessBinaryChunk(data []byte) error {
	// Parse binary chunk
	chunk, err := ParseBinaryChunk(data)
	if err != nil {
		log.Printf("BLE_AA_BINARY: Failed to parse binary chunk: %v", err)
		return fmt.Errorf("binary parse failed: %w", err)
	}

	hashStr := chunk.HashString()
	log.Printf("BLE_AA_BINARY: Processing chunk %d/%d for hash %s (%d bytes)",
		chunk.ChunkIndex+1, chunk.TotalChunks, hashStr, len(chunk.Data))

	// Convert to internal format
	internalChunk := chunk.ToInternal()

	// Comprehensive chunk validation
	if err := h.validator.ValidateChunkInternal(internalChunk); err != nil {
		log.Printf("BLE_AA_BINARY: Chunk validation failed for hash %s chunk %d: %v", hashStr, chunk.ChunkIndex, err)
		return fmt.Errorf("chunk validation failed: %w", err)
	}

	h.mutex.Lock()
	defer h.mutex.Unlock()

	transfer, exists := h.transfers[hashStr]
	if !exists {
		// Auto-initialize transfer if we receive a chunk without explicit start
		h.transfers[hashStr] = &TransferState{
			Hash:         hashStr,
			TotalChunks:  chunk.TotalChunks,
			ReceivedMask: make([]bool, chunk.TotalChunks),
			Data:         make([][]byte, chunk.TotalChunks),
			StartTime:    time.Now(),
			LastUpdate:   time.Now(),
			Complete:     false,
		}
		transfer = h.transfers[hashStr]
		log.Printf("BLE_AA_BINARY: Auto-initialized transfer for hash %s (%d chunks expected)", hashStr, chunk.TotalChunks)
	}

	// Validate chunk parameters
	if chunk.ChunkIndex >= transfer.TotalChunks {
		return fmt.Errorf("invalid chunk index %d for transfer with %d chunks", chunk.ChunkIndex, transfer.TotalChunks)
	}

	if chunk.TotalChunks != transfer.TotalChunks {
		return fmt.Errorf("chunk total chunks mismatch: expected %d, got %d", transfer.TotalChunks, chunk.TotalChunks)
	}

	// Store chunk data if not already received
	if !transfer.ReceivedMask[chunk.ChunkIndex] {
		transfer.ReceivedMask[chunk.ChunkIndex] = true
		transfer.Data[chunk.ChunkIndex] = make([]byte, len(chunk.Data))
		copy(transfer.Data[chunk.ChunkIndex], chunk.Data)
		transfer.LastUpdate = time.Now()

		// Notify retry manager
		if h.retryMgr != nil {
			h.retryMgr.NotifyChunkReceived(hashStr, chunk.ChunkIndex, chunk.TotalChunks)
		}

		receivedCount := 0
		for _, received := range transfer.ReceivedMask {
			if received {
				receivedCount++
			}
		}

		log.Printf("BLE_AA_BINARY: Stored chunk %d/%d for hash %s (%d/%d received)",
			chunk.ChunkIndex+1, transfer.TotalChunks, hashStr, receivedCount, transfer.TotalChunks)
	} else {
		log.Printf("BLE_AA_BINARY: Ignoring duplicate chunk %d for hash %s", chunk.ChunkIndex, hashStr)
	}

	// Check if transfer is complete
	complete := true
	for _, received := range transfer.ReceivedMask {
		if !received {
			complete = false
			break
		}
	}

	if complete && !transfer.Complete {
		transfer.Complete = true
		return h.completeTransfer(transfer)
	}

	return nil
}

// completeTransfer finalizes a complete album art transfer with comprehensive validation
func (h *Handler) completeTransfer(transfer *TransferState) error {
	// Reassemble data
	var buffer bytes.Buffer
	for _, chunkData := range transfer.Data {
		buffer.Write(chunkData)
	}

	data := buffer.Bytes()

	// Comprehensive validation of complete image
	if err := h.validator.ValidateCompleteImage(transfer.Hash, data); err != nil {
		log.Printf("BLE_AA_ADVANCED: Complete image validation failed for hash %s: %v", transfer.Hash, err)

		// Notify retry manager of failure
		if h.retryMgr != nil {
			h.retryMgr.NotifyTransferFailed(transfer.Hash, err)
		}

		// Clean up failed transfer
		delete(h.transfers, transfer.Hash)
		return fmt.Errorf("image validation failed: %w", err)
	}

	duration := time.Since(transfer.StartTime)
	log.Printf("BLE_AA_ADVANCED: Successfully completed album art transfer for hash %s (%d bytes in %v)",
		transfer.Hash, len(data), duration)

	// Cache to disk with validation
	if err := h.cacheAlbumArt(transfer.Hash, data); err != nil {
		log.Printf("BLE_AA_ADVANCED: Failed to cache album art for hash %s: %v", transfer.Hash, err)
		// Don't fail the entire transfer for caching errors
	} else {
		log.Printf("BLE_AA_ADVANCED: Successfully cached album art for hash %s", transfer.Hash)
	}

	// Notify retry manager of successful completion
	if h.retryMgr != nil {
		h.retryMgr.NotifyTransferComplete(transfer.Hash)
	}

	// Notify completion callback SYNCHRONOUSLY to ensure cache coherence
	// This guarantees that Redis is updated before we consider the transfer complete.
	// If Redis update fails, the disk file still exists and will be picked up on cache hit.
	if h.onComplete != nil {
		if err := h.onComplete(transfer.Hash, data); err != nil {
			// Log error but don't fail the transfer - disk cache is valid
			// Redis will be updated on next cache hit via handleAlbumArtHashChange
			log.Printf("BLE_AA_ADVANCED: Warning: onComplete callback failed for hash %s: %v (disk cache is valid)", transfer.Hash, err)
		}
	}

	// Clean up transfer state
	delete(h.transfers, transfer.Hash)

	return nil
}

// GetCachedAlbumArt retrieves album art from disk cache with validation
func (h *Handler) GetCachedAlbumArt(hash string) ([]byte, error) {
	// Sanitize hash for filesystem safety
	sanitizedHash, err := h.validator.SanitizeHash(hash)
	if err != nil {
		return nil, fmt.Errorf("invalid hash format: %w", err)
	}

	cachePath := filepath.Join(h.cacheDir, sanitizedHash+".webp")

	data, err := os.ReadFile(cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Not cached
		}
		return nil, fmt.Errorf("failed to read cached album art: %w", err)
	}

	// Comprehensive validation of cached data
	if err := h.validator.ValidateCompleteImage(hash, data); err != nil {
		log.Printf("BLE_AA_ADVANCED: Cached album art validation failed, removing: %s (%v)", cachePath, err)
		os.Remove(cachePath)
		return nil, nil // Treat as not cached
	}

	log.Printf("BLE_AA_ADVANCED: Retrieved cached album art for hash %s (%d bytes)", hash, len(data))

	return data, nil
}

// cacheAlbumArt stores album art to disk cache with validation
func (h *Handler) cacheAlbumArt(hash string, data []byte) error {
	// Sanitize hash for filesystem safety
	sanitizedHash, err := h.validator.SanitizeHash(hash)
	if err != nil {
		return fmt.Errorf("invalid hash format: %w", err)
	}

	cachePath := filepath.Join(h.cacheDir, sanitizedHash+".webp")

	// Write with secure permissions
	err = os.WriteFile(cachePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write album art cache: %w", err)
	}

	// Verify written file
	verifyData, err := os.ReadFile(cachePath)
	if err != nil {
		return fmt.Errorf("failed to verify cached file: %w", err)
	}

	if !bytes.Equal(data, verifyData) {
		os.Remove(cachePath)
		return fmt.Errorf("cache verification failed: written data does not match")
	}

	log.Printf("BLE_AA_ADVANCED: Successfully cached album art to %s (%d bytes)", cachePath, len(data))
	return nil
}

// CleanupStaleTransfers removes transfers that have been inactive
func (h *Handler) CleanupStaleTransfers(maxAge time.Duration) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	now := time.Now()
	for hash, transfer := range h.transfers {
		if now.Sub(transfer.LastUpdate) > maxAge {
			log.Printf("BLE_AA_ADVANCED: Cleaning up stale album art transfer: %s (inactive for %v)", hash, now.Sub(transfer.LastUpdate))
			delete(h.transfers, hash)
		}
	}
}

// GetTransferStatus returns the status of an ongoing transfer
func (h *Handler) GetTransferStatus(hash string) (received int, total int, complete bool) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	transfer, exists := h.transfers[hash]
	if !exists {
		return 0, 0, false
	}

	received = 0
	for _, recv := range transfer.ReceivedMask {
		if recv {
			received++
		}
	}

	return received, int(transfer.TotalChunks), transfer.Complete
}

// CreateChunks splits album art data into chunks for transmission (now base64 encoded)
func CreateChunks(hash string, data []byte) []*AlbumArtChunk {
	totalSize := len(data)
	totalChunks := uint16((totalSize + ChunkSize - 1) / ChunkSize)

	chunks := make([]*AlbumArtChunk, totalChunks)

	for i := uint16(0); i < totalChunks; i++ {
		start := int(i) * ChunkSize
		end := start + ChunkSize
		if end > totalSize {
			end = totalSize
		}

		chunkData := data[start:end]

		// Encode data as base64 to match Android protocol
		encodedData := base64.StdEncoding.EncodeToString(chunkData)

		chunks[i] = &AlbumArtChunk{
			Hash:        hash,
			ChunkIndex:  i,
			TotalChunks: totalChunks,
			Data:        encodedData,                   // Now base64 string
			CRC32:       crc32.ChecksumIEEE(chunkData), // CRC32 of raw data, not base64
		}
	}

	return chunks
}

// SetRequestFunc sets the function used to request album art retransmission
// requestFunc takes (hash, chunks) where chunks is nil for full request or []uint16 for selective retry
func (h *Handler) SetRequestFunc(requestFunc func(string, []uint16) error) {
	if h.retryMgr != nil {
		return // Already set
	}

	retryConfig := DefaultRetryConfig()
	h.retryMgr = NewRetryManager(retryConfig, requestFunc)

	log.Printf("Album art retry manager initialized with config: max_retries=%d, initial_delay=%v, selective_retry=enabled",
		retryConfig.MaxRetries, retryConfig.InitialDelay)
}

// RequestWithRetry requests album art with automatic retry logic
func (h *Handler) RequestWithRetry(hash string) error {
	if h.retryMgr == nil {
		return fmt.Errorf("retry manager not initialized")
	}

	return h.retryMgr.RequestAlbumArt(hash)
}

// CancelStaleRequests cancels all active requests except for the current hash
// This should be called when the track changes to prevent old requests from continuing
func (h *Handler) CancelStaleRequests(currentHash string) {
	if h.retryMgr == nil {
		return
	}

	activeRequests := h.retryMgr.GetActiveRequests()
	for _, hash := range activeRequests {
		if hash != currentHash {
			log.Printf("BLE_AA_ADVANCED: Cancelling stale album art request for hash %s (current: %s)", hash, currentHash)
			h.retryMgr.CancelRequest(hash)
		}
	}

	// Also clean up any stale transfers in the handler
	h.mutex.Lock()
	for hash := range h.transfers {
		if hash != currentHash {
			log.Printf("BLE_AA_ADVANCED: Cleaning up stale transfer for hash %s (current: %s)", hash, currentHash)
			delete(h.transfers, hash)
		}
	}
	h.mutex.Unlock()
}

// GetRetryStatus returns the status of retry operations
func (h *Handler) GetRetryStatus() map[string]interface{} {
	if h.retryMgr == nil {
		return map[string]interface{}{"enabled": false}
	}

	return map[string]interface{}{
		"enabled":        true,
		"activeRequests": h.retryMgr.GetActiveRequests(),
	}
}

// GetValidationStatus returns validation configuration and stats
func (h *Handler) GetValidationStatus() map[string]interface{} {
	return h.validator.GetValidationStats()
}

// GetMigrationStatus returns cache migration status
func (h *Handler) GetMigrationStatus() map[string]interface{} {
	return h.migration.GetMigrationStatus()
}

// computeCRC32 computes CRC32 hash of data (internal function)
func computeCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// computeImageHash computes the standardized hash for complete image data
func computeImageHash(data []byte) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(data))
}

// ComputeHash computes CRC32 hash of album art data (public API)
func ComputeHash(data []byte) string {
	return computeImageHash(data)
}

// GenerateAlbumArtHash generates the album art hash using the same algorithm as Android
// Android: CRC32("${artist.trim().lowercase()}|${album.trim().lowercase()}") as decimal string
func GenerateAlbumArtHash(artist, album string) string {
	// Trim whitespace and convert to lowercase to match Android behavior
	// This ensures consistent hashing regardless of metadata variations
	artistLower := strings.ToLower(strings.TrimSpace(artist))
	albumLower := strings.ToLower(strings.TrimSpace(album))

	// Create the composite string exactly as Android does
	composite := artistLower + "|" + albumLower

	// Compute CRC32 hash
	hash := crc32.ChecksumIEEE([]byte(composite))

	// Return as decimal string to match Android format
	return fmt.Sprintf("%d", hash)
}

// ValidateAlbumArtHash validates that a received hash matches expected artist/album
func ValidateAlbumArtHash(receivedHash, artist, album string) bool {
	expectedHash := GenerateAlbumArtHash(artist, album)
	return receivedHash == expectedHash
}

// CacheReconcileResult holds the results of cache reconciliation
type CacheReconcileResult struct {
	DiskOnlyCount     int      // Files on disk but not in Redis
	RedisOnlyCount    int      // Entries in Redis but not on disk
	SyncedToRedis     int      // Files synced from disk to Redis
	RemovedFromRedis  int      // Orphaned entries removed from Redis
	Errors            []string // Any errors encountered
}

// ReconcileCaches synchronizes the disk cache with Redis cache at startup
// syncFunc is called for each disk file that needs to be synced to Redis
// Returns reconciliation statistics
func (h *Handler) ReconcileCaches(getRedisHashes func() ([]string, error), syncToRedis func(hash string, data []byte) error, removeFromRedis func(hash string) error) (*CacheReconcileResult, error) {
	log.Printf("BLE_AA_RECONCILE: Starting cache reconciliation for directory: %s", h.cacheDir)

	result := &CacheReconcileResult{
		Errors: make([]string, 0),
	}

	// Step 1: Get all files from disk cache
	diskHashes := make(map[string]string) // hash -> filepath
	entries, err := os.ReadDir(h.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("BLE_AA_RECONCILE: Cache directory does not exist, nothing to reconcile")
			return result, nil
		}
		return nil, fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		filename := entry.Name()
		ext := filepath.Ext(filename)
		if ext != ".jpg" && ext != ".jpeg" && ext != ".png" && ext != ".webp" {
			continue
		}

		hash := strings.TrimSuffix(filename, ext)

		// Validate hash format (8-10 decimal digits for Android CRC32)
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

		if isValidHash {
			diskHashes[hash] = filepath.Join(h.cacheDir, filename)
		}
	}

	log.Printf("BLE_AA_RECONCILE: Found %d valid cache files on disk", len(diskHashes))

	// Step 2: Get all hashes from Redis
	redisHashes := make(map[string]bool)
	if getRedisHashes != nil {
		hashes, err := getRedisHashes()
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to get Redis hashes: %v", err))
			log.Printf("BLE_AA_RECONCILE: Warning: Failed to get Redis hashes: %v", err)
		} else {
			for _, hash := range hashes {
				redisHashes[hash] = true
			}
			log.Printf("BLE_AA_RECONCILE: Found %d cache entries in Redis", len(redisHashes))
		}
	}

	// Step 3: Sync disk → Redis (populate Redis with files missing from cache)
	for hash, filepath := range diskHashes {
		if !redisHashes[hash] {
			result.DiskOnlyCount++

			if syncToRedis != nil {
				// Read file and sync to Redis
				data, err := os.ReadFile(filepath)
				if err != nil {
					errMsg := fmt.Sprintf("failed to read file %s: %v", filepath, err)
					result.Errors = append(result.Errors, errMsg)
					log.Printf("BLE_AA_RECONCILE: %s", errMsg)
					continue
				}

				// Validate image before syncing
				if err := h.validator.ValidateCompleteImage(hash, data); err != nil {
					errMsg := fmt.Sprintf("invalid image file %s: %v", filepath, err)
					result.Errors = append(result.Errors, errMsg)
					log.Printf("BLE_AA_RECONCILE: %s", errMsg)
					continue
				}

				if err := syncToRedis(hash, data); err != nil {
					errMsg := fmt.Sprintf("failed to sync %s to Redis: %v", hash, err)
					result.Errors = append(result.Errors, errMsg)
					log.Printf("BLE_AA_RECONCILE: %s", errMsg)
				} else {
					result.SyncedToRedis++
					log.Printf("BLE_AA_RECONCILE: Synced disk cache to Redis: %s", hash)
				}
			}
		}
	}

	// Step 4: Clean Redis orphans (entries in Redis but not on disk)
	for hash := range redisHashes {
		if _, exists := diskHashes[hash]; !exists {
			result.RedisOnlyCount++

			if removeFromRedis != nil {
				if err := removeFromRedis(hash); err != nil {
					errMsg := fmt.Sprintf("failed to remove orphan %s from Redis: %v", hash, err)
					result.Errors = append(result.Errors, errMsg)
					log.Printf("BLE_AA_RECONCILE: %s", errMsg)
				} else {
					result.RemovedFromRedis++
					log.Printf("BLE_AA_RECONCILE: Removed orphaned Redis entry: %s", hash)
				}
			}
		}
	}

	log.Printf("BLE_AA_RECONCILE: Reconciliation complete: disk_only=%d, redis_only=%d, synced=%d, removed=%d, errors=%d",
		result.DiskOnlyCount, result.RedisOnlyCount, result.SyncedToRedis, result.RemovedFromRedis, len(result.Errors))

	return result, nil
}

// GetDiskCacheHashes returns all valid hashes from the disk cache
func (h *Handler) GetDiskCacheHashes() ([]string, error) {
	hashes := make([]string, 0)

	entries, err := os.ReadDir(h.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return hashes, nil
		}
		return nil, fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		filename := entry.Name()
		ext := filepath.Ext(filename)
		if ext != ".jpg" && ext != ".jpeg" && ext != ".png" && ext != ".webp" {
			continue
		}

		hash := strings.TrimSuffix(filename, ext)

		// Validate hash format (8-10 decimal digits)
		isValid := true
		if len(hash) < 8 || len(hash) > 10 {
			isValid = false
		} else {
			for _, c := range hash {
				if c < '0' || c > '9' {
					isValid = false
					break
				}
			}
		}

		if isValid {
			hashes = append(hashes, hash)
		}
	}

	return hashes, nil
}
