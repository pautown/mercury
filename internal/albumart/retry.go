package albumart

import (
	"context"
	"log"
	"sync"
	"time"
)

// RetryConfig defines configuration for retry logic
type RetryConfig struct {
	MaxRetries     int           `json:"maxRetries"`
	InitialDelay   time.Duration `json:"initialDelay"`
	MaxDelay       time.Duration `json:"maxDelay"`
	Multiplier     float64       `json:"multiplier"`
	RequestTimeout time.Duration `json:"requestTimeout"`
	ChunkTimeout   time.Duration `json:"chunkTimeout"`
}

// DefaultRetryConfig returns sensible defaults for retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:     3,
		InitialDelay:   2 * time.Second,
		MaxDelay:       30 * time.Second,
		Multiplier:     2.0,
		RequestTimeout: 60 * time.Second,
		ChunkTimeout:   30 * time.Second,
	}
}

// RetryManager manages retry logic for album art requests
type RetryManager struct {
	config         RetryConfig
	activeRequests map[string]*RetryState
	mutex          sync.RWMutex
	// requestFunc takes hash and optional chunk indices (nil = request all chunks)
	requestFunc func(hash string, chunks []uint16) error
}

// RetryState tracks the retry state for a specific album art request
type RetryState struct {
	Hash            string
	AttemptCount    int
	LastAttempt     time.Time
	NextRetryTime   time.Time
	RequestedAt     time.Time
	Context         context.Context
	CancelFunc      context.CancelFunc
	ExpectedChunks  uint16
	ReceivedChunks  map[uint16]bool
	LastChunkTime   time.Time
	ChunkRetryCount int
}

// NewRetryManager creates a new retry manager with the given configuration
// requestFunc takes (hash, chunks) where chunks is nil for full request or []uint16 for selective retry
func NewRetryManager(config RetryConfig, requestFunc func(string, []uint16) error) *RetryManager {
	if requestFunc == nil {
		panic("requestFunc cannot be nil")
	}

	return &RetryManager{
		config:         config,
		activeRequests: make(map[string]*RetryState),
		requestFunc:    requestFunc,
	}
}

// RequestAlbumArt initiates an album art request with retry logic
func (rm *RetryManager) RequestAlbumArt(hash string) error {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	// Check if already requesting this hash
	if existing, exists := rm.activeRequests[hash]; exists {
		// Check if request has timed out
		if time.Since(existing.RequestedAt) > rm.config.RequestTimeout {
			log.Printf("BLE_AA_ADVANCED: Album art request for %s timed out after %v, restarting",
				hash, rm.config.RequestTimeout)
			existing.CancelFunc()
			delete(rm.activeRequests, hash)
		} else {
			log.Printf("BLE_AA_ADVANCED: Album art request for %s already in progress (attempt %d/%d)",
				hash, existing.AttemptCount, rm.config.MaxRetries)
			return nil
		}
	}

	// Create new retry state
	ctx, cancel := context.WithTimeout(context.Background(), rm.config.RequestTimeout)
	state := &RetryState{
		Hash:           hash,
		AttemptCount:   0,
		RequestedAt:    time.Now(),
		Context:        ctx,
		CancelFunc:     cancel,
		ReceivedChunks: make(map[uint16]bool),
	}

	rm.activeRequests[hash] = state

	// Start the retry process
	go rm.retryLoop(state)

	return nil
}

// retryLoop handles the retry logic for a specific album art request
func (rm *RetryManager) retryLoop(state *RetryState) {
	defer func() {
		rm.mutex.Lock()
		delete(rm.activeRequests, state.Hash)
		state.CancelFunc()
		rm.mutex.Unlock()
	}()

	for state.AttemptCount < rm.config.MaxRetries {
		select {
		case <-state.Context.Done():
			log.Printf("BLE_AA_ADVANCED: Album art request for %s cancelled or timed out", state.Hash)
			return
		default:
		}

		// Calculate delay for this attempt
		delay := rm.calculateDelay(state.AttemptCount)

		// Wait for retry delay (except for first attempt)
		if state.AttemptCount > 0 {
			log.Printf("BLE_AA_ADVANCED: Retrying album art request for %s in %v (attempt %d/%d)",
				state.Hash, delay, state.AttemptCount+1, rm.config.MaxRetries)

			select {
			case <-time.After(delay):
				// Continue with retry
			case <-state.Context.Done():
				return
			}
		}

		// Attempt the request
		state.AttemptCount++
		state.LastAttempt = time.Now()

		log.Printf("BLE_AA_ADVANCED: Requesting album art for %s (attempt %d/%d)",
			state.Hash, state.AttemptCount, rm.config.MaxRetries)

		// Request all chunks (nil = full request)
		if err := rm.requestFunc(state.Hash, nil); err != nil {
			log.Printf("BLE_AA_ADVANCED: Album art request attempt %d failed for %s: %v",
				state.AttemptCount, state.Hash, err)
			continue
		}

		// Request sent successfully, now wait for chunks
		if rm.waitForChunks(state) {
			log.Printf("Album art transfer completed successfully for %s", state.Hash)
			return
		}

		// If we get here, chunk transfer failed or timed out
		log.Printf("Album art chunk transfer failed for %s, will retry", state.Hash)
	}

	log.Printf("Album art request for %s failed after %d attempts",
		state.Hash, rm.config.MaxRetries)
}

// waitForChunks waits for album art chunks to arrive with timeout
func (rm *RetryManager) waitForChunks(state *RetryState) bool {
	chunkTimeout := time.NewTimer(rm.config.ChunkTimeout)
	defer chunkTimeout.Stop()

	// Reset chunk tracking for this attempt
	state.ReceivedChunks = make(map[uint16]bool)
	state.LastChunkTime = time.Now()
	state.ChunkRetryCount = 0

	for {
		select {
		case <-chunkTimeout.C:
			log.Printf("Chunk timeout for album art %s after %v (received %d/%d chunks)",
				state.Hash, rm.config.ChunkTimeout,
				len(state.ReceivedChunks), state.ExpectedChunks)
			return false

		case <-state.Context.Done():
			return false

		case <-time.After(100 * time.Millisecond):
			// Check if transfer is complete
			rm.mutex.RLock()
			_, stillActive := rm.activeRequests[state.Hash]
			rm.mutex.RUnlock()

			if !stillActive {
				// Transfer completed and state was cleaned up
				return true
			}

			// Check for chunk progress timeout
			if time.Since(state.LastChunkTime) > rm.config.ChunkTimeout/3 && len(state.ReceivedChunks) > 0 {
				log.Printf("No chunk progress for album art %s in %v, requesting missing chunks",
					state.Hash, time.Since(state.LastChunkTime))

				// Request retransmission of missing chunks
				if err := rm.requestMissingChunks(state); err != nil {
					log.Printf("Failed to request missing chunks for %s: %v", state.Hash, err)
				}

				state.LastChunkTime = time.Now()
				state.ChunkRetryCount++

				// Reset timeout for chunk retry
				chunkTimeout.Reset(rm.config.ChunkTimeout)
			}
		}
	}
}

// requestMissingChunks requests retransmission of only the missing chunks
func (rm *RetryManager) requestMissingChunks(state *RetryState) error {
	if state.ExpectedChunks == 0 {
		// Haven't received first chunk yet, can't determine missing chunks
		return nil
	}

	missingChunks := make([]uint16, 0)
	for i := uint16(0); i < state.ExpectedChunks; i++ {
		if !state.ReceivedChunks[i] {
			missingChunks = append(missingChunks, i)
		}
	}

	if len(missingChunks) == 0 {
		return nil
	}

	// Log missing chunk summary
	missingCount := len(missingChunks)
	log.Printf("BLE_AA_ADVANCED: Requesting selective retry for %s: %d/%d chunks missing",
		state.Hash, missingCount, state.ExpectedChunks)

	// If more than 50% missing, request full retransmission (more efficient)
	if float64(missingCount) > float64(state.ExpectedChunks)*0.5 {
		log.Printf("BLE_AA_ADVANCED: >50%% chunks missing (%d/%d), requesting full retransmission",
			missingCount, state.ExpectedChunks)
		return rm.requestFunc(state.Hash, nil)
	}

	// Log first few missing chunks for debugging (limit to 10 to avoid log spam)
	if missingCount <= 10 {
		log.Printf("BLE_AA_ADVANCED: Missing chunks for %s: %v", state.Hash, missingChunks)
	} else {
		log.Printf("BLE_AA_ADVANCED: Missing chunks for %s: %v... and %d more",
			state.Hash, missingChunks[:10], missingCount-10)
	}

	// Request only the missing chunks (selective retry)
	return rm.requestFunc(state.Hash, missingChunks)
}

// calculateDelay calculates exponential backoff delay
func (rm *RetryManager) calculateDelay(attemptCount int) time.Duration {
	if attemptCount == 0 {
		return 0 // No delay for first attempt
	}

	delay := float64(rm.config.InitialDelay) *
		(rm.config.Multiplier * float64(attemptCount-1))

	if time.Duration(delay) > rm.config.MaxDelay {
		return rm.config.MaxDelay
	}

	return time.Duration(delay)
}

// NotifyChunkReceived notifies the retry manager that a chunk was received
func (rm *RetryManager) NotifyChunkReceived(hash string, chunkIndex, totalChunks uint16) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	state, exists := rm.activeRequests[hash]
	if !exists {
		return
	}

	// Update expected chunks count from first chunk
	if state.ExpectedChunks == 0 {
		state.ExpectedChunks = totalChunks
		log.Printf("Album art transfer for %s expects %d chunks", hash, totalChunks)
	}

	// Mark chunk as received
	state.ReceivedChunks[chunkIndex] = true
	state.LastChunkTime = time.Now()

	receivedCount := len(state.ReceivedChunks)
	log.Printf("Received chunk %d/%d for album art %s (%d/%d total received)",
		chunkIndex+1, totalChunks, hash, receivedCount, totalChunks)
}

// NotifyTransferComplete notifies the retry manager that a transfer completed
func (rm *RetryManager) NotifyTransferComplete(hash string) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	if state, exists := rm.activeRequests[hash]; exists {
		log.Printf("Album art transfer completed for %s after %d attempts",
			hash, state.AttemptCount)
		state.CancelFunc()
		delete(rm.activeRequests, hash)
	}
}

// NotifyTransferFailed notifies the retry manager that a transfer failed
func (rm *RetryManager) NotifyTransferFailed(hash string, err error) {
	rm.mutex.RLock()
	state, exists := rm.activeRequests[hash]
	rm.mutex.RUnlock()

	if exists {
		log.Printf("Album art transfer failed for %s: %v (attempt %d/%d)",
			hash, err, state.AttemptCount, rm.config.MaxRetries)
		// Let the retry loop handle the failure
	}
}

// CancelRequest cancels an active album art request
func (rm *RetryManager) CancelRequest(hash string) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	if state, exists := rm.activeRequests[hash]; exists {
		log.Printf("Cancelling album art request for %s", hash)
		state.CancelFunc()
		delete(rm.activeRequests, hash)
	}
}

// GetActiveRequests returns a list of currently active requests
func (rm *RetryManager) GetActiveRequests() []string {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	requests := make([]string, 0, len(rm.activeRequests))
	for hash := range rm.activeRequests {
		requests = append(requests, hash)
	}

	return requests
}

// GetRequestStatus returns detailed status for a specific request
func (rm *RetryManager) GetRequestStatus(hash string) map[string]interface{} {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	state, exists := rm.activeRequests[hash]
	if !exists {
		return nil
	}

	return map[string]interface{}{
		"hash":            state.Hash,
		"attemptCount":    state.AttemptCount,
		"maxRetries":      rm.config.MaxRetries,
		"requestedAt":     state.RequestedAt,
		"lastAttempt":     state.LastAttempt,
		"expectedChunks":  state.ExpectedChunks,
		"receivedChunks":  len(state.ReceivedChunks),
		"lastChunkTime":   state.LastChunkTime,
		"chunkRetryCount": state.ChunkRetryCount,
		"timeoutAt":       state.RequestedAt.Add(rm.config.RequestTimeout),
	}
}

// Cleanup removes stale requests that have timed out
func (rm *RetryManager) Cleanup() {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	now := time.Now()
	for hash, state := range rm.activeRequests {
		if now.Sub(state.RequestedAt) > rm.config.RequestTimeout {
			log.Printf("Cleaning up stale album art request for %s", hash)
			state.CancelFunc()
			delete(rm.activeRequests, hash)
		}
	}
}
