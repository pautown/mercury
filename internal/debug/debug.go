// Package debug provides debug flags for verbose logging of specific subsystems.
package debug

import (
	"log"
	"sync"
)

var (
	// LyricsDebug controls verbose logging for lyrics-related operations.
	// When enabled, all [LYRICS] log statements are printed with extra detail.
	LyricsDebug bool

	// mu protects debug flags from concurrent access
	mu sync.RWMutex
)

// SetLyricsDebug enables or disables lyrics debug mode.
// This is thread-safe.
func SetLyricsDebug(enabled bool) {
	mu.Lock()
	defer mu.Unlock()
	LyricsDebug = enabled
	if enabled {
		log.Println("[LYRICS] Debug mode enabled - showing all lyrics operations")
	}
}

// IsLyricsDebug returns whether lyrics debug mode is enabled.
// This is thread-safe.
func IsLyricsDebug() bool {
	mu.RLock()
	defer mu.RUnlock()
	return LyricsDebug
}

// LogLyrics logs a message with [LYRICS] prefix only if lyrics debug mode is enabled.
func LogLyrics(format string, args ...interface{}) {
	if IsLyricsDebug() {
		log.Printf("[LYRICS] "+format, args...)
	}
}

// LogLyricsVerbose logs verbose/detailed messages only in debug mode.
// Use this for extra detail like full JSON payloads.
func LogLyricsVerbose(format string, args ...interface{}) {
	if IsLyricsDebug() {
		log.Printf("[LYRICS][VERBOSE] "+format, args...)
	}
}
