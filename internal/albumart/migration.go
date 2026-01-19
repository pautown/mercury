package albumart

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CacheMigration handles migration of existing cache files to new format
type CacheMigration struct {
	cacheDir string
}

// NewCacheMigration creates a new cache migration manager
func NewCacheMigration(cacheDir string) *CacheMigration {
	return &CacheMigration{
		cacheDir: cacheDir,
	}
}

// MigrateCache migrates existing cache files to the new hash format
func (m *CacheMigration) MigrateCache() error {
	log.Printf("Starting cache migration for directory: %s", m.cacheDir)

	entries, err := os.ReadDir(m.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Cache directory does not exist, no migration needed")
			return nil
		}
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	migratedCount := 0
	errorCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, ".jpg") && !strings.HasSuffix(filename, ".jpeg") && !strings.HasSuffix(filename, ".png") && !strings.HasSuffix(filename, ".webp") {
			continue
		}

		if err := m.migrateFile(filename); err != nil {
			log.Printf("Failed to migrate file %s: %v", filename, err)
			errorCount++
		} else {
			migratedCount++
		}
	}

	log.Printf("Cache migration completed: %d migrated, %d errors",
		migratedCount, errorCount)

	// Clean up migration markers older than 30 days
	m.cleanupMigrationMarkers()

	return nil
}

// migrateFile migrates a single cache file to the new hash format
func (m *CacheMigration) migrateFile(filename string) error {
	oldPath := filepath.Join(m.cacheDir, filename)

	// Check if this is already in the new format (8-character hex filename)
	baseName := strings.TrimSuffix(filename, filepath.Ext(filename))
	if m.isNewFormatHash(baseName) {
		// Already migrated, verify integrity
		return m.verifyMigratedFile(oldPath, baseName)
	}

	// Read the file data
	data, err := os.ReadFile(oldPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Compute new hash using standardized algorithm
	newHash := computeImageHash(data)
	newFilename := newHash + ".webp"
	newPath := filepath.Join(m.cacheDir, newFilename)

	// Check if target already exists
	if _, err := os.Stat(newPath); err == nil {
		// Target exists, verify it's the same data
		existingData, err := os.ReadFile(newPath)
		if err != nil {
			return fmt.Errorf("failed to read existing target file: %w", err)
		}

		if len(data) == len(existingData) {
			// Same size, assume same file and remove old one
			log.Printf("Target file already exists with same size, removing old: %s -> %s",
				filename, newFilename)
			return os.Remove(oldPath)
		}
	}

	// Copy to new location
	if err := os.WriteFile(newPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write migrated file: %w", err)
	}

	// Verify the migration
	if err := m.verifyMigratedFile(newPath, newHash); err != nil {
		// Migration verification failed, clean up
		os.Remove(newPath)
		return fmt.Errorf("migration verification failed: %w", err)
	}

	// Create migration marker
	if err := m.createMigrationMarker(filename, newFilename); err != nil {
		log.Printf("Warning: Failed to create migration marker: %v", err)
	}

	// Remove old file
	if err := os.Remove(oldPath); err != nil {
		log.Printf("Warning: Failed to remove old file %s: %v", filename, err)
		// Don't fail the migration for this
	}

	log.Printf("Successfully migrated: %s -> %s", filename, newFilename)
	return nil
}

// isNewFormatHash checks if a filename is already in the new hash format
// The new format is a decimal CRC32 string (8-10 digits) matching Android's algorithm
func (m *CacheMigration) isNewFormatHash(hash string) bool {
	// Decimal CRC32 hashes are 8-10 digits (0 to 4294967295)
	if len(hash) < 8 || len(hash) > 10 {
		return false
	}

	// Check if all characters are decimal digits (not hex)
	for _, c := range hash {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// verifyMigratedFile verifies that a migrated file has correct hash
func (m *CacheMigration) verifyMigratedFile(filePath, expectedHash string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file for verification: %w", err)
	}

	actualHash := computeImageHash(data)
	if actualHash != expectedHash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	return nil
}

// createMigrationMarker creates a marker file to track successful migrations
func (m *CacheMigration) createMigrationMarker(oldFilename, newFilename string) error {
	markerDir := filepath.Join(m.cacheDir, ".migration_markers")
	if err := os.MkdirAll(markerDir, 0755); err != nil {
		return err
	}

	markerFile := filepath.Join(markerDir, "migration_"+time.Now().Format("20060102")+".log")
	f, err := os.OpenFile(markerFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf("%s: %s -> %s\n",
		time.Now().Format(time.RFC3339), oldFilename, newFilename))
	return err
}

// cleanupMigrationMarkers removes old migration marker files
func (m *CacheMigration) cleanupMigrationMarkers() {
	markerDir := filepath.Join(m.cacheDir, ".migration_markers")

	entries, err := os.ReadDir(markerDir)
	if err != nil {
		return // Directory doesn't exist or can't be read
	}

	cutoff := time.Now().AddDate(0, 0, -30) // 30 days ago

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			oldPath := filepath.Join(markerDir, entry.Name())
			if err := os.Remove(oldPath); err != nil {
				log.Printf("Warning: Failed to remove old migration marker %s: %v", oldPath, err)
			}
		}
	}
}

// GetMigrationStatus returns information about cache migration
func (m *CacheMigration) GetMigrationStatus() map[string]interface{} {
	status := map[string]interface{}{
		"cacheDir": m.cacheDir,
	}

	// Count files by format
	entries, err := os.ReadDir(m.cacheDir)
	if err != nil {
		status["error"] = err.Error()
		return status
	}

	newFormatCount := 0
	oldFormatCount := 0
	otherCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if strings.HasPrefix(filename, ".") {
			continue // Skip hidden files
		}

		ext := filepath.Ext(filename)
		if ext != ".jpg" && ext != ".jpeg" && ext != ".png" && ext != ".webp" {
			otherCount++
			continue
		}

		baseName := strings.TrimSuffix(filename, ext)
		if m.isNewFormatHash(baseName) {
			newFormatCount++
		} else {
			oldFormatCount++
		}
	}

	status["newFormatFiles"] = newFormatCount
	status["oldFormatFiles"] = oldFormatCount
	status["otherFiles"] = otherCount
	status["migrationNeeded"] = oldFormatCount > 0

	return status
}

// ForceMigration forces a re-migration of all files
func (m *CacheMigration) ForceMigration() error {
	log.Printf("Starting forced cache migration")

	// Remove migration markers to force re-processing
	markerDir := filepath.Join(m.cacheDir, ".migration_markers")
	os.RemoveAll(markerDir)

	return m.MigrateCache()
}

// ValidateCache validates all cached files have correct hashes
func (m *CacheMigration) ValidateCache() error {
	log.Printf("Validating cache integrity")

	entries, err := os.ReadDir(m.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	validCount := 0
	invalidCount := 0

	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		filename := entry.Name()
		ext := filepath.Ext(filename)
		if ext != ".jpg" && ext != ".jpeg" && ext != ".png" && ext != ".webp" {
			continue
		}

		baseName := strings.TrimSuffix(filename, ext)
		if !m.isNewFormatHash(baseName) {
			log.Printf("Skipping old format file: %s", filename)
			continue
		}

		filePath := filepath.Join(m.cacheDir, filename)
		if err := m.verifyMigratedFile(filePath, baseName); err != nil {
			log.Printf("Invalid cache file detected: %s (%v)", filename, err)
			invalidCount++

			// Remove invalid file
			if removeErr := os.Remove(filePath); removeErr != nil {
				log.Printf("Failed to remove invalid file %s: %v", filename, removeErr)
			} else {
				log.Printf("Removed invalid cache file: %s", filename)
			}
		} else {
			validCount++
		}
	}

	log.Printf("Cache validation completed: %d valid, %d invalid (removed)", validCount, invalidCount)

	if invalidCount > 0 {
		return fmt.Errorf("found and removed %d invalid cache files", invalidCount)
	}

	return nil
}
