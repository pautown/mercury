package ble

import (
	"fmt"
	"os"
	"time"
)

const errorLogPath = "/tmp/mediadash_ble_errors.log"

// logErrorToFile appends an error entry to the persistent error log file
// Creates the file if it doesn't exist, appends if it does
func logErrorToFile(eventType string, message string, err error) {
	f, fileErr := os.OpenFile(errorLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileErr != nil {
		// Can't log to file, just use standard log as fallback
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	var logLine string
	if err != nil {
		logLine = fmt.Sprintf("[%s] %s: %s - %v\n", timestamp, eventType, message, err)
	} else {
		logLine = fmt.Sprintf("[%s] %s: %s\n", timestamp, eventType, message)
	}

	f.WriteString(logLine)
}

// LogError logs a general error to the persistent error log
func LogError(errorType string, err error) {
	logErrorToFile("ERROR", errorType, err)
}

// LogDisconnect logs a disconnection event to the persistent error log
func LogDisconnect(message string) {
	logErrorToFile("DISCONNECT", message, nil)
}

// LogCrash logs a crash/panic to the persistent error log
func LogCrash(recovered interface{}) {
	logErrorToFile("CRASH", fmt.Sprintf("panic recovered: %v", recovered), nil)
}

// LogConnectionIssue logs connection-related issues
func LogConnectionIssue(message string, err error) {
	logErrorToFile("CONNECTION", message, err)
}
