package queue

import (
	"fmt"
	"sync"
)

// Config represents configuration options for setting up a Queue or database.
type Config struct {
	LocalFile string // The path to the local file or in-memory database identifier.
	Reset     bool   // Flag to indicate whether the database should be reset.
}

var (
	counter int        // Global counter for generating unique LocalFile identifiers.
	mx      sync.Mutex // Mutex to ensure thread-safe increments of the counter.
)

// getNextLocalFile generates a unique identifier for an in-memory SQLite database.
// It increments a global counter to ensure uniqueness.
func getNextLocalFile() string {
	mx.Lock()         // Lock to ensure thread-safe access to the counter.
	defer mx.Unlock() // Unlock after the function finishes.

	counter++                                                            // Increment the counter to get a new unique value.
	return fmt.Sprintf("file:memdb%d?mode=memory&cache=shared", counter) // Return the formatted string as the next LocalFile.
}

// configDefault provides default configuration settings when none are specified.
// It assigns a unique in-memory LocalFile and sets the default Reset flag.
func configDefault(config ...Config) Config {
	var defaultValue = Config{
		LocalFile: getNextLocalFile(), // Set a default LocalFile to a new unique in-memory database.
		Reset:     false,              // Default Reset flag is false.
	}

	// Return default configuration if no custom config is provided.
	if len(config) < 1 {
		return defaultValue
	}

	cfg := config[0] // Use the provided configuration for defaults extension.

	// Apply default LocalFile if it's not specified in the provided config.
	if cfg.LocalFile == "" {
		cfg.LocalFile = defaultValue.LocalFile
	}

	return cfg
}
