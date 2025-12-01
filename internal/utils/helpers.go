package utils

import (
	"encoding/json"
	"os"
	"time"
)

// LogEntry estructura para logs JSON
type LogEntry struct {
	Level   string                 `json:"level"`
	Message string                 `json:"message"`
	Context map[string]interface{} `json:"context,omitempty"`
	Time    time.Time              `json:"time"`
}

// LogJSON escribe un log estructurado en stdout
func LogJSON(level, msg string, ctx map[string]interface{}) {
	entry := LogEntry{
		Level:   level,
		Message: msg,
		Context: ctx,
		Time:    time.Now(),
	}
	json.NewEncoder(os.Stdout).Encode(entry)
}

// GetEnv obtiene una variable de entorno o un default
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}