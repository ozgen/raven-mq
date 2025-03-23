package log

import (
	"fmt"
	"github.com/ozgen/raven-mq/internal/config"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	DEBUG    = "DEBUG"
	INFO     = "INFO"
	WARN     = "WARN"
	ERROR    = "ERROR"
	CRITICAL = "CRITICAL"
	LOG_FILE = "raven-mq.log"
)

var logLevelPriority = map[string]int{
	DEBUG:    1,
	INFO:     2,
	WARN:     3,
	ERROR:    4,
	CRITICAL: 5,
}

var configuredLogLevel = config.Envs.LogLevel

var logger *log.Logger

func init() {
	logDir := config.Envs.LogDir

	// Create logs directory if it doesn't exist
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err := os.Mkdir(logDir, 0755)
		if err != nil {
			log.Fatalf("Could not create log directory: %v", err)
		}
	}

	// Open log file
	logPath := filepath.Join(logDir, LOG_FILE)
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Could not open log file: %v", err)
	}

	if _, ok := logLevelPriority[configuredLogLevel]; !ok {
		configuredLogLevel = INFO
	}

	// Initialize logger with std out and log file
	multiWriter := io.MultiWriter(file, os.Stdout)
	logger = log.New(multiWriter, "", log.LstdFlags|log.Lmicroseconds)
}

// logMessage now supports formatted messages
func logMessage(level, format string, args ...interface{}) {

	if logLevelPriority[level] < logLevelPriority[configuredLogLevel] {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	logEntry := fmt.Sprintf("[%s] [%s] %s", timestamp, level, message)

	// Print to stdout
	fmt.Println(logEntry)

	// Write to log file
	logger.Println(logEntry)
}

// Log functions now accept formatted strings
func LogDebug(format string, args ...interface{}) {
	logMessage(DEBUG, format, args...)
}

func LogInfo(format string, args ...interface{}) {
	logMessage(INFO, format, args...)
}

func LogWarn(format string, args ...interface{}) {
	logMessage(WARN, format, args...)
}

func LogError(format string, args ...interface{}) {
	logMessage(ERROR, format, args...)
}

func LogCritical(format string, args ...interface{}) {
	logMessage(CRITICAL, format, args...)
}
