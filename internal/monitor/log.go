package monitor

import (
	"fmt"
	"log"
	"sync/atomic"
)

// Log levels matching standard syslog
const (
	LogEmerg   = 0
	LogAlert   = 1
	LogCrit    = 2
	LogErr     = 3
	LogWarning = 4
	LogNotice  = 5
	LogInfo    = 6
	LogDebug   = 7
)

var severityPrefixes = map[int]string{
	LogEmerg:   "[EMERG]",
	LogAlert:   "[ALERT]",
	LogCrit:    "[CRIT]",
	LogErr:     "[ERROR]",
	LogWarning: "[WARNING]",
	LogNotice:  "[NOTICE]",
	LogInfo:    "[INFO]",
	LogDebug:   "[DEBUG]",
}

// AtomicDebugLevel holds the globally configured debug severity level.
var AtomicDebugLevel int32 = 100

// LogMsg logs a message if the configured debugLevel is greater than or equal to the message severity.
func LogMsg(severity int, format string, v ...interface{}) {
	if int(atomic.LoadInt32(&AtomicDebugLevel)) >= severity {
		prefix, ok := severityPrefixes[severity]
		if !ok {
			prefix = fmt.Sprintf("[LEVEL-%d]", severity)
		}
		msg := fmt.Sprintf(format, v...)
		log.Printf("%s %s", prefix, msg)
	}
}
