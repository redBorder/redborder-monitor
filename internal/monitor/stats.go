package monitor

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// SensorStatus holds stats for an individual sensor.
type SensorStatus struct {
	Name              string `json:"name"`
	IP                string `json:"ip"`
	Queue             string `json:"queue"` // "HIGH" or "LOW"
	LastRunTimestamp  int64  `json:"last_run_timestamp"`
	LastRunDurationMs int64  `json:"last_run_duration_ms"`
	LastError         string `json:"last_error,omitempty"`
	ErrorCount        int64  `json:"error_count"`
	TimeoutCount      int64  `json:"timeout_count"`
	SkippedCount      int64  `json:"skipped_count"`
}

// Stats holds statistical telemetry for the redborder-monitor daemon.
type Stats struct {
	UptimeSeconds          int64          `json:"uptime_seconds"`
	TotalWorkers           int64          `json:"total_workers"`
	ActiveWorkers          int64          `json:"active_workers"`
	AllocBytes             uint64         `json:"alloc_bytes"`
	SysBytes               uint64         `json:"sys_bytes"`
	NumGC                  uint32         `json:"num_gc"`
	SensorsProcessed       int64          `json:"sensors_processed"`
	MetricsGenerated       int64          `json:"metrics_generated"`
	MetricsSentStdout      int64          `json:"metrics_sent_stdout"`
	MetricsSentKafka       int64          `json:"metrics_sent_kafka"`
	MetricsFailedKafka     int64          `json:"metrics_failed_kafka"`
	MetricsSentHTTP        int64          `json:"metrics_sent_http"`
	MetricsFailedHTTP      int64          `json:"metrics_failed_http"`
	SensorsSkipped         int64          `json:"sensors_skipped"`
	MaxSensorRunDurationMs int64          `json:"max_sensor_run_duration_ms"`
	CycleBudgetOk          bool           `json:"cycle_budget_ok"`
	Sensors                []SensorStatus `json:"sensors,omitempty"`
}

var (
	startTime          = time.Now()
	totalWorkers       int64
	activeWorkers      int64
	sensorsProcessed   int64
	metricsGenerated   int64
	metricsSentStdout  int64
	metricsSentKafka   int64
	metricsFailedKafka int64
	metricsSentHTTP    int64
	metricsFailedHTTP  int64
	sensorsSkipped     int64
	activeSensors      []*SafeSensor
	activeSensorsMu    sync.RWMutex
)

// SetActiveSensors registers the active list of SafeSensors for status reporting.
func SetActiveSensors(sensors []*SafeSensor) {
	activeSensorsMu.Lock()
	defer activeSensorsMu.Unlock()
	activeSensors = sensors
}

// IncActiveWorkers increments the active worker count.
func IncActiveWorkers() { atomic.AddInt64(&activeWorkers, 1) }

// DecActiveWorkers decrements the active worker count.
func DecActiveWorkers() { atomic.AddInt64(&activeWorkers, -1) }

// SetTotalWorkers sets the configured total worker thread count.
func SetTotalWorkers(val int64) { atomic.StoreInt64(&totalWorkers, val) }

// IncSensorsProcessed increments the total processed sensor cycle count.
func IncSensorsProcessed() { atomic.AddInt64(&sensorsProcessed, 1) }

// IncSensorsSkipped increments the count of skipped sensor cycles.
func IncSensorsSkipped() { atomic.AddInt64(&sensorsSkipped, 1) }

// IncMetricsGenerated increments the generated metric count.
func IncMetricsGenerated() { atomic.AddInt64(&metricsGenerated, 1) }

// IncMetricsSentStdout increments the stdout metric output count.
func IncMetricsSentStdout() { atomic.AddInt64(&metricsSentStdout, 1) }

// IncMetricsSentKafka increments the successful Kafka write count.
func IncMetricsSentKafka() { atomic.AddInt64(&metricsSentKafka, 1) }

// IncMetricsFailedKafka increments the failed Kafka write count.
func IncMetricsFailedKafka() { atomic.AddInt64(&metricsFailedKafka, 1) }

// IncMetricsSentHTTP increments the successful HTTP POST write count.
func IncMetricsSentHTTP() { atomic.AddInt64(&metricsSentHTTP, 1) }

// IncMetricsFailedHTTP increments the failed HTTP POST write count.
func IncMetricsFailedHTTP() { atomic.AddInt64(&metricsFailedHTTP, 1) }

// GetStats returns a snapshot of the current stats.
func GetStats() Stats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	activeSensorsMu.RLock()
	var sensorStatuses []SensorStatus
	var maxDuration int64
	budgetOk := true
	for _, ss := range activeSensors {
		if ss != nil {
			status := ss.GetStatus()
			sensorStatuses = append(sensorStatuses, status)
			if status.LastRunDurationMs > maxDuration {
				maxDuration = status.LastRunDurationMs
			}
			// If a sensor has run at least once, check if it exceeded its timeout
			if status.LastRunTimestamp > 0 && status.LastRunDurationMs >= int64(ss.Sensor.Timeout) {
				budgetOk = false
			}
		}
	}
	activeSensorsMu.RUnlock()

	return Stats{
		UptimeSeconds:          int64(time.Since(startTime).Seconds()),
		TotalWorkers:           atomic.LoadInt64(&totalWorkers),
		ActiveWorkers:          atomic.LoadInt64(&activeWorkers),
		AllocBytes:             m.Alloc,
		SysBytes:               m.Sys,
		NumGC:                  m.NumGC,
		SensorsProcessed:       atomic.LoadInt64(&sensorsProcessed),
		MetricsGenerated:       atomic.LoadInt64(&metricsGenerated),
		MetricsSentStdout:      atomic.LoadInt64(&metricsSentStdout),
		MetricsSentKafka:       atomic.LoadInt64(&metricsSentKafka),
		MetricsFailedKafka:     atomic.LoadInt64(&metricsFailedKafka),
		MetricsSentHTTP:        atomic.LoadInt64(&metricsSentHTTP),
		MetricsFailedHTTP:      atomic.LoadInt64(&metricsFailedHTTP),
		SensorsSkipped:         atomic.LoadInt64(&sensorsSkipped),
		MaxSensorRunDurationMs: maxDuration,
		CycleBudgetOk:          budgetOk,
		Sensors:                sensorStatuses,
	}
}

// ResetStats resets all atomic stats counters and per-sensor statistics.
func ResetStats() {
	atomic.StoreInt64(&sensorsProcessed, 0)
	atomic.StoreInt64(&metricsGenerated, 0)
	atomic.StoreInt64(&metricsSentStdout, 0)
	atomic.StoreInt64(&metricsSentKafka, 0)
	atomic.StoreInt64(&metricsFailedKafka, 0)
	atomic.StoreInt64(&metricsSentHTTP, 0)
	atomic.StoreInt64(&metricsFailedHTTP, 0)
	atomic.StoreInt64(&sensorsSkipped, 0)

	activeSensorsMu.RLock()
	for _, ss := range activeSensors {
		if ss != nil {
			ss.ResetStats()
		}
	}
	activeSensorsMu.RUnlock()
}
