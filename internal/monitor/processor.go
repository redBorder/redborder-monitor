package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MonitorValue holds a single value and its timestamp.
type MonitorValue struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
	Text      string  `json:"-"`
}

// MonitorState holds the cached/current state of a monitor.
type MonitorState struct {
	Scalar   *MonitorValue
	Children []*MonitorValue // for vector instances
	SplitOp  *MonitorValue   // aggregate
}

// Metric is the output metric structure sent to stdout, HTTP, or Kafka.
type Metric struct {
	Timestamp  int64                  `json:"timestamp"`
	Monitor    string                 `json:"monitor"`
	Instance   string                 `json:"instance,omitempty"`
	Value      interface{}            `json:"value"` // float64 (as string) or int64
	GroupID    interface{}            `json:"group_id,omitempty"`
	Enrichment map[string]interface{} `json:"-"`
}

// MarshalJSON customizes the JSON output of Metric to merge enrichment keys and format values.
func (m Metric) MarshalJSON() ([]byte, error) {
	res := make(map[string]interface{})
	for k, v := range m.Enrichment {
		res[k] = v
	}
	res["timestamp"] = m.Timestamp
	res["monitor"] = m.Monitor
	if m.Instance != "" {
		res["instance"] = m.Instance
	}
	res["value"] = m.Value
	if m.GroupID != nil {
		res["group_id"] = m.GroupID
	}
	return json.Marshal(res)
}

// MaxSimultaneousQueries is the maximum number of concurrent SNMP/Plugin queries executed per sensor at any given time.
var MaxSimultaneousQueries int = 10

// SafeSensor wraps a Sensor with a mutex to prevent concurrent runs and a cache for states.
// lowPriority : 1 if slow/timed out/failed, 0 otherwise
type SafeSensor struct {
	Sensor            *Sensor
	Mutex             sync.Mutex
	Cache             map[string]*MonitorState
	Enrichment        map[string]interface{}
	lowPriority       int32
	lastRunTimestamp  int64
	lastRunDurationMs int64
	lastError         string
	errorCount        int64
	timeoutCount      int64
	skippedCount      int64
	statsMu           sync.RWMutex
}

func (ss *SafeSensor) IsLowPriority() bool {
	return atomic.LoadInt32(&ss.lowPriority) == 1
}

func (ss *SafeSensor) SetLowPriority(low bool) {
	var val int32
	if low {
		val = 1
	}
	atomic.StoreInt32(&ss.lowPriority, val)
}

func NewSafeSensor(sensor *Sensor) *SafeSensor {
	// Pre-build sensor level enrichment
	enrich := make(map[string]interface{})
	for k, v := range sensor.Enrichment {
		enrich[k] = v
	}
	enrich["sensor_name"] = sensor.SensorName
	if sensor.SensorID > 0 {
		enrich["sensor_id"] = sensor.SensorID
	}

	// Normalize timeout: if specified in seconds (< 100), convert to milliseconds
	if sensor.Timeout > 0 && sensor.Timeout < 100 {
		sensor.Timeout = sensor.Timeout * 1000
	} else if sensor.Timeout <= 0 {
		sensor.Timeout = 5000 // default to 5000ms
	}

	return &SafeSensor{
		Sensor:     sensor,
		Cache:      make(map[string]*MonitorState),
		Enrichment: enrich,
	}
}

func (ss *SafeSensor) IncSkippedCount() {
	atomic.AddInt64(&ss.skippedCount, 1)
}

func (ss *SafeSensor) GetSkippedCount() int64 {
	return atomic.LoadInt64(&ss.skippedCount)
}

func (ss *SafeSensor) GetStatus() SensorStatus {
	ss.statsMu.RLock()
	defer ss.statsMu.RUnlock()

	queue := "HIGH"
	if ss.IsLowPriority() {
		queue = "LOW"
	}

	return SensorStatus{
		Name:              ss.Sensor.SensorName,
		IP:                ss.Sensor.SensorIP,
		Queue:             queue,
		LastRunTimestamp:  ss.lastRunTimestamp,
		LastRunDurationMs: ss.lastRunDurationMs,
		LastError:         ss.lastError,
		ErrorCount:        ss.errorCount,
		TimeoutCount:      ss.timeoutCount,
		SkippedCount:      ss.GetSkippedCount(),
	}
}

func (ss *SafeSensor) ResetStats() {
	ss.statsMu.Lock()
	defer ss.statsMu.Unlock()
	ss.lastRunTimestamp = 0
	ss.lastRunDurationMs = 0
	ss.lastError = ""
	ss.errorCount = 0
	ss.timeoutCount = 0
	atomic.StoreInt64(&ss.skippedCount, 0)
}

// BuildMonitorEnrichment merges sensor-level and monitor-level enrichments.
func (ss *SafeSensor) BuildMonitorEnrichment(m *Monitor) map[string]interface{} {
	enrich := make(map[string]interface{})
	for k, v := range ss.Enrichment {
		enrich[k] = v
	}
	for k, v := range m.Enrichment {
		enrich[k] = v
	}
	enrich["type"] = getMonitorType(m)
	if m.Unit != "" {
		enrich["unit"] = m.Unit
	}
	if m.GroupName != "" {
		enrich["group_name"] = m.GroupName
	}
	return enrich
}

func getMonitorType(m *Monitor) string {
	if m.System != "" {
		return "system"
	}
	if m.Oid != "" {
		return "snmp"
	}
	if m.Op != "" {
		return "op"
	}
	return ""
}

// ParseGroupID returns the group ID as int64 or string, or nil.
func ParseGroupID(gid interface{}) interface{} {
	if gid == nil {
		return nil
	}
	switch v := gid.(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case string:
		// Check if it represents an integer
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		return v
	default:
		return v
	}
}

// ProcessSensor executes queries for all monitors in a sensor asynchronously/concurrently for I/O bounds, and sequentially for dependency calculations.
func ProcessSensor(ctx context.Context, ss *SafeSensor, outputChan chan<- Metric) {
	if !ss.Mutex.TryLock() {
		LogMsg(LogInfo, "Sensor '%s' is already being processed. Skipping.", ss.Sensor.SensorName)
		IncSensorsSkipped()
		ss.IncSkippedCount()
		return
	}
	defer ss.Mutex.Unlock()

	IncActiveWorkers()
	defer DecActiveWorkers()
	IncSensorsProcessed()

	priority := "FAST"
	if ss.IsLowPriority() {
		priority = "SLOW"
	}
	LogMsg(LogDebug, "Starting query cycle for sensor '%s' (Priority: %s)", ss.Sensor.SensorName, priority)

	start := time.Now()
	var hasError int32
	var errorsList []string
	var errorsMu sync.Mutex

	// Holds the values gathered in this iteration for dependency resolution.
	// Map key is: monitorName or monitorName_gid_<groupId>
	currentVals := make(map[string]*MonitorState)

	for k, v := range ss.Cache {
		currentVals[k] = v
	}

	// Pre-calculate/query SNMP and Plugin monitors in parallel (asynchronously)
	newStates := make([]*MonitorState, len(ss.Sensor.Monitors))
	var wg sync.WaitGroup

	limit := MaxSimultaneousQueries
	if limit <= 0 {
		limit = 10
	}
	sem := make(chan struct{}, limit)

	for i := range ss.Sensor.Monitors {
		m := &ss.Sensor.Monitors[i]
		if m.Oid != "" {
			wg.Add(1)
			go func(idx int, mon *Monitor) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-sem }()

				res, err := processSNMPMonitor(ctx, ss, mon)
				if err != nil {
					LogMsg(LogWarning, "SNMP query failed for monitor '%s' on sensor '%s': %v", mon.Name, ss.Sensor.SensorName, err)
					atomic.StoreInt32(&hasError, 1)
					errorsMu.Lock()
					errorsList = append(errorsList, fmt.Sprintf("SNMP:%s: %v", mon.Name, err))
					errorsMu.Unlock()
				}
				newStates[idx] = res
			}(i, m)
		} else if m.Plugin != "" {
			wg.Add(1)
			go func(idx int, mon *Monitor) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-sem }()

				res, err := processPluginMonitor(ctx, ss, mon)
				if err != nil {
					LogMsg(LogWarning, "Plugin '%s' failed for monitor '%s' on sensor '%s': %v", mon.Plugin, mon.Name, ss.Sensor.SensorName, err)
					atomic.StoreInt32(&hasError, 1)
					errorsMu.Lock()
					errorsList = append(errorsList, fmt.Sprintf("PLUGIN:%s: %v", mon.Name, err))
					errorsMu.Unlock()
				}
				newStates[idx] = res
			}(i, m)
		}
	}

	// Wait for all SNMP/Plugin queries to complete
	wg.Wait()

	// Sequentially process each monitor (System queries, Op calculations, and cache/output updates)
	for i := range ss.Sensor.Monitors {
		m := &ss.Sensor.Monitors[i]
		var newState *MonitorState

		if m.Oid != "" || m.Plugin != "" {
			newState = newStates[i]
		} else if m.System != "" {
			var err error
			newState, err = processSystemMonitor(ctx, ss, m)
			if err != nil {
				LogMsg(LogWarning, "System command failed for monitor '%s' on sensor '%s': %v", m.Name, ss.Sensor.SensorName, err)
				atomic.StoreInt32(&hasError, 1)
				errorsMu.Lock()
				errorsList = append(errorsList, fmt.Sprintf("SYS:%s: %v", m.Name, err))
				errorsMu.Unlock()
			}
		} else if m.Op != "" {
			newState = processOpMonitor(ctx, ss, m, currentVals)
		}

		if newState == nil {
			continue
		}

		// Save current state for downstream calculations
		key := m.Name
		gid := ParseGroupID(m.GroupID)
		if gid != nil {
			key = fmt.Sprintf("%s_gid_%v", m.Name, gid)
		}
		currentVals[key] = newState
		currentVals[m.Name] = newState // Also map by base name for same-group lookup

		// Determine what to output based on cache and properties
		cachedState := ss.Cache[m.Name]
		outputMetrics(m, newState, cachedState, ss.BuildMonitorEnrichment(m), outputChan)

		// Update cache
		ss.Cache[m.Name] = newState
	}

	// Measure duration and update priority status at the end of the entire query cycle
	duration := time.Since(start)
	threshold := time.Duration(ss.Sensor.Timeout) * time.Millisecond * 9 / 10

	wasLowPriority := ss.IsLowPriority()
	isLowPriorityNow := duration >= threshold || atomic.LoadInt32(&hasError) == 1
	ss.SetLowPriority(isLowPriorityNow)

	if isLowPriorityNow && !wasLowPriority {
		LogMsg(LogWarning, "Sensor '%s' transitioned to SLOW priority (Duration: %v, Timeout: %dms, HasError: %t)", ss.Sensor.SensorName, duration, ss.Sensor.Timeout, atomic.LoadInt32(&hasError) == 1)
	} else if !isLowPriorityNow && wasLowPriority {
		LogMsg(LogNotice, "Sensor '%s' recovered to FAST priority (Duration: %v)", ss.Sensor.SensorName, duration)
	}

	// Update sensor stats telemetry
	ss.statsMu.Lock()
	ss.lastRunTimestamp = start.Unix()
	ss.lastRunDurationMs = duration.Milliseconds()
	if len(errorsList) > 0 {
		ss.lastError = strings.Join(errorsList, "; ")
		ss.errorCount++
	} else {
		ss.lastError = ""
	}
	if duration >= threshold {
		ss.timeoutCount++
	}
	ss.statsMu.Unlock()

	LogMsg(LogDebug, "Finished query cycle for sensor '%s' in %v (Priority: %s, LowPriority: %t)", ss.Sensor.SensorName, duration, priority, isLowPriorityNow)
}

func processSystemMonitor(ctx context.Context, ss *SafeSensor, m *Monitor) (*MonitorState, error) {
	timeout := m.TimestampGiven != nil
	_ = timeout
	out, err := SolveSystemCommand(ctx, m.System, ss.Sensor.Timeout/1000)
	return parseRawOutput(out, m), err
}

func processPluginMonitor(ctx context.Context, ss *SafeSensor, m *Monitor) (*MonitorState, error) {
	pluginDef, ok := PluginsRegistry[m.Plugin]
	if !ok {
		return parseRawOutput("0", m), fmt.Errorf("plugin '%s' not found in registry", m.Plugin)
	}
	out, err := pluginDef.Func(ctx, ss, m)
	if err != nil {
		if out == "" {
			out = "0"
		}
	}
	return parseRawOutput(out, m), err
}

func processSNMPMonitor(ctx context.Context, ss *SafeSensor, m *Monitor) (*MonitorState, error) {
	outStr, outFloat, err := SolveSNMPQuery(
		ctx,
		ss.Sensor.SensorIP,
		ss.Sensor.Community,
		ss.Sensor.SnmpVersion,
		ss.Sensor.Timeout,
		m.Oid,
		ss.Sensor.SnmpUsername,
		ss.Sensor.SnmpSecurityLevel,
		ss.Sensor.SnmpAuthProtocol,
		ss.Sensor.SnmpAuthPassword,
		ss.Sensor.SnmpPrivProtocol,
		ss.Sensor.SnmpPrivPassword,
	)
	if err != nil {
		// On SNMP fail, default to 0
		outStr = "0"
		outFloat = 0
	}
	return parseRawOutputWithValue(outStr, outFloat, m), err
}

func processOpMonitor(ctx context.Context, ss *SafeSensor, m *Monitor, currentVals map[string]*MonitorState) *MonitorState {
	vars, err := ExtractVariables(m.Op)
	if err != nil {
		return nil
	}

	// Resolve dependencies
	deps := make(map[string]*MonitorState)
	isVector := false
	vectorLen := 0

	for _, vName := range vars {
		depState, ok := currentVals[vName]
		if !ok {
			// If not found, look up with current monitor's group suffix
			gid := ParseGroupID(m.GroupID)
			if gid != nil {
				depState, ok = currentVals[fmt.Sprintf("%s_gid_%v", vName, gid)]
			}
		}

		if !ok || depState == nil {
			// Missing dependency
			return nil
		}

		deps[vName] = depState
		if len(depState.Children) > 0 {
			isVector = true
			if vectorLen == 0 {
				vectorLen = len(depState.Children)
			} else if len(depState.Children) != vectorLen {
				// Different vector sizes - error/mismatch
				return nil
			}
		}
	}

	now := time.Now().Unix()

	if isVector {
		children := make([]*MonitorValue, vectorLen)
		var sum float64
		count := 0

		for k := 0; k < vectorLen; k++ {
			// Build evaluation variables map
			evalVars := make(map[string]float64)
			hasAllVal := true
			var itemTimestamp int64

			for _, vName := range vars {
				depState := deps[vName]
				if len(depState.Children) > 0 {
					child := depState.Children[k]
					if child == nil {
						hasAllVal = false
						break
					}
					evalVars[vName] = child.Value
					if itemTimestamp == 0 {
						itemTimestamp = child.Timestamp
					}
				} else {
					// Scalar dependency in vector op (as fallback)
					if depState.Scalar == nil {
						hasAllVal = false
						break
					}
					evalVars[vName] = depState.Scalar.Value
				}
			}

			if hasAllVal {
				res, err := EvaluateExpression(m.Op, evalVars)
				if err == nil && !math.IsNaN(res) && !math.IsInf(res, 0) {
					if itemTimestamp == 0 {
						itemTimestamp = now
					}
					children[k] = &MonitorValue{
						Timestamp: itemTimestamp,
						Value:     res,
						Text:      fmt.Sprintf("%f", res),
					}
					sum += res
					count++
				}
			}
		}

		var splitOpVal *MonitorValue
		if m.SplitOp != "" && count > 0 {
			res := sum
			if m.SplitOp == "mean" {
				res = sum / float64(count)
			}
			splitOpVal = &MonitorValue{
				Timestamp: now,
				Value:     res,
				Text:      fmt.Sprintf("%f", res),
			}
		}

		return &MonitorState{
			Children: children,
			SplitOp:  splitOpVal,
		}
	} else {
		// Scalar op
		evalVars := make(map[string]float64)
		for _, vName := range vars {
			depState := deps[vName]
			if depState.Scalar == nil {
				return nil
			}
			evalVars[vName] = depState.Scalar.Value
		}

		res, err := EvaluateExpression(m.Op, evalVars)
		if err != nil || math.IsNaN(res) || math.IsInf(res, 0) {
			return nil
		}

		return &MonitorState{
			Scalar: &MonitorValue{
				Timestamp: now,
				Value:     res,
				Text:      fmt.Sprintf("%f", res),
			},
		}
	}
}

func parseRawOutput(out string, m *Monitor) *MonitorState {
	fVal, _ := strconv.ParseFloat(out, 64)
	return parseRawOutputWithValue(out, fVal, m)
}

func parseRawOutputWithValue(outStr string, outFloat float64, m *Monitor) *MonitorState {
	now := time.Now().Unix()

	if m.Split == "" {
		return &MonitorState{
			Scalar: &MonitorValue{
				Timestamp: now,
				Value:     outFloat,
				Text:      outStr,
			},
		}
	}

	// Vector split
	parts := strings.Split(outStr, m.Split)
	children := make([]*MonitorValue, len(parts))
	var sum float64
	count := 0

	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var timestamp int64 = now
		valStr := part

		if m.TimestampGiven != nil && *m.TimestampGiven == 1 {
			colonIdx := strings.Index(part, ":")
			if colonIdx != -1 {
				tStr := part[:colonIdx]
				valStr = part[colonIdx+1:]
				if t, err := strconv.ParseInt(tStr, 10, 64); err == nil {
					timestamp = t
				}
			}
		}

		valFloat, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}

		children[i] = &MonitorValue{
			Timestamp: timestamp,
			Value:     valFloat,
			Text:      valStr,
		}
		sum += valFloat
		count++
	}

	var splitOpVal *MonitorValue
	if m.SplitOp != "" && count > 0 {
		res := sum
		if m.SplitOp == "mean" {
			res = sum / float64(count)
		}
		splitOpVal = &MonitorValue{
			Timestamp: now,
			Value:     res,
			Text:      fmt.Sprintf("%f", res),
		}
	}

	return &MonitorState{
		Children: children,
		SplitOp:  splitOpVal,
	}
}

func outputMetrics(m *Monitor, newState, cachedState *MonitorState, enrichment map[string]interface{}, outputChan chan<- Metric) {
	// If m.Send is configured and explicitly set to 0, do not send metrics
	if m.Send != nil && *m.Send == 0 {
		return
	}

	timestampGiven := m.TimestampGiven != nil && *m.TimestampGiven == 1
	integerOutput := m.Integer != nil && *m.Integer == 1

	formatValue := func(val float64) interface{} {
		if integerOutput {
			return int64(val)
		}
		// Format float as string matching C's %lf
		return fmt.Sprintf("%f", val)
	}

	gid := ParseGroupID(m.GroupID)

	if newState.Scalar != nil {
		newVal := newState.Scalar
		shouldSend := true
		if timestampGiven && cachedState != nil && cachedState.Scalar != nil {
			cachedVal := cachedState.Scalar
			shouldSend = newVal.Timestamp > cachedVal.Timestamp || newVal.Value != cachedVal.Value
		}

		if shouldSend {
			IncMetricsGenerated()
			outputChan <- Metric{
				Timestamp:  newVal.Timestamp,
				Monitor:    m.Name,
				Value:      formatValue(newVal.Value),
				GroupID:    gid,
				Enrichment: enrichment,
			}
		}
	}

	if len(newState.Children) > 0 {
		for i, newVal := range newState.Children {
			if newVal == nil {
				continue
			}

			shouldSend := true
			if timestampGiven && cachedState != nil && i < len(cachedState.Children) && cachedState.Children[i] != nil {
				cachedVal := cachedState.Children[i]
				shouldSend = newVal.Timestamp > cachedVal.Timestamp || newVal.Value != cachedVal.Value
			}

			if shouldSend {
				monName := m.Name
				if m.NameSplitSuffix != "" {
					monName = m.Name + m.NameSplitSuffix
				}
				inst := fmt.Sprintf("%d", i)
				if m.InstancePrefix != "" {
					inst = m.InstancePrefix + inst
				}

				IncMetricsGenerated()
				outputChan <- Metric{
					Timestamp:  newVal.Timestamp,
					Monitor:    monName,
					Instance:   inst,
					Value:      formatValue(newVal.Value),
					GroupID:    gid,
					Enrichment: enrichment,
				}
			}
		}
	}

	if newState.SplitOp != nil {
		IncMetricsGenerated()
		outputChan <- Metric{
			Timestamp:  newState.SplitOp.Timestamp,
			Monitor:    m.Name,
			Value:      formatValue(newState.SplitOp.Value),
			GroupID:    gid,
			Enrichment: enrichment,
		}
	}
}
