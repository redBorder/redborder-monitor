package monitor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ErrNoSample indicates that a monitor has nothing to publish this cycle,
// without anything being wrong: e.g. the first reading of a counter (no
// previous reading to compute a rate from) or a counter reset. The monitor
// is skipped silently and the sensor is not counted as failing.
var ErrNoSample = errors.New("no sample to publish this cycle")

// counterSample is one raw reading of an SNMP counter.
type counterSample struct {
	value uint64
	bits  int // 32 or 64
	at    time.Time
}

// swapCounterSample stores cur as the latest reading for key and returns the
// previous one (ok=false when there was none).
func (ss *SafeSensor) swapCounterSample(key string, cur counterSample) (counterSample, bool) {
	ss.countersMu.Lock()
	defer ss.countersMu.Unlock()

	if ss.counters == nil {
		ss.counters = make(map[string]counterSample)
	}
	prev, ok := ss.counters[key]
	ss.counters[key] = cur
	return prev, ok
}

// counterRate returns the per-second increase between two readings of the
// same counter. ok=false when no rate can be computed: the time didn't move
// forward, or the counter went backwards for a reason other than a Counter32
// wrap (a Counter64 that goes backwards, or a change of width, means the
// agent reset it, e.g. on reboot).
//
// A Counter32 that goes backwards is taken as having wrapped exactly once.
// A reset of a 32-bit counter can't be told apart from a wrap; callers that
// care can bound the result (see the max_rate param of snmp_rate).
func counterRate(prev, cur counterSample) (float64, bool) {
	seconds := cur.at.Sub(prev.at).Seconds()
	if seconds <= 0 {
		return 0, false
	}

	var delta uint64
	switch {
	case cur.bits != prev.bits:
		return 0, false
	case cur.value >= prev.value:
		delta = cur.value - prev.value
	case cur.bits == 32:
		delta = (1 << 32) - prev.value + cur.value
	default:
		return 0, false
	}

	return float64(delta) / seconds, true
}

// SNMPRatePlugin reads an SNMP Counter32/Counter64 and publishes its rate per
// second instead of the raw value, so e.g. ifHCInOctets is stored as traffic
// instead of a number that only grows.
//
// Params:
//
//	"oid" (string): counter OID (defaults to the monitor's "oid")
//	"multiplier" (float): applied to the rate, e.g. 8 to turn octets/s into bits/s (default 1)
//	"max_rate" (float): rates above this value (after multiplier) are dropped
//	                    instead of published; 0 disables the check (default 0)
//
// The previous reading is kept per sensor in memory, so the first cycle
// after start (and after a counter reset) publishes nothing. A failed query
// doesn't touch the previous reading: the next successful one gives the
// average rate over the gap.
func SNMPRatePlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	oid := getStringParam(m.Params, "oid", m.Oid)
	if oid == "" {
		return "", fmt.Errorf("missing OID for SNMP rate plugin")
	}
	multiplier := getFloatParam(m.Params, "multiplier", 1)
	maxRate := getFloatParam(m.Params, "max_rate", 0)

	value, bits, err := SolveSNMPCounter(
		ctx,
		ss.Sensor.SensorIP,
		ss.Sensor.Community,
		ss.Sensor.SnmpVersion,
		ss.Sensor.Timeout,
		oid,
		ss.Sensor.SnmpUsername,
		ss.Sensor.SnmpSecurityLevel,
		ss.Sensor.SnmpAuthProtocol,
		ss.Sensor.SnmpAuthPassword,
		ss.Sensor.SnmpPrivProtocol,
		ss.Sensor.SnmpPrivPassword,
	)
	if err != nil {
		return "", err
	}

	cur := counterSample{value: value, bits: bits, at: time.Now()}
	prev, ok := ss.swapCounterSample(m.Name+"|"+oid, cur)
	if !ok {
		return "", fmt.Errorf("%w: first reading of %s", ErrNoSample, oid)
	}

	rate, ok := counterRate(prev, cur)
	if !ok {
		LogMsg(LogInfo, "Counter %s of monitor '%s' on sensor '%s' was reset (%d -> %d), skipping this cycle",
			oid, m.Name, ss.Sensor.SensorName, prev.value, cur.value)
		return "", fmt.Errorf("%w: counter %s reset", ErrNoSample, oid)
	}

	rate *= multiplier
	if maxRate > 0 && rate > maxRate {
		LogMsg(LogInfo, "Rate %f of monitor '%s' on sensor '%s' is above max_rate %f (counter %s reset?), skipping this cycle",
			rate, m.Name, ss.Sensor.SensorName, maxRate, oid)
		return "", fmt.Errorf("%w: rate above max_rate for %s", ErrNoSample, oid)
	}

	return strconv.FormatFloat(rate, 'f', 6, 64), nil
}
