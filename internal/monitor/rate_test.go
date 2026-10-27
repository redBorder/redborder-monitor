package monitor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
)

func TestCounterRate(t *testing.T) {
	t0 := time.Unix(1000, 0)
	t10 := t0.Add(10 * time.Second)

	cases := []struct {
		name   string
		prev   counterSample
		cur    counterSample
		want   float64
		wantOK bool
	}{
		{"64-bit increase", counterSample{1000, 64, t0}, counterSample{11000, 64, t10}, 1000, true},
		{"32-bit increase", counterSample{1000, 32, t0}, counterSample{2000, 32, t10}, 100, true},
		{"no change", counterSample{5000, 64, t0}, counterSample{5000, 64, t10}, 0, true},
		// 2^32 - 100 -> 900 is 1000 octets after one wrap.
		{"32-bit wrap", counterSample{(1 << 32) - 100, 32, t0}, counterSample{900, 32, t10}, 100, true},
		{"64-bit reset", counterSample{1 << 40, 64, t0}, counterSample{10, 64, t10}, 0, false},
		{"width change", counterSample{1000, 32, t0}, counterSample{1 << 40, 64, t10}, 0, false},
		{"same timestamp", counterSample{1000, 64, t0}, counterSample{2000, 64, t0}, 0, false},
		{"time going back", counterSample{1000, 64, t10}, counterSample{2000, 64, t0}, 0, false},
		// Above 2^53 float64 can't hold the counter exactly; the delta is still exact.
		{"huge 64-bit counter", counterSample{math.MaxUint64 - 20000, 64, t0}, counterSample{math.MaxUint64 - 10000, 64, t10}, 1000, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := counterRate(c.prev, c.cur)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if ok && math.Abs(got-c.want) > 1e-9 {
				t.Errorf("rate = %f, want %f", got, c.want)
			}
		})
	}
}

func TestSwapCounterSample(t *testing.T) {
	// SafeSensor built as a literal (as other tests do) has a nil map.
	ss := &SafeSensor{Sensor: &Sensor{SensorName: "s"}}

	first := counterSample{value: 1, bits: 64, at: time.Unix(1, 0)}
	if _, ok := ss.swapCounterSample("k", first); ok {
		t.Fatal("expected no previous sample on first swap")
	}

	second := counterSample{value: 2, bits: 64, at: time.Unix(2, 0)}
	prev, ok := ss.swapCounterSample("k", second)
	if !ok || prev != first {
		t.Fatalf("expected previous %+v, got %+v (ok=%v)", first, prev, ok)
	}

	if _, ok := ss.swapCounterSample("other", first); ok {
		t.Error("keys must not share samples")
	}
}

func TestDecodeSNMPCounter(t *testing.T) {
	v, bits, err := decodeSNMPCounter(gosnmp.SnmpPDU{Type: gosnmp.Counter32, Value: uint(4000000000)})
	if err != nil || v != 4000000000 || bits != 32 {
		t.Errorf("Counter32: got %d/%d/%v", v, bits, err)
	}

	v, bits, err = decodeSNMPCounter(gosnmp.SnmpPDU{Type: gosnmp.Counter64, Value: uint64(30374476063247)})
	if err != nil || v != 30374476063247 || bits != 64 {
		t.Errorf("Counter64: got %d/%d/%v", v, bits, err)
	}

	for _, typ := range []gosnmp.Asn1BER{gosnmp.Gauge32, gosnmp.Integer, gosnmp.OctetString} {
		_, _, err = decodeSNMPCounter(gosnmp.SnmpPDU{Name: ".1.2.3", Type: typ, Value: 1})
		if !errors.Is(err, ErrSNMPNonNumericValue) {
			t.Errorf("%v: expected ErrSNMPNonNumericValue, got %v", typ, err)
		}
	}
}

func TestGetFloatParam(t *testing.T) {
	params := map[string]interface{}{"f": 8.0, "i": 8, "s": "0.5", "bad": "x"}
	cases := map[string]float64{"f": 8, "i": 8, "s": 0.5, "bad": 1, "missing": 1}
	for key, want := range cases {
		if got := getFloatParam(params, key, 1); got != want {
			t.Errorf("%s: got %f, want %f", key, got, want)
		}
	}
	if got := getFloatParam(nil, "f", 3); got != 3 {
		t.Errorf("nil params: got %f, want 3", got)
	}
}

func TestSNMPRatePlugin_Registered(t *testing.T) {
	def, ok := PluginsRegistry["snmp_rate"]
	if !ok {
		t.Fatal("snmp_rate plugin not registered")
	}
	if !def.NoValueOnError {
		t.Error("snmp_rate must not publish a placeholder 0 on errors")
	}
}

func TestSNMPRatePlugin_Failure(t *testing.T) {
	ss := &SafeSensor{Sensor: &Sensor{SensorIP: "192.0.2.1", Community: "public", SnmpVersion: "2c", Timeout: 100}}
	m := &Monitor{Name: "rate", Plugin: "snmp_rate", Params: map[string]interface{}{"oid": "1.3.6.1.2.1.31.1.1.1.6.1"}}

	_, err := SNMPRatePlugin(context.Background(), ss, m)
	if err == nil || errors.Is(err, ErrNoSample) {
		t.Fatalf("expected a query error, got %v", err)
	}
	if len(ss.counters) != 0 {
		t.Error("a failed query must not store a sample")
	}

	if _, err := SNMPRatePlugin(context.Background(), ss, &Monitor{Name: "rate", Plugin: "snmp_rate"}); err == nil {
		t.Error("expected an error for a missing OID")
	}
}

// processPluginMonitor must skip (nil state) instead of publishing 0 when a
// plugin reports ErrNoSample, or fails with NoValueOnError set.
func TestProcessPluginMonitor_SkipWithoutValue(t *testing.T) {
	queryErr := fmt.Errorf("request timeout")
	PluginsRegistry["test_no_sample"] = &PluginDef{Func: func(context.Context, *SafeSensor, *Monitor) (string, error) {
		return "", fmt.Errorf("%w: first reading", ErrNoSample)
	}}
	PluginsRegistry["test_no_value_on_error"] = &PluginDef{NoValueOnError: true, Func: func(context.Context, *SafeSensor, *Monitor) (string, error) {
		return "", queryErr
	}}
	PluginsRegistry["test_zero_on_error"] = &PluginDef{Func: func(context.Context, *SafeSensor, *Monitor) (string, error) {
		return "", queryErr
	}}
	defer func() {
		delete(PluginsRegistry, "test_no_sample")
		delete(PluginsRegistry, "test_no_value_on_error")
		delete(PluginsRegistry, "test_zero_on_error")
	}()

	ss := &SafeSensor{Sensor: &Sensor{SensorName: "s"}}

	state, err := processPluginMonitor(context.Background(), ss, &Monitor{Name: "a", Plugin: "test_no_sample"})
	if state != nil || err != nil {
		t.Errorf("ErrNoSample: expected nil state and nil error, got %v / %v", state, err)
	}

	state, err = processPluginMonitor(context.Background(), ss, &Monitor{Name: "b", Plugin: "test_no_value_on_error"})
	if state != nil || !errors.Is(err, queryErr) {
		t.Errorf("NoValueOnError: expected nil state and the query error, got %v / %v", state, err)
	}

	// Unchanged behaviour for the other plugins: placeholder 0 plus the error.
	state, err = processPluginMonitor(context.Background(), ss, &Monitor{Name: "c", Plugin: "test_zero_on_error"})
	if state == nil || state.Scalar == nil || state.Scalar.Value != 0 || !errors.Is(err, queryErr) {
		t.Errorf("default: expected 0 and the query error, got %v / %v", state, err)
	}
}

// End to end through ProcessSensor: nothing is sent for a monitor whose
// plugin has no sample, and the sensor is not counted as failing.
func TestProcessSensor_NoSampleNotSentNorFailed(t *testing.T) {
	PluginsRegistry["test_no_sample_e2e"] = &PluginDef{Func: func(context.Context, *SafeSensor, *Monitor) (string, error) {
		return "", ErrNoSample
	}}
	defer delete(PluginsRegistry, "test_no_sample_e2e")

	ss := NewSafeSensor(&Sensor{
		SensorName: "s",
		Timeout:    1000,
		Monitors:   []Monitor{{Name: "rate", Plugin: "test_no_sample_e2e"}},
	})
	out := make(chan Metric, 10)

	for i := 0; i < MaxSNMPFails+1; i++ {
		ProcessSensor(context.Background(), ss, out)
	}

	if len(out) != 0 {
		t.Errorf("expected no metrics, got %d", len(out))
	}
	if ss.GetConsecutiveFails() != 0 || ss.IsLowPriority() {
		t.Errorf("sensor must not be counted as failing (fails=%d, low=%v)", ss.GetConsecutiveFails(), ss.IsLowPriority())
	}
}
