package main

import (
	"flag"
	"os"
	"sync/atomic"
	"testing"

	"redborder-monitor/internal/monitor"
)

func TestConfigReload(t *testing.T) {
	// Setup initial safeSensors
	sensorsMu.Lock()
	safeSensors = []*monitor.SafeSensor{
		monitor.NewSafeSensor(&monitor.Sensor{SensorName: "sensor-A", Timeout: 1000}),
		monitor.NewSafeSensor(&monitor.Sensor{SensorName: "sensor-B", Timeout: 2000}),
	}
	safeSensors[0].SetLowPriority(true) // Mark sensor-A as low priority
	safeSensors[0].Cache["test"] = &monitor.MonitorState{
		Scalar: &monitor.MonitorValue{Value: 42.0},
	}
	sensorsMu.Unlock()

	// Write a temporary new config file
	tempFile, err := os.CreateTemp("", "test_reload_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	configJSON := `{
		"conf": {
			"debug": 3,
			"threads": 5,
			"timeout": 10
		},
		"sensors": [
			{
				"sensor_name": "sensor-A",
				"timeout": 1000
			},
			{
				"sensor_name": "sensor-C",
				"timeout": 3000
			}
		]
	}`
	if _, err := tempFile.Write([]byte(configJSON)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close()

	// Perform reload
	reloadConfig(tempFile.Name())

	sensorsMu.RLock()
	defer sensorsMu.RUnlock()

	if len(safeSensors) != 2 {
		t.Errorf("Expected 2 safe sensors after reload, got %d", len(safeSensors))
	}

	// Verify sensor-A (existing) kept its priority state and cache!
	var ssA *monitor.SafeSensor
	for _, ss := range safeSensors {
		if ss.Sensor.SensorName == "sensor-A" {
			ssA = ss
		}
	}
	if ssA == nil {
		t.Fatalf("sensor-A was not preserved in reload")
	}
	if !ssA.IsLowPriority() {
		t.Errorf("sensor-A lost its low priority status in reload")
	}
	if state, exists := ssA.Cache["test"]; !exists || state.Scalar == nil || state.Scalar.Value != 42.0 {
		t.Errorf("sensor-A lost its cache state in reload")
	}

	// Verify global config values updated
	if config.Conf.Threads != 5 {
		t.Errorf("Expected threads to be updated to 5, got %d", config.Conf.Threads)
	}
	if int(atomic.LoadInt32(&monitor.AtomicDebugLevel)) != 3 {
		t.Errorf("Expected debug level to be updated to 3, got %d", atomic.LoadInt32(&monitor.AtomicDebugLevel))
	}
}

func TestFlagsRegistered(t *testing.T) {
	flagPairs := []struct {
		short string
		long  string
	}{
		{"c", "config"},
		{"d", "debug"},
		{"g", "daemon"},
		{"h", "help"},
		{"i", "info"},
		{"r", "reset"},
		{"s", "search-mibs"},
		{"t", "status"},
		{"v", "version"},
	}

	for _, pair := range flagPairs {
		fShort := flag.Lookup(pair.short)
		if fShort == nil {
			t.Errorf("Expected short flag -%s to be registered, but it was not", pair.short)
		}
		fLong := flag.Lookup(pair.long)
		if fLong == nil {
			t.Errorf("Expected long flag --%s to be registered, but it was not", pair.long)
		}
	}
}
