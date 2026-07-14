package monitor

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestStats_Counters(t *testing.T) {
	initial := GetStats()

	// Set total workers
	SetTotalWorkers(5)
	
	// Reset/Set active workers
	IncActiveWorkers()
	IncActiveWorkers()
	DecActiveWorkers()

	// Increment other counters
	IncSensorsProcessed()
	IncMetricsGenerated()
	IncMetricsSentStdout()
	IncMetricsSentKafka()
	IncMetricsFailedKafka()
	IncMetricsSentHTTP()
	IncMetricsFailedHTTP()

	stats := GetStats()

	if stats.TotalWorkers != 5 {
		t.Errorf("expected TotalWorkers to be 5, got %d", stats.TotalWorkers)
	}
	if stats.ActiveWorkers != initial.ActiveWorkers+1 {
		t.Errorf("expected ActiveWorkers to change by +1, got %d vs %d", stats.ActiveWorkers, initial.ActiveWorkers)
	}
	if stats.SensorsProcessed != initial.SensorsProcessed+1 {
		t.Errorf("expected SensorsProcessed to change by +1, got %d vs %d", stats.SensorsProcessed, initial.SensorsProcessed)
	}
	if stats.MetricsGenerated != initial.MetricsGenerated+1 {
		t.Errorf("expected MetricsGenerated to change by +1, got %d vs %d", stats.MetricsGenerated, initial.MetricsGenerated)
	}
	if stats.MetricsSentStdout != initial.MetricsSentStdout+1 {
		t.Errorf("expected MetricsSentStdout to change by +1, got %d vs %d", stats.MetricsSentStdout, initial.MetricsSentStdout)
	}
	if stats.MetricsSentKafka != initial.MetricsSentKafka+1 {
		t.Errorf("expected MetricsSentKafka to change by +1, got %d vs %d", stats.MetricsSentKafka, initial.MetricsSentKafka)
	}
	if stats.MetricsFailedKafka != initial.MetricsFailedKafka+1 {
		t.Errorf("expected MetricsFailedKafka to change by +1, got %d vs %d", stats.MetricsFailedKafka, initial.MetricsFailedKafka)
	}
	if stats.MetricsSentHTTP != initial.MetricsSentHTTP+1 {
		t.Errorf("expected MetricsSentHTTP to change by +1, got %d vs %d", stats.MetricsSentHTTP, initial.MetricsSentHTTP)
	}
	if stats.MetricsFailedHTTP != initial.MetricsFailedHTTP+1 {
		t.Errorf("expected MetricsFailedHTTP to change by +1, got %d vs %d", stats.MetricsFailedHTTP, initial.MetricsFailedHTTP)
	}
}

func TestIPCServer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_ipc_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	socketPath := filepath.Join(tmpDir, "test_status.sock")

	listener, err := StartIPCServer(socketPath)
	if err != nil {
		t.Fatalf("failed to start IPC status server: %v", err)
	}
	defer listener.Close()

	// 1. Query "stats"
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to dial IPC server: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("stats"))
	if err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var stats Stats
	err = json.Unmarshal(buf.Bytes(), &stats)
	if err != nil {
		t.Fatalf("failed to unmarshal JSON response: %v\nResponse: %s", err, buf.String())
	}

	// 2. Query unknown command
	conn2, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to dial IPC server second time: %v", err)
	}
	defer conn2.Close()

	_, err = conn2.Write([]byte("invalid_command_here"))
	if err != nil {
		t.Fatalf("failed to write invalid request: %v", err)
	}

	var buf2 bytes.Buffer
	_, err = io.Copy(&buf2, conn2)
	if err != nil {
		t.Fatalf("failed to read invalid response: %v", err)
	}

	var errResp map[string]string
	err = json.Unmarshal(buf2.Bytes(), &errResp)
	if err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}

	if errResp["error"] != "unknown command" {
		t.Errorf("expected error 'unknown command', got '%s'", errResp["error"])
	}
}

func TestIPCServer_TCP(t *testing.T) {
	listener, err := StartIPCServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start TCP IPC server: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial TCP IPC server: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("stats"))
	if err != nil {
		t.Fatalf("failed to write TCP request: %v", err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, conn)
	if err != nil {
		t.Fatalf("failed to read TCP response: %v", err)
	}

	var stats Stats
	err = json.Unmarshal(buf.Bytes(), &stats)
	if err != nil {
		t.Fatalf("failed to unmarshal TCP JSON response: %v\nResponse: %s", err, buf.String())
	}
}

func TestIPCServer_Reset(t *testing.T) {
	IncSensorsProcessed()
	IncMetricsGenerated()
	IncSensorsSkipped()

	listener, err := StartIPCServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start TCP IPC server: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	_, _ = conn.Write([]byte("stats"))
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, conn)
	conn.Close()

	var stats Stats
	_ = json.Unmarshal(buf.Bytes(), &stats)
	if stats.SensorsProcessed == 0 {
		t.Errorf("expected SensorsProcessed to be non-zero before reset")
	}

	connReset, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial for reset: %v", err)
	}
	_, _ = connReset.Write([]byte("reset"))
	var bufReset bytes.Buffer
	_, _ = io.Copy(&bufReset, connReset)
	connReset.Close()

	var resp map[string]string
	_ = json.Unmarshal(bufReset.Bytes(), &resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status ok response, got: %s", bufReset.String())
	}

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	_, _ = conn2.Write([]byte("stats"))
	var buf2 bytes.Buffer
	_, _ = io.Copy(&buf2, conn2)
	conn2.Close()

	var stats2 Stats
	_ = json.Unmarshal(buf2.Bytes(), &stats2)
	if stats2.SensorsProcessed != 0 {
		t.Errorf("expected SensorsProcessed to be 0 after reset, got %d", stats2.SensorsProcessed)
	}
	if stats2.SensorsSkipped != 0 {
		t.Errorf("expected SensorsSkipped to be 0 after reset, got %d", stats2.SensorsSkipped)
	}
}
