package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
)

func TestGetParamHelpers(t *testing.T) {
	params := map[string]interface{}{
		"host":        "127.0.0.1",
		"count":       5,
		"interval":    "200",
		"float_count": 3.0,
	}

	if val := getStringParam(params, "host", "default"); val != "127.0.0.1" {
		t.Errorf("expected '127.0.0.1', got '%s'", val)
	}
	if val := getStringParam(params, "missing", "default"); val != "default" {
		t.Errorf("expected 'default', got '%s'", val)
	}

	if val := getIntParam(params, "count", 1); val != 5 {
		t.Errorf("expected 5, got %d", val)
	}
	if val := getIntParam(params, "interval", 1); val != 200 {
		t.Errorf("expected 200, got %d", val)
	}
	if val := getIntParam(params, "float_count", 1); val != 3 {
		t.Errorf("expected 3, got %d", val)
	}
	if val := getIntParam(params, "missing", 42); val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestPingPlugin_Invalid(t *testing.T) {
	ctx := context.Background()
	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorIP: "192.0.2.1", // Test-Net IP range, unreachable
			Timeout:  10,
		},
	}
	m := &Monitor{
		Name:   "ping_test",
		Plugin: "ping",
		Params: map[string]interface{}{
			"count":      1,
			"timeout_ms": 10,
			"privileged": false,
		},
	}

	// Unreachable IP should return 100.0% packet loss (or error out to 100.0)
	loss, err := PingPlugin(ctx, ss, m)
	if loss != "100.0" {
		t.Errorf("expected loss '100.0' for unreachable host, got '%s' (err: %v)", loss, err)
	}

	// Unreachable IP should return 0.0 latency on failure
	m2 := &Monitor{
		Name:   "ping_latency_test",
		Plugin: "ping",
		Params: map[string]interface{}{
			"count":      1,
			"timeout_ms": 10,
			"metric":     "latency",
			"privileged": false,
		},
	}
	latency, err2 := PingPlugin(ctx, ss, m2)
	if latency != "0.00" {
		t.Errorf("expected latency '0.00' for unreachable host, got '%s' (err: %v)", latency, err2)
	}
}

func TestSNMPPlugins_Failure(t *testing.T) {
	ctx := context.Background()
	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorIP:    "192.0.2.1",
			Community:   "public",
			SnmpVersion: "2c",
			Timeout:     50,
		},
	}

	mGet := &Monitor{
		Oid: "1.3.6.1.4.1.2021.10.1.3.1",
	}
	val, err := SNMPPlugin(ctx, ss, mGet)
	if err == nil {
		t.Error("expected error for SNMP GET on unreachable host, got nil")
	}
	if val != "0" {
		t.Errorf("expected default '0' on failure, got '%s'", val)
	}

	mWalk := &Monitor{
		Oid: "1.3.6.1.4.1.2021.10.1.3",
	}
	valWalk, err := SNMPWalkPlugin(ctx, ss, mWalk)
	if err == nil {
		t.Error("expected error for SNMP Walk on unreachable host, got nil")
	}
	if valWalk != "" {
		t.Errorf("expected empty string on walk failure, got '%s'", valWalk)
	}
}

func TestRedfishPlugin_Success(t *testing.T) {
	ctx := context.Background()

	// 1. Setup mock Redfish JSON server
	mockData := map[string]interface{}{
		"Temperatures": []map[string]interface{}{
			{
				"Name":           "CPU Temp",
				"ReadingCelsius": 42.5,
			},
		},
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		// Verify Basic Auth headers
		username, password, ok := r.BasicAuth()
		if !ok || username != "admin" || password != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockData)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(handler))
	defer server.Close()

	// Extract host port from TLSServer URL (strip https://)
	hostPort := server.URL[8:]

	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorIP: hostPort,
		},
	}
	m := &Monitor{
		Plugin: "redfish",
		Params: map[string]interface{}{
			"path":      "/redfish/v1/Chassis/System/Thermal",
			"json_path": "Temperatures.0.ReadingCelsius",
			"username":  "admin",
			"password":  "secret",
		},
	}

	val, err := RedfishPlugin(ctx, ss, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "42.5" {
		t.Errorf("expected extracted temp '42.5', got '%s'", val)
	}
}

func TestGovcPlugin_Failure(t *testing.T) {
	ctx := context.Background()
	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorIP: "127.0.0.1:1",
		},
	}
	m := &Monitor{
		Plugin: "govc",
		Params: map[string]interface{}{
			"username": "admin",
			"password": "password",
			"target":   "nonexistent-vm",
		},
	}

	val, err := GovcPlugin(ctx, ss, m)
	if err == nil {
		t.Error("expected connection error for govc on unreachable host, got nil")
	}
	if val != "0" {
		t.Errorf("expected default '0' on failure, got '%s'", val)
	}
}

func TestIpmiPlugin_Failure(t *testing.T) {
	ctx := context.Background()
	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorIP: "127.0.0.1:1",
		},
	}
	m := &Monitor{
		Plugin: "ipmi",
		Params: map[string]interface{}{
			"username": "admin",
			"password": "password",
			"command":  "chassis_status",
		},
	}

	val, err := IpmiPlugin(ctx, ss, m)
	if err == nil {
		t.Error("expected connection error for ipmi on unreachable host, got nil")
	}
	if val != "0" {
		t.Errorf("expected default '0' on failure, got '%s'", val)
	}
}

type mockRoundTripper struct{}

func (m *mockRoundTripper) RoundTrip(ctx context.Context, req, res soap.HasFault) error {
	return fmt.Errorf("mock roundtrip error")
}

func TestGovcPlugin_Fallback(t *testing.T) {
	ctx := context.Background()

	// Create a SafeSensor with govc credentials and a sensor name
	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorName:   "my-fallback-vm-target",
			SensorIP:     "127.0.0.1",
			GovcUsername: "fallback-user",
			GovcPassword: "fallback-password",
		},
	}

	// Create a Monitor without credentials and without a target, targeting vm
	m := &Monitor{
		Plugin: "govc",
		Params: map[string]interface{}{
			"target_type": "vm",
		},
	}

	// Mock the cache!
	// sessionKey in GovcPlugin is fmt.Sprintf("%s:%s", host, username)
	// host is ss.Sensor.SensorIP ("127.0.0.1"), username is ss.Sensor.GovcUsername ("fallback-user")
	sessionKey := "127.0.0.1:fallback-user"

	// Construct a dummy cachedGovcClient with a non-nil client and a dummy vim25.Client
	mockClient := &govmomi.Client{
		Client: &vim25.Client{
			RoundTripper: &mockRoundTripper{},
			ServiceContent: types.ServiceContent{
				About: types.AboutInfo{
					ApiType: "VirtualCenter",
				},
				SearchIndex:       &types.ManagedObjectReference{},
				PropertyCollector: types.ManagedObjectReference{},
				RootFolder:        types.ManagedObjectReference{},
			},
		},
	}

	govcCacheMu.Lock()
	govcSessionCache[sessionKey] = &cachedGovcClient{
		client:      mockClient,
		resolvedVMs: make(map[string]*object.VirtualMachine),
		resolvedDC:  make(map[string]*object.Datacenter),
	}
	govcCacheMu.Unlock()

	// Ensure cleanup
	defer func() {
		govcCacheMu.Lock()
		delete(govcSessionCache, sessionKey)
		govcCacheMu.Unlock()
	}()

	// Call GovcPlugin
	_, err := GovcPlugin(ctx, ss, m)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	expectedErrPart := "vm 'my-fallback-vm-target' not found"
	if !strings.Contains(err.Error(), expectedErrPart) {
		t.Errorf("expected error containing %q, got %q", expectedErrPart, err.Error())
	}
}

func TestGovcPlugin_FallbackConnectionRefused(t *testing.T) {
	ctx := context.Background()

	// Create a SafeSensor with govc credentials and a closed port
	ss := &SafeSensor{
		Sensor: &Sensor{
			SensorName:   "my-fallback-vm-target",
			SensorIP:     "127.0.0.1:1", // closed port
			GovcUsername: "fallback-user",
			GovcPassword: "fallback-password",
		},
	}

	// Create a Monitor without credentials and without a target, targeting vm
	m := &Monitor{
		Plugin: "govc",
		Params: map[string]interface{}{
			"target_type": "vm",
		},
	}

	// Call GovcPlugin
	_, err := GovcPlugin(ctx, ss, m)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Since we didn't mock the cache, it should attempt to connect and fail with connection refused
	expectedErrPart := "failed to connect to VMware"
	if !strings.Contains(err.Error(), expectedErrPart) {
		t.Errorf("expected error containing %q, got %q", expectedErrPart, err.Error())
	}
}


