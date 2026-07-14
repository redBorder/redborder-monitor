package monitor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	ipmi "github.com/bougou/go-ipmi"
	probing "github.com/prometheus-community/pro-bing"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

// PluginFunc defines the signature of a native plugin.
type PluginFunc func(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error)

// PluginParam defines metadata for a single plugin parameter.
type PluginParam struct {
	Name        string
	Type        string
	Default     string
	Description string
}

// PluginDef defines the complete metadata and execution function for a plugin.
type PluginDef struct {
	Func        PluginFunc
	Description string
	Params      []PluginParam
}

// PluginsRegistry registers all available in-memory plugins by name.
var PluginsRegistry = map[string]*PluginDef{
	"ping": {
		Func:        PingPlugin,
		Description: "Measures packet loss percentage or average round-trip latency (RTT).",
		Params: []PluginParam{
			{Name: "host", Type: "string", Default: "parent sensor's IP", Description: "Target host/IP"},
			{Name: "count", Type: "int", Default: "3", Description: "Number of ping packets to send"},
			{Name: "interval_ms", Type: "int", Default: "100", Description: "Delay in milliseconds between ping packets"},
			{Name: "timeout_ms", Type: "int", Default: "sensor's timeout", Description: "Total timeout in milliseconds"},
			{Name: "metric", Type: "string", Default: "\"packet_loss\"", Description: "Target metric: \"packet_loss\" or \"latency\" / \"rtt\""},
			{Name: "size", Type: "int", Default: "56", Description: "ICMP packet payload size in bytes"},
			{Name: "privileged", Type: "bool", Default: "false", Description: "Use privileged raw socket ping"},
		},
	},
	"snmp": {
		Func:        SNMPPlugin,
		Description: "Queries a single SNMP OID using the sensor's credentials.",
		Params: []PluginParam{
			{Name: "oid", Type: "string", Default: "monitor's \"oid\" field", Description: "SNMP Object Identifier"},
		},
	},
	"snmp_walk": {
		Func:        SNMPWalkPlugin,
		Description: "Performs an SNMP BulkWalk and returns values joined by semicolon (\";\").",
		Params: []PluginParam{
			{Name: "oid", Type: "string", Default: "monitor's \"oid\" field", Description: "SNMP Object Identifier to walk"},
		},
	},
	"govc": {
		Func:        GovcPlugin,
		Description: "Queries VM or ESXi host metrics from VMware vCenter/ESXi.",
		Params: []PluginParam{
			{Name: "host", Type: "string", Default: "parent sensor's IP", Description: "VMware vCenter/ESXi host IP/hostname"},
			{Name: "username", Type: "string", Default: "parent sensor's govc_username", Description: "VMware Basic Auth username"},
			{Name: "password", Type: "string", Default: "parent sensor's govc_password", Description: "VMware Basic Auth password"},
			{Name: "target", Type: "string", Default: "parent sensor's name", Description: "Name of the target VM or ESXi host"},
			{Name: "target_type", Type: "string", Default: "\"vm\"", Description: "Type of target: \"vm\" or \"host\""},
			{Name: "datacenter", Type: "string", Default: "none", Description: "Name of the target VMware datacenter (optional)"},
			{Name: "disk_path", Type: "string", Default: "none", Description: "Mount path/drive letter for VM disk queries (optional)"},
			{Name: "metric", Type: "string", Default: "\"power_state\"", Description: "Metric to query. For \"vm\": power_state, cpu_usage, cpu_usage_mhz, memory_usage, memory_usage_mb, num_cpu, memory_size_mb, heartbeat_status, ip, disk_usage. For \"host\": power_state, connection_state, cpu_usage, cpu_usage_mhz, memory_usage, memory_usage_mb, memory_size, disk_usage."},
		},
	},
	"redfish": {
		Func:        RedfishPlugin,
		Description: "Performs HTTP REST queries to OOB controllers (iDRAC, iLO, etc.) and extracts values using JSON dot-paths.",
		Params: []PluginParam{
			{Name: "host", Type: "string", Default: "parent sensor's IP", Description: "Target IP/hostname"},
			{Name: "path", Type: "string", Default: "\"/redfish/v1/\"", Description: "HTTP REST API path"},
			{Name: "json_path", Type: "string", Default: "none", Description: "Dot-notation path to extract from the JSON response (e.g. \"Temperatures.0.ReadingCelsius\")"},
			{Name: "username", Type: "string", Default: "none", Description: "REST API Basic Auth username"},
			{Name: "password", Type: "string", Default: "none", Description: "REST API Basic Auth password"},
		},
	},
	"ipmi": {
		Func:        IpmiPlugin,
		Description: "Queries hardware sensors and chassis status over IPMI UDP.",
		Params: []PluginParam{
			{Name: "host", Type: "string", Default: "parent sensor's IP", Description: "Target IP/hostname"},
			{Name: "command", Type: "string", Default: "\"chassis_status\"", Description: "IPMI command: \"chassis_status\", \"power_status\", or \"sdr\" / \"sensors\""},
			{Name: "port", Type: "int", Default: "623", Description: "IPMI UDP port"},
			{Name: "username", Type: "string", Default: "none", Description: "IPMI username"},
			{Name: "password", Type: "string", Default: "none", Description: "IPMI password"},
			{Name: "sensor_name", Type: "string", Default: "none", Description: "Specific sensor to query from SDR (only applicable when command is \"sdr\" or \"sensors\")"},
		},
	},
}

// Helper functions for parameter extraction

func getStringParam(params map[string]interface{}, key string, def string) string {
	if params == nil {
		return def
	}
	if val, ok := params[key]; ok {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return def
}

func getIntParam(params map[string]interface{}, key string, def int) int {
	if params == nil {
		return def
	}
	if val, ok := params[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case float64:
			return int(v)
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		}
	}
	return def
}

// 1. Ping / Fping Plugin
// Params:
//   "host" (string): target host (defaults to sensor IP)
//   "count" (int): number of pings (defaults to 3)
//   "interval_ms" (int): interval between pings (defaults to 100)
//   "timeout_ms" (int): total timeout (defaults to sensor timeout)
//   "metric" (string): "packet_loss" or "latency" / "rtt" (defaults to "packet_loss")
// Returns: percentage of packets lost (float string, e.g. "0.0") or average RTT in ms (e.g. "2.5")
func PingPlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	host := getStringParam(m.Params, "host", ss.Sensor.SensorIP)
	metric := getStringParam(m.Params, "metric", "packet_loss")

	defaultErrVal := "100.0"
	if strings.ToLower(metric) == "latency" || strings.ToLower(metric) == "rtt" {
		defaultErrVal = "0.00"
	}

	if host == "" {
		return defaultErrVal, fmt.Errorf("missing target host for ping plugin")
	}

	count := getIntParam(m.Params, "count", 3)
	intervalMs := getIntParam(m.Params, "interval_ms", 100)
	timeoutMs := getIntParam(m.Params, "timeout_ms", ss.Sensor.Timeout)

	pinger, err := probing.NewPinger(host)
	if err != nil {
		return defaultErrVal, err
	}

	pinger.Count = count
	pinger.Interval = time.Duration(intervalMs) * time.Millisecond
	pinger.Timeout = time.Duration(timeoutMs) * time.Millisecond
	pinger.Size = getIntParam(m.Params, "size", 56)

	privileged := false
	if val, ok := m.Params["privileged"]; ok {
		if b, ok := val.(bool); ok {
			privileged = b
		} else if s, ok := val.(string); ok {
			privileged = (strings.ToLower(s) == "true" || s == "1")
		} else if f, ok := val.(float64); ok {
			privileged = (f != 0)
		} else if i, ok := val.(int); ok {
			privileged = (i != 0)
		}
	}

	pinger.SetPrivileged(privileged)

	// Run with context support
	done := make(chan error, 1)
	go func() {
		done <- pinger.Run()
	}()

	select {
	case <-ctx.Done():
		pinger.Stop()
		return defaultErrVal, ctx.Err()
	case err := <-done:
		if err != nil {
			return defaultErrVal, err
		}
	}

	stats := pinger.Statistics()
	if strings.ToLower(metric) == "latency" || strings.ToLower(metric) == "rtt" {
		rttMs := float64(stats.AvgRtt) / float64(time.Millisecond)
		return fmt.Sprintf("%.2f", rttMs), nil
	}
	return fmt.Sprintf("%.1f", stats.PacketLoss), nil
}

// 2. SNMP Plugin (Single OID GET query wrapper)
// Params:
//   "oid" (string): OID to query
func SNMPPlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	oid := getStringParam(m.Params, "oid", m.Oid)
	if oid == "" {
		return "0", fmt.Errorf("missing OID for SNMP plugin")
	}

	outStr, _, err := SolveSNMPQuery(
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
	return outStr, err
}

// 3. SNMP Walk Plugin
// Params:
//   "oid" (string): OID to walk
func SNMPWalkPlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	oid := getStringParam(m.Params, "oid", m.Oid)
	if oid == "" {
		return "", fmt.Errorf("missing OID for SNMP walk plugin")
	}

	return SolveSNMPWalk(
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
}

// 4. Govc / VMware vSphere Plugin (Concrete implementation using govmomi)
type cachedGovcClient struct {
	client        *govmomi.Client
	resolvedHosts map[string]*object.HostSystem
	resolvedVMs   map[string]*object.VirtualMachine
	resolvedDC    map[string]*object.Datacenter
	mu            sync.RWMutex
}

var (
	govcSessionCache = make(map[string]*cachedGovcClient)
	govcCacheMu      sync.Mutex
)

func getVMwareClient(ctx context.Context, host, username, password string) (*cachedGovcClient, error) {
	govcCacheMu.Lock()
	defer govcCacheMu.Unlock()

	sessionKey := fmt.Sprintf("%s:%s", host, username)
	clientWrapper, exists := govcSessionCache[sessionKey]

	if exists && clientWrapper != nil {
		return clientWrapper, nil
	}

	u := &url.URL{
		Scheme: "https",
		User:   url.UserPassword(username, password),
		Host:   host,
		Path:   "/sdk",
	}

	newClient, err := govmomi.NewClient(ctx, u, true)
	if err != nil {
		return nil, err
	}

	clientWrapper = &cachedGovcClient{
		client:        newClient,
		resolvedHosts: make(map[string]*object.HostSystem),
		resolvedVMs:   make(map[string]*object.VirtualMachine),
		resolvedDC:    make(map[string]*object.Datacenter),
	}

	govcSessionCache[sessionKey] = clientWrapper
	return clientWrapper, nil
}

func GovcPlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	host := getStringParam(m.Params, "host", ss.Sensor.SensorIP)
	username := getStringParam(m.Params, "username", ss.Sensor.GovcUsername)

	for attempt := 1; attempt <= 2; attempt++ {
		val, err := govcPluginInternal(ctx, ss, m)
		if err != nil {
			errStr := err.Error()
			if (strings.Contains(errStr, "NotAuthenticated") || strings.Contains(errStr, "session") || strings.Contains(errStr, "unauthorized") || strings.Contains(errStr, "login") || strings.Contains(errStr, "EOF") || strings.Contains(errStr, "connection") || strings.Contains(errStr, "broken pipe") || strings.Contains(errStr, "reset")) && attempt == 1 {
				govcCacheMu.Lock()
				sessionKey := fmt.Sprintf("%s:%s", host, username)
				delete(govcSessionCache, sessionKey)
				govcCacheMu.Unlock()
				continue
			}
			return val, err
		}
		return val, nil
	}
	return "0", fmt.Errorf("govc plugin failed after retry")
}

func govcPluginInternal(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	host := getStringParam(m.Params, "host", ss.Sensor.SensorIP)
	username := getStringParam(m.Params, "username", ss.Sensor.GovcUsername)
	password := getStringParam(m.Params, "password", ss.Sensor.GovcPassword)
	target := getStringParam(m.Params, "target", "")
	metric := getStringParam(m.Params, "metric", "power_state")
	targetType := getStringParam(m.Params, "target_type", "vm")
	datacenter := getStringParam(m.Params, "datacenter", "")

	if host == "" || username == "" || password == "" {
		return "0", fmt.Errorf("missing credentials/host for govc plugin")
	}

	clientWrapper, err := getVMwareClient(ctx, host, username, password)
	if err != nil {
		return "0", fmt.Errorf("failed to connect to VMware: %v", err)
	}

	if target == "" {
		if strings.ToLower(targetType) == "host" && !clientWrapper.client.IsVC() {
			target = "*"
		} else if strings.ToLower(targetType) == "vm" {
			target = ss.Sensor.SensorName
		} else {
			return "0", fmt.Errorf("missing target name for govc plugin")
		}
	}

	if !clientWrapper.client.IsVC() {
		if datacenter == "" || datacenter == "ha-datacenter" {
			datacenter = "/ha-datacenter"
		}
	}

	var dc *object.Datacenter
	clientWrapper.mu.RLock()
	dc = clientWrapper.resolvedDC[datacenter]
	clientWrapper.mu.RUnlock()

	if dc == nil {
		finder := find.NewFinder(clientWrapper.client.Client, true)
		if datacenter != "" {
			var lookupErr error
			dc, lookupErr = finder.Datacenter(ctx, datacenter)
			if lookupErr != nil {
				return "0", fmt.Errorf("datacenter '%s' not found: %v", datacenter, lookupErr)
			}
		} else {
			if dcDefault, errDefault := finder.DefaultDatacenter(ctx); errDefault == nil {
				dc = dcDefault
			}
		}
		if dc != nil {
			clientWrapper.mu.Lock()
			clientWrapper.resolvedDC[datacenter] = dc
			clientWrapper.mu.Unlock()
		}
	}

	finder := find.NewFinder(clientWrapper.client.Client, true)
	if dc != nil {
		finder.SetDatacenter(dc)
	} else {
		finder.SetDatacenter(nil)
	}

	if strings.ToLower(targetType) == "host" {
		var hs *object.HostSystem
		clientWrapper.mu.RLock()
		hs = clientWrapper.resolvedHosts[target]
		clientWrapper.mu.RUnlock()

		if hs == nil {
			var lookupErr error
			hs, lookupErr = finder.HostSystem(ctx, target)
			if lookupErr != nil {
				if !clientWrapper.client.IsVC() {
					if hosts, werr := finder.HostSystemList(ctx, "*"); werr == nil && len(hosts) == 1 {
						hs = hosts[0]
						lookupErr = nil
					}
				}
			}
			if lookupErr != nil {
				return "0", fmt.Errorf("host '%s' not found: %v", target, lookupErr)
			}
			clientWrapper.mu.Lock()
			clientWrapper.resolvedHosts[target] = hs
			clientWrapper.mu.Unlock()
		}

		var props mo.HostSystem
		propsList := []string{"summary"}
		if strings.ToLower(metric) == "disk_usage" {
			propsList = append(propsList, "datastore")
		}
		err = hs.Properties(ctx, hs.Reference(), propsList, &props)
		if err != nil {
			return "0", fmt.Errorf("failed to fetch host properties: %v", err)
		}

		switch strings.ToLower(metric) {
		case "power_state":
			return string(props.Summary.Runtime.PowerState), nil
		case "connection_state":
			return string(props.Summary.Runtime.ConnectionState), nil
		case "cpu_usage":
			overallCpuUsage := int64(props.Summary.QuickStats.OverallCpuUsage)
			
			var cpuMhz, numCores int64
			if props.Summary.Hardware != nil {
				cpuMhz = int64(props.Summary.Hardware.CpuMhz)
				numCores = int64(props.Summary.Hardware.NumCpuCores)
			}
			if cpuMhz == 0 || numCores == 0 {
				return "0", fmt.Errorf("host CPU specs (CpuMhz=%d, NumCpuCores=%d) are missing or 0", cpuMhz, numCores)
			}
			
			totalCapacityMhz := cpuMhz * numCores
			percent := (float64(overallCpuUsage) / float64(totalCapacityMhz)) * 100
			return fmt.Sprintf("%.2f", percent), nil
		case "cpu_usage_mhz", "cpu_mhz":
			overallCpuUsage := int64(props.Summary.QuickStats.OverallCpuUsage)
			return strconv.FormatInt(overallCpuUsage, 10), nil
		case "memory_usage":
			overallMemoryUsage := int64(props.Summary.QuickStats.OverallMemoryUsage)
			
			var totalMemoryBytes int64
			if props.Summary.Hardware != nil {
				totalMemoryBytes = props.Summary.Hardware.MemorySize
			}
			if totalMemoryBytes == 0 {
				return "0", fmt.Errorf("host memory size is missing or 0")
			}
			
			totalMemoryMb := float64(totalMemoryBytes) / (1024 * 1024)
			percent := (float64(overallMemoryUsage) / totalMemoryMb) * 100
			return fmt.Sprintf("%.2f", percent), nil
		case "memory_usage_mb", "memory_mb":
			overallMemoryUsage := int64(props.Summary.QuickStats.OverallMemoryUsage)
			return strconv.FormatInt(overallMemoryUsage, 10), nil
		case "memory_size":
			if props.Summary.Hardware != nil {
				return strconv.FormatInt(props.Summary.Hardware.MemorySize, 10), nil
			}
			return "0", fmt.Errorf("host hardware summary is missing")
		case "disk_usage":
			if len(props.Datastore) == 0 {
				return "0", nil
			}
			var totalCapacity, totalFree int64
			for _, dsRef := range props.Datastore {
				var dsProps mo.Datastore
				err = hs.Properties(ctx, dsRef, []string{"summary"}, &dsProps)
				if err == nil {
					totalCapacity += dsProps.Summary.Capacity
					totalFree += dsProps.Summary.FreeSpace
				}
			}
			if totalCapacity > 0 {
				used := totalCapacity - totalFree
				percent := (float64(used) / float64(totalCapacity)) * 100
				return fmt.Sprintf("%.2f", percent), nil
			}
			return "0", fmt.Errorf("no datastores found or total capacity is 0")
		default:
			return "0", fmt.Errorf("unknown host metric '%s'", metric)
		}
	} else {
		var vm *object.VirtualMachine
		clientWrapper.mu.RLock()
		vm = clientWrapper.resolvedVMs[target]
		clientWrapper.mu.RUnlock()

		if vm == nil {
			var lookupErr error
			vm, lookupErr = finder.VirtualMachine(ctx, target)
			if lookupErr != nil {
				return "0", fmt.Errorf("vm '%s' not found: %v", target, lookupErr)
			}
			clientWrapper.mu.Lock()
			clientWrapper.resolvedVMs[target] = vm
			clientWrapper.mu.Unlock()
		}

		var props mo.VirtualMachine
		propsList := []string{"summary"}
		metricLower := strings.ToLower(metric)
		if metricLower == "heartbeat_status" || metricLower == "ip" {
			propsList = []string{"guest"}
		} else if metricLower == "disk_usage" {
			propsList = []string{"guest", "summary"}
		} else if metricLower == "cpu_usage" {
			propsList = []string{"summary", "runtime"}
		}
		err = vm.Properties(ctx, vm.Reference(), propsList, &props)
		if err != nil {
			return "0", fmt.Errorf("failed to fetch vm properties: %v", err)
		}

		switch strings.ToLower(metric) {
		case "power_state":
			return string(props.Summary.Runtime.PowerState), nil
		case "cpu_usage":
			overallCpuUsage := int64(props.Summary.QuickStats.OverallCpuUsage)
			numCpu := int64(props.Summary.Config.NumCpu)
			hostRef := props.Summary.Runtime.Host
			if hostRef == nil && props.Runtime.Host != nil {
				hostRef = props.Runtime.Host
			}
			if numCpu <= 0 {
				return "0", fmt.Errorf("VM CPU count (NumCpu=%d) is missing or 0", numCpu)
			}
			if hostRef == nil {
				return "0", fmt.Errorf("VM host reference is missing")
			}
			var hostProps mo.HostSystem
			err = vm.Properties(ctx, *hostRef, []string{"summary"}, &hostProps)
			if err != nil {
				return "0", fmt.Errorf("failed to fetch VM host properties: %v", err)
			}
			var hostCpuMhz int64
			if hostProps.Summary.Hardware != nil {
				hostCpuMhz = int64(hostProps.Summary.Hardware.CpuMhz)
			}
			if hostCpuMhz == 0 {
				return "0", fmt.Errorf("VM host CPU frequency (CpuMhz) is missing or 0")
			}
			
			totalMhz := numCpu * hostCpuMhz
			percent := (float64(overallCpuUsage) / float64(totalMhz)) * 100
			return fmt.Sprintf("%.2f", percent), nil
		case "cpu_usage_mhz", "cpu_mhz":
			overallCpuUsage := int64(props.Summary.QuickStats.OverallCpuUsage)
			return strconv.FormatInt(overallCpuUsage, 10), nil
		case "memory_usage":
			guestMemoryUsage := int64(props.Summary.QuickStats.GuestMemoryUsage)
			totalMemoryMb := int64(props.Summary.Config.MemorySizeMB)
			if totalMemoryMb <= 0 {
				return "0", fmt.Errorf("VM memory size (MemorySizeMB=%d) is missing or 0", totalMemoryMb)
			}
			percent := (float64(guestMemoryUsage) / float64(totalMemoryMb)) * 100
			return fmt.Sprintf("%.2f", percent), nil
		case "memory_usage_mb", "memory_mb":
			guestMemoryUsage := int64(props.Summary.QuickStats.GuestMemoryUsage)
			return strconv.FormatInt(guestMemoryUsage, 10), nil
		case "num_cpu":
			return strconv.FormatInt(int64(props.Summary.Config.NumCpu), 10), nil
		case "memory_size_mb":
			return strconv.FormatInt(int64(props.Summary.Config.MemorySizeMB), 10), nil
		case "heartbeat_status":
			return string(props.GuestHeartbeatStatus), nil
		case "ip":
			return props.Guest.IpAddress, nil
		case "disk_usage":
			diskPath := getStringParam(m.Params, "disk_path", "")
			if len(props.Guest.Disk) == 0 {
				// Fallback: Use hypervisor-level storage metrics if guest info (VMware Tools) is not running
				committed := props.Summary.Storage.Committed
				uncommitted := props.Summary.Storage.Uncommitted
				total := committed + uncommitted
				if total > 0 {
					percent := (float64(committed) / float64(total)) * 100
					return fmt.Sprintf("%.2f", percent), nil
				}
				return "0", fmt.Errorf("no guest disk information available (VMware Tools not running) and datastore storage info is 0")
			}
			var selectedDisk *types.GuestDiskInfo
			if diskPath != "" {
				for i := range props.Guest.Disk {
					if strings.EqualFold(props.Guest.Disk[i].DiskPath, diskPath) {
						selectedDisk = &props.Guest.Disk[i]
						break
					}
				}
			}
			if selectedDisk == nil {
				selectedDisk = &props.Guest.Disk[0]
			}
			if selectedDisk.Capacity > 0 {
				used := selectedDisk.Capacity - selectedDisk.FreeSpace
				percent := (float64(used) / float64(selectedDisk.Capacity)) * 100
				return fmt.Sprintf("%.2f", percent), nil
			}
			return "0", nil
		default:
			return "0", fmt.Errorf("unknown vm metric '%s'", metric)
		}
	}
}

// 5. Redfish Plugin (Native HTTP API Query)
// Params:
//   "path" (string): REST endpoint (e.g. "/redfish/v1/Chassis/System.Embedded.1/Thermal")
//   "json_path" (string): dot-notation path to extract (e.g. "Temperatures.0.ReadingCelsius")
//   "username" (string): credential
//   "password" (string): credential
var redfishClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Hardware controllers often use self-signed certs
	},
}

func RedfishPlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	host := getStringParam(m.Params, "host", ss.Sensor.SensorIP)
	path := getStringParam(m.Params, "path", "/redfish/v1/")
	jsonPath := getStringParam(m.Params, "json_path", "")
	username := getStringParam(m.Params, "username", "")
	password := getStringParam(m.Params, "password", "")

	if host == "" {
		return "0", fmt.Errorf("missing host for Redfish plugin")
	}

	url := fmt.Sprintf("https://%s%s", host, path)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "0", err
	}
	req.Header.Set("Accept", "application/json")
	if username != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := redfishClient.Do(req)
	if err != nil {
		return "0", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "0", fmt.Errorf("redfish API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "0", err
	}

	if jsonPath == "" {
		return string(body), nil
	}

	// Simple extraction from JSON
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "0", err
	}

	// Quick dot-path lookup (supports maps and array slices)
	parts := strings.Split(jsonPath, ".")
	var current interface{} = data
	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else if slice, ok := current.([]interface{}); ok {
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(slice) {
				return "0", fmt.Errorf("array index '%s' invalid or out of bounds", part)
			}
			current = slice[idx]
		} else {
			return "0", fmt.Errorf("path key/index '%s' not found in Redfish JSON", part)
		}
	}

	return fmt.Sprintf("%v", current), nil
}

// 6. IPMI Plugin (Native IPMI over LAN)
// Params:
//   "command" (string): IPMI command, e.g., "sdr", "chassis_status", "power_status"
//   "port" (int): IPMI port (defaults to 623)
//   "username" (string): credential
//   "password" (string): credential
//   "sensor_name" (string): specific sensor to query (only for "sdr" / "sensors")
func getIPMIClient(ctx context.Context, host string, port int, username, password string) (*ipmi.Client, error) {
	client, err := ipmi.NewClient(host, port, username, password)
	if err != nil {
		return nil, err
	}
	client.Interface = "lanplus" // default to lanplus (RMCP+)

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func IpmiPlugin(ctx context.Context, ss *SafeSensor, m *Monitor) (string, error) {
	host := getStringParam(m.Params, "host", ss.Sensor.SensorIP)
	command := getStringParam(m.Params, "command", "chassis_status")
	port := getIntParam(m.Params, "port", 623)
	username := getStringParam(m.Params, "username", "")
	password := getStringParam(m.Params, "password", "")
	sensorName := getStringParam(m.Params, "sensor_name", "")

	if host == "" {
		return "0", fmt.Errorf("missing host for ipmi plugin")
	}

	client, err := getIPMIClient(ctx, host, port, username, password)
	if err != nil {
		return "0", err
	}
	defer client.Close(ctx)

	switch strings.ToLower(command) {
	case "chassis_status", "chassis power status", "power_status":
		status, err := client.GetChassisStatus(ctx)
		if err != nil {
			return "0", err
		}
		if status.PowerIsOn {
			return "on", nil
		}
		return "off", nil

	case "sdr", "sensors":
		sensors, err := client.GetSensors(ctx)
		if err != nil {
			return "0", err
		}

		if sensorName != "" {
			for _, s := range sensors {
				if strings.EqualFold(s.Name, sensorName) {
					if s.HasAnalogReading {
						return fmt.Sprintf("%.2f", s.Value), nil
					}
					return s.ReadingStr(), nil
				}
			}
			return "0", fmt.Errorf("sensor '%s' not found", sensorName)
		}

		var results []string
		for _, s := range sensors {
			if s.HasAnalogReading {
				results = append(results, fmt.Sprintf("%s:%.2f", s.Name, s.Value))
			} else {
				results = append(results, fmt.Sprintf("%s:%s", s.Name, s.ReadingStr()))
			}
		}
		return strings.Join(results, ";"), nil

	default:
		return "ok", nil
	}
}
