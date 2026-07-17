# redborder-monitor

`rbredborder-monitor` is a concurrent, high-performance telemetry collection and device monitoring daemon. It periodically polls system statistics and network devices using SNMP, native plugins, or shell commands, processes and aggregates the collected data, and routes the metrics to Stdout, Apache Kafka, or an HTTP POST endpoint.

Designed for high-concurrency environments, it schedules and executes polls asynchronously while protecting healthy queries from being starved by slow or unresponsive devices.

---

## Key Features

* **Dynamic SNMP & MIB Loading**: Dynamically loads and parses standard or custom SNMP MIB text files at startup or on config reload. Textual OIDs (e.g. `IF-MIB::ifIndex.1` or `laLoad.1`) are resolved to numeric OIDs automatically without Net-SNMP dependencies.
* **Native In-Memory Plugins**: Employs statically-compiled, high-performance plugins (`ping`, `snmp_walk`, `govc` using govmomi SDK, `redfish` REST, `ipmi` over LAN), avoiding process fork overhead.
* **Math Expression Evaluator**: Safely evaluates infix math formulas (`op` parameter, e.g. `a + b`, `(x * 100) / y`) and resolves cross-group dependency graphs (using `_gid_<group_id>` syntax).
* **Vector Splits & Aggregations**: Splits vector telemetry streams via custom delimiters (`split`), handles explicit timestamps (`timestamp:value`), and calculates aggregates (`sum`, `mean`).
* **Self-Healing Priority Scheduling**: Automatically isolates slow or failing sensors to a low-priority queue to prevent poll starvation, returning them to high priority once they recover.
* **Hot Configuration Reloading**: Responds to `SIGHUP` by reloading configuration, rebuilding the MIB catalog, and updating active sensors without losing priority state or metric caches.

---

## Configuration Guide

The configuration is defined in a single JSON file consisting of two main blocks:
1. `conf`: Global daemon, output, and scheduler configurations.
2. `sensors`: Array of target devices/hosts to monitor, each containing a list of `monitors` (queries).

### Complete Configuration Example
Since the daemon automatically strips comments, you can include inline comments in your JSON config files as shown below:

```json
{
  "conf": {
    "debug": 6,                         // Severity filter: 0=Emerg, 3=Err, 6=Info, 7=Debug
    "stdout": 1,                        // 1 to output JSON metrics to stdout
    "syslog": 0,                        // 1 to log to syslog
    "threads": 10,                      // Number of parallel worker threads
    "timeout": 5,                       // Default query timeout in seconds
    "max_snmp_fails": 2,                // Fails before moving sensor to low priority
    "max_simultaneous_queries": 10,     // Max concurrent SNMP/Plugin queries per sensor
    "sleep_main": 10,                   // Main scheduler loop interval (seconds)
    "sleep_worker": 2,                  // Idle worker sleep interval (seconds)
    "mib_dirs": [                       // Custom directories to search for SNMP MIBs
      "/usr/share/snmp/mibs"
    ],
    // Kafka Export Configuration
    "kafka_broker": "127.0.0.1:9092",
    "kafka_topic": "telemetry",
    "kafka_timeout": 5,
    // HTTP Export Configuration
    "http_endpoint": "http://127.0.0.1:8080/metrics",
    "http_timeout": 5,
    "http_insecure": 0
  },
  "sensors": [
    {
      "sensor_name": "edge-switch-01",
      "sensor_ip": "10.1.30.150",
      "timeout": 3,                     // Overrides global timeout
      "snmp_version": "2c",
      "community": "public",
      "monitors": [
        // 1. SNMP Query with Textual OID translation
        {
          "name": "port_status",
          "oid": "IF-MIB::ifOperStatus.1",
          "unit": "state",
          "integer": 1
        },
        // 2. SNMP Walk with Vector Splitting and Aggregation
        {
          "name": "cpu_utilization",
          "plugin": "snmp_walk",
          "params": {
            "oid": "1.3.6.1.4.1.9.9.109.1.1.1.1.3"
          },
          "split": ";",
          "split_op": "mean",           // Calculate average value of all CPUs
          "unit": "%"
        }
      ]
    },
    {
      "sensor_name": "linux-hypervisor-01",
      "sensor_ip": "10.1.30.156",
      "monitors": [
        // 3. Ping / Latency plugin
        {
          "name": "latency",
          "plugin": "ping",
          "params": {
            "count": 5,
            "interval_ms": 100,
            "metric": "latency",        // Can be "latency" / "rtt" or "packet_loss"
            "privileged": false         // Set true to use raw socket ping
          },
          "unit": "ms"
        },
        // 4. IPMI Hardware Sensor Query
        {
          "name": "inlet_temperature",
          "plugin": "ipmi",
          "params": {
            "command": "sdr",
            "sensor_name": "Inlet Temp",
            "username": "admin",
            "password": "secretPassword"
          },
          "unit": "C"
        }
      ]
    }
  ]
}
```

---

## Configuration Reference

### Global Settings (`"conf"`)

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `debug` | Integer | `100` | Log filter level (0=Emerg, 3=Err, 6=Info, 7=Debug). |
| `stdout` | Integer | `1` | Write JSON metric lines to stdout if non-zero. |
| `syslog` | Integer | `0` | Log messages to syslog if non-zero. |
| `threads` | Integer | `10` | Worker concurrency pool limit. |
| `timeout` | Integer | `5` | Default query timeout in seconds. |
| `max_snmp_fails` | Integer | `2` | Number of sequential failures before marking a sensor low priority. |
| `max_simultaneous_queries` | Integer | `10` | Maximum number of concurrent SNMP/Plugin queries executed per sensor at any given time. |
| `sleep_main` | Integer | `10` | Interval in seconds for the scheduler's main loop. |
| `mib_dirs` | Array | `[]` | List of folders containing custom SNMP MIB text files to load. |
| `kafka_broker` | String | `""` | Address of destination Kafka broker (e.g. `localhost:9092`). |
| `kafka_topic` | String | `""` | Topic name for Kafka message output. |
| `http_endpoint` | String | `""` | HTTP POST URL endpoint for metric output. |

### Sensor Settings (`"sensors"`)
| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `sensor_id` | Integer | `0` | Optional unique numeric ID for the sensor (attached to exported metrics). |
| `sensor_name` | String | `""` | Hostname or display name of the device (at least name or IP is required). |
| `sensor_ip` | String | `""` | IP address or domain name of the device (at least name or IP is required). |
| `timeout` | Integer | Global | Sensor-specific timeout in seconds. |
| `snmp_version` | String | `""` | Version of SNMP to use (`"1"`, `"2c"`, `"3"`). |
| `community` | String | `"public"` | SNMP community string (v1/v2c only). |
| `govc_username` | String | `""` | Fallback vCenter/ESXi username for `govc` plugin. |
| `govc_password` | String | `""` | Fallback vCenter/ESXi password for `govc` plugin. |
| `snmp_username` | String | `""` | SNMPv3 security user. |
| `snmp_security_level` | String | `""` | SNMPv3 security level (`noAuthNoPriv`, `authNoPriv`, `authPriv`). |
| `snmp_auth_protocol` | String | `""` | SNMPv3 authentication protocol (`MD5`, `SHA`). |
| `snmp_auth_password` | String | `""` | SNMPv3 authentication password. |
| `snmp_priv_protocol` | String | `""` | SNMPv3 privacy protocol (`DES`, `AES`). |
| `snmp_priv_password` | String | `""` | SNMPv3 privacy password. |
| `enrichment` | Map | `{}` | Static tags to attach to all metrics produced by this sensor. |

### Monitor Settings (`"monitors"`)

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `name` | String | *Required* | Unique name for the collected metric. |
| `oid` | String | `""` | SNMP OID to fetch (supports names like `IF-MIB::ifIndex.1`). |
| `unit` | String | `""` | Unit label attached to outputs. |
| `send` | Integer | `1` | Set to `0` to keep the metric in cache without exporting it. |
| `op` | String | `""` | Infix math operation to evaluate (e.g. `a + b`). |
| `system` | String | `""` | CLI command line string to run as a fallback shell query. |
| `plugin` | String | `""` | Native plugin name to execute (e.g. `ping`, `govc`, `redfish`, `ipmi`). |
| `params` | Map | `{}` | Key-value settings passed to the plugin. |
| `split` | String | `""` | Character used to split vector/list telemetry output. |
| `split_op` | String | `""` | Vector aggregation operator (`sum`, `mean`). |
| `integer` | Integer | `0` | Set to `1` to round metric value to nearest integer. |

---

## Native Plugins Reference

You can review all compiled plugins and parameters directly from the binary by running:
```bash
./redborder-monitor --info
```

### 1. `ping`
Sends ICMP Echo requests to verify availability and average response latency.
* **Parameters**:
  * `host` (string, default: sensor's IP): Destination host.
  * `count` (int, default: `3`): Number of ping packets to send.
  * `interval_ms` (int, default: `100`): Interval between packets.
  * `metric` (string, default: `"packet_loss"`): Metric to emit (`"packet_loss"` or `"latency"`).
  * `size` (int, default: `56`): Data payload size.
  * `privileged` (bool, default: `false`): Use raw sockets instead of unprivileged UDP sockets.

### 2. `snmp`
Performs a single SNMP GET query.
* **Parameters**:
  * `oid` (string, default: monitor's `oid`): Target OID symbol or number.

### 3. `snmp_walk`
Performs an SNMP BulkWalk, returning values concatenated by a semicolon.
* **Parameters**:
  * `oid` (string, default: monitor's `oid`): Root OID branch.

### 4. `govc`
Retrieves performance metrics from VMware vCenter or ESXi hypervisors.
* **Parameters**:
  * `host` (string, default: sensor's IP): vCenter/ESXi API address.
  * `username` (string, default: sensor's `govc_username`): VMware login.
  * `password` (string, default: sensor's `govc_password`): VMware password.
  * `target` (string, default: sensor's name): Target virtual machine or host.
  * `target_type` (string, default: `"vm"`): Type of target (`"vm"` or `"host"`).
  * `metric` (string, default: `"power_state"`): Metric path (e.g. `cpu_usage`, `memory_usage`, `disk_usage`).

### 5. `redfish`
Polls OOB BMC interfaces (Dell iDRAC, HPE iLO, etc.) using Redfish JSON REST APIs.
* **Parameters**:
  * `host` (string, default: sensor's IP): BMC IP address.
  * `path` (string, default: `"/redfish/v1/"`): Redfish endpoint URL.
  * `json_path` (string): JSON dot-path to query (e.g. `Temperatures.0.ReadingCelsius`).
  * `username` (string): BMC login.
  * `password` (string): BMC password.

### 6. `ipmi`
Queries hardware IPMI SDR tables and chassis status directly over UDP.
* **Parameters**:
  * `host` (string, default: sensor's IP): BMC IP address.
  * `command` (string, default: `"chassis_status"`): IPMI action (`"chassis_status"`, `"power_status"`, `"sdr"`).
  * `sensor_name` (string): SDR sensor descriptor name (e.g. `"CPU Temp"`).
  * `port` (int, default: `623`): IPMI RMCP port.
  * `username` (string): BMC login.
  * `password` (string): BMC password.

---

## Compilation & Execution

### Build from Source
Ensure Go 1.18+ is installed:
```bash
go build -o redborder-monitor ./cmd/redborder-monitor
```

### Command Line CLI Usage
```text
Usage of ./redborder-monitor:
  -c, --config string      Path to configuration file
  -d, --debug int          Debug severity level (default 100)
  -g, --daemon             Go Daemon mode (run in background)
  -h, --help               Show help details
  -i, --info               Show available plugins and configuration JSON fields
  -r, --reset              Reset daemon stats counters and exit
  -s, --search-mibs string Search for loaded MIB objects by name (case-insensitive)
  -t, --status             Show daemon status/stats and exit
  -v, --version            Show version
```

### Hot Configuration Reloading
Trigger a configuration hot-reload without stopping the daemon by sending `SIGHUP` to the main process:
```bash
kill -HUP $(pgrep redborder-monitor)
```
This forces the daemon to reload the configuration file, reload custom SNMP MIBs, and safely update active queries while keeping the cached data structures intact.
