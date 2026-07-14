package monitor

import (
	"fmt"
	"io"
	"sort"
)

// PrintInfo prints detailed configuration fields and native plugin descriptions to the specified writer.
func PrintInfo(w io.Writer) {
	fmt.Fprintln(w, "redborder_monitor Configuration & Plugin Documentation")
	fmt.Fprintln(w, "======================================================")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "CONFIGURATION STRUCTURE (JSON)")
	fmt.Fprintln(w, "------------------------------")
	fmt.Fprintln(w, "The configuration JSON file consists of two main blocks:")
	fmt.Fprintln(w, "  1. \"conf\": Global settings block (logging, threads, outputs)")
	fmt.Fprintln(w, "  2. \"sensors\": Array of target devices and their query monitors")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "1. Global settings (\"conf\"):")
	fmt.Fprintln(w, "   - debug (int, default 100): Debug severity level (lower is more verbose: 0=Emerg, 3=Err, 6=Info, 7=Debug)")
	fmt.Fprintln(w, "   - stdout (int, default 1): If non-zero, writes JSON metric outputs to stdout")
	fmt.Fprintln(w, "   - syslog (int, default 0): If non-zero, writes log messages to syslog")
	fmt.Fprintln(w, "   - threads (int, default 10): Number of worker threads for parallel sensor processing")
	fmt.Fprintln(w, "   - timeout (int, default 5): Default timeout in seconds for sensor queries")
	fmt.Fprintln(w, "   - max_snmp_fails (int, default 2): Max consecutive SNMP fails before marking a sensor low priority")
	fmt.Fprintln(w, "   - max_kafka_fails (int, default 0): Max consecutive Kafka delivery failures before exiting/erroring")
	fmt.Fprintln(w, "   - sleep_main (int, default 10): Main scheduler loop sleep interval in seconds")
	fmt.Fprintln(w, "   - sleep_worker (int, default 2): Sleep interval in seconds for workers when idle")
	fmt.Fprintln(w, "   - kafka_broker (string): Kafka broker address (e.g. \"localhost:9092\")")
	fmt.Fprintln(w, "   - kafka_topic (string): Kafka topic for metric output")
	fmt.Fprintln(w, "   - kafka_timeout (int): Kafka delivery timeout in seconds")
	fmt.Fprintln(w, "   - http_endpoint (string): HTTP POST endpoint URL for metric output")
	fmt.Fprintln(w, "   - http_max_total_connections (int): Maximum total HTTP connections")
	fmt.Fprintln(w, "   - http_timeout (int): HTTP client timeout in seconds")
	fmt.Fprintln(w, "   - http_connttimeout (int): HTTP connection timeout in seconds")
	fmt.Fprintln(w, "   - http_verbose (int): HTTP verbose logging (0/1)")
	fmt.Fprintln(w, "   - http_insecure (int): HTTP insecure TLS skip verification (0/1)")
	fmt.Fprintln(w, "   - rb_http_max_messages (int): Max messages per HTTP post batch")
	fmt.Fprintln(w, "   - rb_http_mode (string): HTTP post format/mode")
	fmt.Fprintln(w, "   - ipc_socket_path (string): Unix socket path for IPC daemon status (default \"/var/run/redborder-monitor/redborder-monitor.sock\")")
	fmt.Fprintln(w, "   - enrichment (map): Global static tags to add to all metrics (key-value pairs)")
	fmt.Fprintln(w, "   - mib_dirs (array of strings): Directory paths containing custom SNMP MIB text files to load dynamically at startup (e.g. [\"/usr/share/snmp/mibs\"])")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "2. Device / Host settings (\"sensors\" array):")
	fmt.Fprintln(w, "   - sensor_id (int, optional): Unique sensor ID")
	fmt.Fprintln(w, "   - timeout (int): Sensor-specific timeout in seconds (overrides global timeout)")
	fmt.Fprintln(w, "   - sensor_name (string): Name of the sensor device (at least name or IP is required)")
	fmt.Fprintln(w, "   - sensor_ip (string): IP address / hostname of the sensor device (at least name or IP is required)")
	fmt.Fprintln(w, "   - snmp_version (string): SNMP version (\"1\", \"2c\", \"3\")")
	fmt.Fprintln(w, "   - community (string, default \"public\"): SNMP community string for v1/v2c")
	fmt.Fprintln(w, "   - govc_username (string): VMware Basic Auth username (fallback for govc plugin)")
	fmt.Fprintln(w, "   - govc_password (string): VMware Basic Auth password (fallback for govc plugin)")
	fmt.Fprintln(w, "   - snmp_username (string): SNMP v3 username")
	fmt.Fprintln(w, "   - snmp_security_level (string): SNMP v3 security level (\"noAuthNoPriv\", \"authNoPriv\", \"authPriv\")")
	fmt.Fprintln(w, "   - snmp_auth_protocol (string): SNMP v3 auth protocol (\"MD5\", \"SHA\")")
	fmt.Fprintln(w, "   - snmp_auth_password (string): SNMP v3 auth password")
	fmt.Fprintln(w, "   - snmp_priv_protocol (string): SNMP v3 privacy protocol (\"DES\", \"AES\")")
	fmt.Fprintln(w, "   - snmp_priv_password (string): SNMP v3 privacy password")
	fmt.Fprintln(w, "   - enrichment (map): Sensor-specific static tags to add to all metrics for this sensor")
	fmt.Fprintln(w, "   - monitors (array): Array of monitor query configurations (see below)")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "3. Metric Query settings (\"monitors\" array):")
	fmt.Fprintln(w, "   - name (string): Unique metric name")
	fmt.Fprintln(w, "   - oid (string): SNMP OID to query")
	fmt.Fprintln(w, "   - unit (string): Unit label for the metric")
	fmt.Fprintln(w, "   - nonzero (int, default 0): If 1, filter out zero/empty values")
	fmt.Fprintln(w, "   - send (int, default 1): If 0, do not output this metric (useful for intermediate math steps)")
	fmt.Fprintln(w, "   - op (string): Math formula to evaluate (e.g. \"a + b\", supports cross-group reference: \"_gid_<id>\")")
	fmt.Fprintln(w, "   - system (string): Shell command to execute (fallback if no plugin or OID is specified)")
	fmt.Fprintln(w, "   - split (string): Delimiter to split vector/list telemetry output")
	fmt.Fprintln(w, "   - instance_prefix (string): Prefix to prepend to split instances")
	fmt.Fprintln(w, "   - name_split_suffix (string): Suffix to append to split names")
	fmt.Fprintln(w, "   - split_op (string): Split aggregation operation (\"sum\", \"mean\")")
	fmt.Fprintln(w, "   - timestamp_given (int, default 0): Set to 1 if timestamp is present in split values (\"timestamp:value\")")
	fmt.Fprintln(w, "   - integer (int, default 0): Set to 1 if output metric value should be formatted/rounded as integer")
	fmt.Fprintln(w, "   - group_id (int/string): Group ID for cross-group math dependencies")
	fmt.Fprintln(w, "   - group_name (string): Group name")
	fmt.Fprintln(w, "   - enrichment (map): Monitor-specific static tags to add to this metric")
	fmt.Fprintln(w, "   - plugin (string): Native plugin name to execute instead of SNMP/System command")
	fmt.Fprintln(w, "   - params (map): Key-value parameters passed to the native plugin")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "AVAILABLE PLUGINS & USAGE")
	fmt.Fprintln(w, "-------------------------")

	// Sort plugin names for deterministic output
	var keys []string
	for k := range PluginsRegistry {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, name := range keys {
		def := PluginsRegistry[name]
		fmt.Fprintf(w, "%d. %s:\n", i+1, name)
		fmt.Fprintf(w, "   Description: %s\n", def.Description)
		if len(def.Params) > 0 {
			fmt.Fprintln(w, "   Parameters:")
			for _, p := range def.Params {
				fmt.Fprintf(w, "     - %s (%s, default: %s): %s\n", p.Name, p.Type, p.Default, p.Description)
			}
		}
		fmt.Fprintln(w)
	}
}
