package monitor

import (
	"encoding/json"
	"io"
	"os"
)

// Conf holds the global configuration parameters.
type Conf struct {
	Debug                   int                    `json:"debug"`
	Stdout                  int                    `json:"stdout"`
	Syslog                  int                    `json:"syslog"`
	Threads                 int                    `json:"threads"`
	Timeout                 int                    `json:"timeout"`
	MaxSNMPFails            int                    `json:"max_snmp_fails"`
	MaxKafkaFails           int                    `json:"max_kafka_fails"`
	SleepMain               int                    `json:"sleep_main"`
	SleepWorker             int                    `json:"sleep_worker"`
	KafkaBroker             string                 `json:"kafka_broker"`
	KafkaTopic              string                 `json:"kafka_topic"`
	KafkaTimeout            int                    `json:"kafka_timeout"`
	HttpEndpoint            string                 `json:"http_endpoint"`
	HttpMaxTotalConnections int                    `json:"http_max_total_connections"`
	HttpTimeout             int                    `json:"http_timeout"`
	HttpConnTimeout         int                    `json:"http_connttimeout"`
	HttpVerbose             int                    `json:"http_verbose"`
	HttpInsecure            int                    `json:"http_insecure"`
	RbHttpMaxMessages       int                    `json:"rb_http_max_messages"`
	RbHttpMode              string                 `json:"rb_http_mode"`
	IpcSocketPath           string                 `json:"ipc_socket_path"`
	Enrichment              map[string]interface{} `json:"enrichment"`
	MibDirs                 []string               `json:"mib_dirs"`
}

// Monitor represents an individual monitor query within a Sensor.
type Monitor struct {
	Name            string                 `json:"name"`
	Oid             string                 `json:"oid"`
	Unit            string                 `json:"unit"`
	Nonzero         int                    `json:"nonzero"`
	Send            *int                   `json:"send"`              // Use pointer to check if present
	Op              string                 `json:"op"`                // Operation formula
	System          string                 `json:"system"`            // Command execution
	Split           string                 `json:"split"`             // Vector split delimiter
	InstancePrefix  string                 `json:"instance_prefix"`   // Prefix for vector instances
	NameSplitSuffix string                 `json:"name_split_suffix"` // Suffix for split names
	SplitOp         string                 `json:"split_op"`          // Split operation (sum/mean)
	TimestampGiven  *int                   `json:"timestamp_given"`   // 1 if timestamp is present in split values
	Integer         *int                   `json:"integer"`           // 1 if output value should be printed as integer
	GroupID         interface{}            `json:"group_id"`          // Can be int or string
	GroupName       string                 `json:"group_name"`
	Enrichment      map[string]interface{} `json:"enrichment"`
	Plugin          string                 `json:"plugin"`            // Plugin name (e.g., "ping", "snmp_walk")
	Params          map[string]interface{} `json:"params"`            // Plugin parameters
}

// Sensor defines the device to monitor.
type Sensor struct {
	SensorID          int                    `json:"sensor_id"`
	Timeout           int                    `json:"timeout"`
	SensorName        string                 `json:"sensor_name"`
	SensorIP          string                 `json:"sensor_ip"`
	SnmpVersion       string                 `json:"snmp_version"`
	Community         string                 `json:"community"`
	SnmpUsername      string                 `json:"snmp_username"`
	SnmpSecurityLevel string                 `json:"snmp_security_level"`
	SnmpAuthProtocol  string                 `json:"snmp_auth_protocol"`
	SnmpAuthPassword  string                 `json:"snmp_auth_password"`
	SnmpPrivProtocol  string                 `json:"snmp_priv_protocol"`
	SnmpPrivPassword  string                 `json:"snmp_priv_password"`
	GovcUsername      string                 `json:"govc_username"`
	GovcPassword      string                 `json:"govc_password"`
	Enrichment        map[string]interface{} `json:"enrichment"`
	Monitors          []Monitor              `json:"monitors"`
}

// Config is the root configuration structure.
type Config struct {
	Conf    Conf     `json:"conf"`
	Sensors []Sensor `json:"sensors"`
}

// parseRawInt helper to parse a JSON RawMessage that can be a number, a quoted string, or a boolean
func parseRawInt(raw json.RawMessage) (int, bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false, nil
	}
	s := string(raw)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	// Try parsing as boolean first (both bare true/false and stringified "true"/"false")
	var b bool
	if err := json.Unmarshal([]byte(s), &b); err == nil {
		if b {
			return 1, true, nil
		}
		return 0, true, nil
	}
	var val int
	if err := json.Unmarshal([]byte(s), &val); err != nil {
		return 0, false, err
	}
	return val, true, nil
}

// UnmarshalJSON customizes unmarshaling for Conf to handle stringified integers.
func (c *Conf) UnmarshalJSON(data []byte) error {
	type Alias Conf
	var temp struct {
		Alias
		Debug                   json.RawMessage `json:"debug"`
		Stdout                  json.RawMessage `json:"stdout"`
		Syslog                  json.RawMessage `json:"syslog"`
		Threads                 json.RawMessage `json:"threads"`
		Timeout                 json.RawMessage `json:"timeout"`
		MaxSNMPFails            json.RawMessage `json:"max_snmp_fails"`
		MaxKafkaFails           json.RawMessage `json:"max_kafka_fails"`
		SleepMain               json.RawMessage `json:"sleep_main"`
		SleepWorker             json.RawMessage `json:"sleep_worker"`
		KafkaTimeout            json.RawMessage `json:"kafka_timeout"`
		HttpMaxTotalConnections json.RawMessage `json:"http_max_total_connections"`
		HttpTimeout             json.RawMessage `json:"http_timeout"`
		HttpConnTimeout         json.RawMessage `json:"http_connttimeout"`
		HttpVerbose             json.RawMessage `json:"http_verbose"`
		HttpInsecure            json.RawMessage `json:"http_insecure"`
		RbHttpMaxMessages       json.RawMessage `json:"rb_http_max_messages"`
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	*c = Conf(temp.Alias)

	assign := func(raw json.RawMessage, target *int) error {
		val, ok, err := parseRawInt(raw)
		if err != nil {
			return err
		}
		if ok {
			*target = val
		}
		return nil
	}

	if err := assign(temp.Debug, &c.Debug); err != nil {
		return err
	}
	if err := assign(temp.Stdout, &c.Stdout); err != nil {
		return err
	}
	if err := assign(temp.Syslog, &c.Syslog); err != nil {
		return err
	}
	if err := assign(temp.Threads, &c.Threads); err != nil {
		return err
	}
	if err := assign(temp.Timeout, &c.Timeout); err != nil {
		return err
	}
	if err := assign(temp.MaxSNMPFails, &c.MaxSNMPFails); err != nil {
		return err
	}
	if err := assign(temp.MaxKafkaFails, &c.MaxKafkaFails); err != nil {
		return err
	}
	if err := assign(temp.SleepMain, &c.SleepMain); err != nil {
		return err
	}
	if err := assign(temp.SleepWorker, &c.SleepWorker); err != nil {
		return err
	}
	if err := assign(temp.KafkaTimeout, &c.KafkaTimeout); err != nil {
		return err
	}
	if err := assign(temp.HttpMaxTotalConnections, &c.HttpMaxTotalConnections); err != nil {
		return err
	}
	if err := assign(temp.HttpTimeout, &c.HttpTimeout); err != nil {
		return err
	}
	if err := assign(temp.HttpConnTimeout, &c.HttpConnTimeout); err != nil {
		return err
	}
	if err := assign(temp.HttpVerbose, &c.HttpVerbose); err != nil {
		return err
	}
	if err := assign(temp.HttpInsecure, &c.HttpInsecure); err != nil {
		return err
	}
	if err := assign(temp.RbHttpMaxMessages, &c.RbHttpMaxMessages); err != nil {
		return err
	}

	return nil
}

// UnmarshalJSON customizes unmarshaling for Monitor to ignore string comments in arrays and handle stringified integers.
func (m *Monitor) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return nil
	}
	type Alias Monitor
	var temp struct {
		Alias
		Nonzero        json.RawMessage `json:"nonzero"`
		Send           json.RawMessage `json:"send"`
		TimestampGiven json.RawMessage `json:"timestamp_given"`
		Integer        json.RawMessage `json:"integer"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	*m = Monitor(temp.Alias)

	val, ok, err := parseRawInt(temp.Nonzero)
	if err != nil {
		return err
	}
	if ok {
		m.Nonzero = val
	}

	val, ok, err = parseRawInt(temp.Send)
	if err != nil {
		return err
	}
	if ok {
		sendVal := val
		m.Send = &sendVal
	}

	val, ok, err = parseRawInt(temp.TimestampGiven)
	if err != nil {
		return err
	}
	if ok {
		tgVal := val
		m.TimestampGiven = &tgVal
	}

	val, ok, err = parseRawInt(temp.Integer)
	if err != nil {
		return err
	}
	if ok {
		intVal := val
		m.Integer = &intVal
	}

	return nil
}

// UnmarshalJSON customizes unmarshaling for Sensor to ignore string comments in arrays and handle stringified integers.
func (s *Sensor) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return nil
	}
	type Alias Sensor
	var temp struct {
		Alias
		SensorID json.RawMessage `json:"sensor_id"`
		Timeout  json.RawMessage `json:"timeout"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	*s = Sensor(temp.Alias)

	val, ok, err := parseRawInt(temp.SensorID)
	if err != nil {
		return err
	}
	if ok {
		s.SensorID = val
	}

	val, ok, err = parseRawInt(temp.Timeout)
	if err != nil {
		return err
	}
	if ok {
		s.Timeout = val
	}

	return nil
}

// StripComments removes C-style (/* ... */) and C++ style (// ...) comments from JSON data -- compatibility c version
func StripComments(input []byte) []byte {
	var result []byte
	inString := false
	escaped := false
	i := 0
	n := len(input)

	for i < n {
		ch := input[i]
		if inString {
			result = append(result, ch)
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inString = false
			}
			i++
			continue
		}

		if ch == '"' {
			inString = true
			result = append(result, ch)
			i++
			continue
		}

		// Check multi-line comment
		if i+1 < n && ch == '/' && input[i+1] == '*' {
			i += 2
			for i+1 < n && !(input[i] == '*' && input[i+1] == '/') {
				i++
			}
			i += 2
			continue
		}

		// Check single-line comment
		if i+1 < n && ch == '/' && input[i+1] == '/' {
			i += 2
			for i < n && input[i] != '\n' {
				i++
			}
			continue
		}

		result = append(result, ch)
		i++
	}
	return result
}

// LoadConfig reads the configuration file, strips comments, and unmarshals it.
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	sanitized := StripComments(data)

	// Set default values before unmarshal
	config := &Config{
		Conf: Conf{
			Debug:        100,
			Stdout:       1,
			Threads:      10,
			Timeout:      5,
			MaxSNMPFails: 2,
			SleepMain:    10,
			SleepWorker:  2,
		},
	}

	if err := json.Unmarshal(sanitized, config); err != nil {
		return nil, err
	}

	// Filter out skipped sensors and monitors
	var cleanSensors []Sensor
	for _, s := range config.Sensors {
		if s.SensorName == "" && s.SensorIP == "" {
			continue
		}
		var cleanMonitors []Monitor
		for _, m := range s.Monitors {
			if m.Name == "" {
				continue
			}
			cleanMonitors = append(cleanMonitors, m)
		}
		s.Monitors = cleanMonitors
		cleanSensors = append(cleanSensors, s)
	}
	config.Sensors = cleanSensors


	// Initialize dynamic MIB parsing (errors are logged internally, do not block starting the daemon)
	_ = InitMIBs(config.Conf.MibDirs)

	return config, nil
}
