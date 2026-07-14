package monitor

import (
	"os"
	"testing"
)

func removeSpaces(s string) string {
	var result []rune
	for _, r := range s {
		if r != ' ' && r != '\n' && r != '\t' && r != '\r' {
			result = append(result, r)
		}
	}
	return string(result)
}

func TestStripComments(t *testing.T) {
	input := []byte(`{
		"conf": {
			"debug": 2, /* multi-line comment */
			"stdout": 1, // single-line comment
			"url": "http://example.com/api" /* comment with // inside string */
		}
	}`)
	expected := `{
		"conf": {
			"debug": 2, 
			"stdout": 1, 
			"url": "http://example.com/api" 
		}
	}`

	output := StripComments(input)
	// Compare after stripping spaces
	cleanOutput := removeSpaces(string(output))
	cleanExpected := removeSpaces(expected)

	if cleanOutput != cleanExpected {
		t.Errorf("StripComments failed.\nExpected: %s\nGot: %s", cleanExpected, cleanOutput)
	}
}

func TestLoadConfigWithArrayComments(t *testing.T) {
	configData := `{
		"conf": {
			"debug": 2
		},
		"sensors": [
			"/* comment inside sensors array */",
			{
				"sensor_name": "test-sensor",
				"sensor_ip": "127.0.0.1",
				"monitors": [
					"/* comment inside monitors array */",
					{
						"name": "load_1",
						"oid": "1.3.6.1.4.1.2021.10.1.3.1"
					}
				]
			}
		]
	}`

	// Write to a temporary file
	tmpFile, err := os.CreateTemp("", "test_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(configData)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Load the config
	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify filtering worked
	if len(config.Sensors) != 1 {
		t.Fatalf("Expected 1 sensor, got %d", len(config.Sensors))
	}
	sensor := config.Sensors[0]
	if sensor.SensorName != "test-sensor" {
		t.Errorf("Expected sensor name 'test-sensor', got '%s'", sensor.SensorName)
	}

	if len(sensor.Monitors) != 1 {
		t.Fatalf("Expected 1 monitor, got %d", len(sensor.Monitors))
	}
	monitor := sensor.Monitors[0]
	if monitor.Name != "load_1" {
		t.Errorf("Expected monitor name 'load_1', got '%s'", monitor.Name)
	}
}

func TestLoadConfigWithStringifiedIntegers(t *testing.T) {
	configData := `{
		"conf": {
			"debug": "3",
			"threads": "5"
		},
		"sensors": [
			{
				"sensor_name": "test-sensor",
				"sensor_ip": "127.0.0.1",
				"timeout": "1000",
				"monitors": [
					{
						"name": "load_1",
						"integer": "1",
						"send": "0"
					}
				]
			}
		]
	}`

	tmpFile, err := os.CreateTemp("", "test_config_stringified_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(configData)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.Conf.Debug != 3 {
		t.Errorf("Expected Debug=3, got %d", config.Conf.Debug)
	}
	if config.Conf.Threads != 5 {
		t.Errorf("Expected Threads=5, got %d", config.Conf.Threads)
	}
	if config.Sensors[0].Timeout != 1000 {
		t.Errorf("Expected sensor timeout=1000, got %d", config.Sensors[0].Timeout)
	}
	monitor := config.Sensors[0].Monitors[0]
	if monitor.Integer == nil || *monitor.Integer != 1 {
		t.Errorf("Expected monitor.Integer=1, got %v", monitor.Integer)
	}
	if monitor.Send == nil || *monitor.Send != 0 {
		t.Errorf("Expected monitor.Send=0, got %v", monitor.Send)
	}
}

func TestLoadConfigWithBooleanValues(t *testing.T) {
	configData := `{
		"conf": {
			"syslog": true,
			"stdout": false,
			"http_insecure": "true",
			"debug": "false"
		},
		"sensors": [
			{
				"sensor_name": "test-sensor",
				"sensor_ip": "127.0.0.1",
				"monitors": [
					{
						"name": "load_1",
						"integer": true,
						"send": "false"
					}
				]
			}
		]
	}`

	tmpFile, err := os.CreateTemp("", "test_config_boolean_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(configData)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.Conf.Syslog != 1 {
		t.Errorf("Expected Syslog=1, got %d", config.Conf.Syslog)
	}
	if config.Conf.Stdout != 0 {
		t.Errorf("Expected Stdout=0, got %d", config.Conf.Stdout)
	}
	if config.Conf.HttpInsecure != 1 {
		t.Errorf("Expected HttpInsecure=1, got %d", config.Conf.HttpInsecure)
	}
	if config.Conf.Debug != 0 {
		t.Errorf("Expected Debug=0, got %d", config.Conf.Debug)
	}
	monitor := config.Sensors[0].Monitors[0]
	if monitor.Integer == nil || *monitor.Integer != 1 {
		t.Errorf("Expected monitor.Integer=1, got %v", monitor.Integer)
	}
	if monitor.Send == nil || *monitor.Send != 0 {
		t.Errorf("Expected monitor.Send=0, got %v", monitor.Send)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	config, err := LoadConfig("non_existent_file_path_12345.json")
	if err == nil {
		t.Fatal("expected error when loading non-existent file, got nil")
	}
	if config != nil {
		t.Errorf("expected config to be nil on error, got %v", config)
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "invalid_config_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(`{ "conf": { "debug": `)); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err == nil {
		t.Fatal("expected error when loading invalid JSON, got nil")
	}
	if config != nil {
		t.Errorf("expected config to be nil, got %v", config)
	}
}

func TestLoadConfig_InvalidFieldType(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "invalid_field_config_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configData := `{
		"conf": {
			"debug": "abc"
		}
	}`

	if _, err := tmpFile.Write([]byte(configData)); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err == nil {
		t.Fatal("expected error when debug is 'abc', got nil")
	}
	if config != nil {
		t.Errorf("expected config to be nil, got %v", config)
	}
}
