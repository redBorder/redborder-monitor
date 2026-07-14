package monitor

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrintInfo(t *testing.T) {
	var buf bytes.Buffer
	PrintInfo(&buf)

	output := buf.String()

	// Check for a few critical keywords to make sure config description is present
	keywords := []string{
		"debug (int, default 100)",
		"max_snmp_fails",
		"sensors",
		"monitors",
		"ping",
		"govc",
		"redfish",
		"ipmi",
		"metric",
		"packet_loss",
	}

	for _, kw := range keywords {
		if !strings.Contains(output, kw) {
			t.Errorf("Expected PrintInfo output to contain keyword %q, but it did not", kw)
		}
	}
}
