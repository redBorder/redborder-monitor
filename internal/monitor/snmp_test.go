package monitor

import (
	"context"
	"testing"

	"github.com/gosnmp/gosnmp"
)

func TestTranslateOID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"UCD-SNMP-MIB::laLoad.2", ".1.3.6.1.4.1.2021.10.1.3.2"},
		{"laLoad.2", ".1.3.6.1.4.1.2021.10.1.3.2"},
		{".1.3.6.1.4.1.2021.10.1.3.2", ".1.3.6.1.4.1.2021.10.1.3.2"},
		{"laLoad.3", ".1.3.6.1.4.1.2021.10.1.3.3"},
		{"SNMPv2-SMI::enterprises.9.9.109.1.1.1.1.4.2", "1.3.6.1.4.1.9.9.109.1.1.1.1.4.2"},
		{"IF-MIB::ifIndex.1", "1.3.6.1.2.1.2.ifIndex.1"},
	}

	for _, tt := range tests {
		got := TranslateOID(tt.input)
		if got != tt.expected {
			t.Errorf("TranslateOID(%s) = %s, expected %s", tt.input, got, tt.expected)
		}
	}
}

func TestDecodeSNMPPDU_OctetString(t *testing.T) {
	// 1. Valid float string
	pdu1 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: []byte("45.67"),
	}
	s, f := decodeSNMPPDU(pdu1)
	if s != "45.67" || f != 45.67 {
		t.Errorf("expected ('45.67', 45.67), got ('%s', %f)", s, f)
	}

	// 2. Empty string
	pdu2 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: []byte(""),
	}
	s, f = decodeSNMPPDU(pdu2)
	if s != "0" || f != 0 {
		t.Errorf("expected ('0', 0), got ('%s', %f)", s, f)
	}

	// 3. Non-numeric string
	pdu3 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: []byte("not_a_number"),
	}
	s, f = decodeSNMPPDU(pdu3)
	if s != "not_a_number" || f != 0 {
		t.Errorf("expected ('not_a_number', 0), got ('%s', %f)", s, f)
	}

	// 4. String type value (not byte slice)
	pdu4 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: "123.4",
	}
	s, f = decodeSNMPPDU(pdu4)
	if s != "123.4" || f != 123.4 {
		t.Errorf("expected ('123.4', 123.4), got ('%s', %f)", s, f)
	}
}

func TestDecodeSNMPPDU_Integers(t *testing.T) {
	types := []gosnmp.Asn1BER{
		gosnmp.Integer,
		gosnmp.Counter32,
		gosnmp.Gauge32,
		gosnmp.TimeTicks,
	}

	for _, ty := range types {
		pdu := gosnmp.SnmpPDU{
			Type:  ty,
			Value: int(9876),
		}
		s, f := decodeSNMPPDU(pdu)
		if s != "9876" || f != 9876.0 {
			t.Errorf("type %v: expected ('9876', 9876), got ('%s', %f)", ty, s, f)
		}
	}
}

func TestDecodeSNMPPDU_Counter64(t *testing.T) {
	// 1. Normal uint64 value
	pdu1 := gosnmp.SnmpPDU{
		Type:  gosnmp.Counter64,
		Value: uint64(12345678901234),
	}
	s, f := decodeSNMPPDU(pdu1)
	if s != "12345678901234" || f != 12345678901234.0 {
		t.Errorf("expected ('12345678901234', 12345678901234), got ('%s', %f)", s, f)
	}

	// 2. Fallback value
	pdu2 := gosnmp.SnmpPDU{
		Type:  gosnmp.Counter64,
		Value: int64(12345),
	}
	s, f = decodeSNMPPDU(pdu2)
	if s != "12345" || f != 12345.0 {
		t.Errorf("expected ('12345', 12345), got ('%s', %f)", s, f)
	}
}

func TestSolveSNMPQuery_Failure(t *testing.T) {
	ctx := context.Background()
	sVal, fVal, err := SolveSNMPQuery(ctx, "192.0.2.1", "public", "2c", 100, "1.3.6.1.4.1.2021.10.1.3.1", "", "", "", "", "", "")
	if err == nil {
		t.Fatal("expected error connecting to non-existent SNMP server, got nil")
	}
	if sVal != "0" || fVal != 0 {
		t.Errorf("expected default values ('0', 0) on error, got ('%s', %f)", sVal, fVal)
	}
}

func TestDynamicOIDTranslation(t *testing.T) {
	// Initialize the MIB engine with the default system directory
	err := InitMIBs([]string{"/usr/share/snmp/mibs"})
	if err != nil {
		t.Fatalf("Failed to initialize MIB engine: %v", err)
	}

	// Clean up after the test
	defer func() {
		MibEngineMu.Lock()
		MibEngine = nil
		MibEngineMu.Unlock()
	}()

	tests := []struct {
		input    string
		expected string
	}{
		{"IF-MIB::ifIndex.1", ".1.3.6.1.2.1.2.2.1.1.1"},
		{"UCD-SNMP-MIB::laLoad.2", ".1.3.6.1.4.1.2021.10.1.3.2"},
		{"IP-MIB::ipAdEntAddr.127.0.0.1", ".1.3.6.1.2.1.4.20.1.1.127.0.0.1"},
	}

	for _, tt := range tests {
		got := TranslateOID(tt.input)
		if got != tt.expected {
			t.Errorf("TranslateOID(%q) = %q, expected %q", tt.input, got, tt.expected)
		}
	}
}
