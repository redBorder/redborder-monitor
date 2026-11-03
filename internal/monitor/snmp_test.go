package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gosnmp/gosnmp"
)

func TestTranslateOID(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"UCD-SNMP-MIB::laLoad.2", []string{".1.3.6.1.4.1.2021.10.1.3.2"}},
		{"laLoad.2", []string{".1.3.6.1.4.1.2021.10.1.3.2"}},
		{".1.3.6.1.4.1.2021.10.1.3.2", []string{".1.3.6.1.4.1.2021.10.1.3.2"}},
		{"laLoad.3", []string{".1.3.6.1.4.1.2021.10.1.3.3"}},
		{"SNMPv2-SMI::enterprises.9.9.109.1.1.1.1.4.2", []string{"1.3.6.1.4.1.9.9.109.1.1.1.1.4.2", ".1.3.6.1.4.1.9.9.109.1.1.1.1.4.2"}},
		{"IF-MIB::ifIndex.1", []string{"1.3.6.1.2.1.2.ifIndex.1", ".1.3.6.1.2.1.2.2.1.1.1", "1.3.6.1.2.1.2.2.1.1.1"}},
	}

	for _, tt := range tests {
		got := TranslateOID(tt.input)
		match := false
		for _, exp := range tt.expected {
			if got == exp {
				match = true
				break
			}
		}
		if !match {
			t.Errorf("TranslateOID(%s) = %s, expected one of %v", tt.input, got, tt.expected)
		}
	}
}

func TestDecodeSNMPPDU_OctetString(t *testing.T) {
	// 1. Valid float string
	pdu1 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: []byte("45.67"),
	}
	s, f, err := decodeSNMPPDU(pdu1)
	if err != nil || s != "45.67" || f != 45.67 {
		t.Errorf("expected ('45.67', 45.67, nil), got ('%s', %f, %v)", s, f, err)
	}

	// 2. Empty string
	pdu2 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: []byte(""),
	}
	s, f, err = decodeSNMPPDU(pdu2)
	if err != nil || s != "0" || f != 0 {
		t.Errorf("expected ('0', 0, nil), got ('%s', %f, %v)", s, f, err)
	}

	// 3. Non-numeric string returns ErrSNMPNonNumericValue
	pdu3 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: []byte("not_a_number"),
	}
	s, f, err = decodeSNMPPDU(pdu3)
	if !errors.Is(err, ErrSNMPNonNumericValue) {
		t.Errorf("expected ErrSNMPNonNumericValue for non-numeric string, got %v", err)
	}
	if s != "not_a_number" || f != 0 {
		t.Errorf("expected ('not_a_number', 0), got ('%s', %f)", s, f)
	}

	// 4. String type value (not byte slice)
	pdu4 := gosnmp.SnmpPDU{
		Type:  gosnmp.OctetString,
		Value: "123.4",
	}
	s, f, err = decodeSNMPPDU(pdu4)
	if err != nil || s != "123.4" || f != 123.4 {
		t.Errorf("expected ('123.4', 123.4, nil), got ('%s', %f, %v)", s, f, err)
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
		s, f, err := decodeSNMPPDU(pdu)
		if err != nil || s != "9876" || f != 9876.0 {
			t.Errorf("type %v: expected ('9876', 9876, nil), got ('%s', %f, %v)", ty, s, f, err)
		}
	}
}

func TestDecodeSNMPPDU_Counter64(t *testing.T) {
	// 1. Normal uint64 value
	pdu1 := gosnmp.SnmpPDU{
		Type:  gosnmp.Counter64,
		Value: uint64(12345678901234),
	}
	s, f, err := decodeSNMPPDU(pdu1)
	if err != nil || s != "12345678901234" || f != 12345678901234.0 {
		t.Errorf("expected ('12345678901234', 12345678901234, nil), got ('%s', %f, %v)", s, f, err)
	}

	// 2. Fallback value
	pdu2 := gosnmp.SnmpPDU{
		Type:  gosnmp.Counter64,
		Value: int64(12345),
	}
	s, f, err = decodeSNMPPDU(pdu2)
	if err != nil || s != "12345" || f != 12345.0 {
		t.Errorf("expected ('12345', 12345, nil), got ('%s', %f, %v)", s, f, err)
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

func TestIsSNMPExceptionType(t *testing.T) {
	exceptionTypes := []gosnmp.Asn1BER{gosnmp.NoSuchObject, gosnmp.NoSuchInstance, gosnmp.EndOfMibView}
	for _, typ := range exceptionTypes {
		if !isSNMPExceptionType(typ) {
			t.Errorf("expected %v to be treated as an SNMP exception type", typ)
		}
	}

	valueTypes := []gosnmp.Asn1BER{gosnmp.OctetString, gosnmp.Integer, gosnmp.Counter32, gosnmp.Gauge32, gosnmp.TimeTicks, gosnmp.Counter64}
	for _, typ := range valueTypes {
		if isSNMPExceptionType(typ) {
			t.Errorf("did not expect %v to be treated as an SNMP exception type", typ)
		}
	}
}

func TestErrSNMPObjectNotFound_Wrapping(t *testing.T) {
	// SolveSNMPQuery wraps ErrSNMPObjectNotFound with fmt.Errorf("%w: ...");
	// verify callers can still detect it via errors.Is, as processSNMPMonitor does.
	wrapped := fmt.Errorf("%w: OID .1.3.6.1.2.1.1.3.0 (%v)", ErrSNMPObjectNotFound, gosnmp.NoSuchObject)
	if !errors.Is(wrapped, ErrSNMPObjectNotFound) {
		t.Error("expected wrapped error to match ErrSNMPObjectNotFound via errors.Is")
	}
}

func TestIsValidNumericOID(t *testing.T) {
	valid := []string{
		".1.3.6.1.2.1.1.3.0",
		"1.3.6.1.4.1.2021.10.1.3.1",
		".1.3.6.1.2.1.2.2.1.1.1",
		"1.3.6.1.2.1.25.1.1.0",
	}
	for _, oid := range valid {
		if !isValidNumericOID(oid) {
			t.Errorf("expected %q to be valid numeric OID", oid)
		}
	}

	invalid := []string{
		"",
		".",
		"sysUpTimeInstance",
		"DISMAN-EVENT-MIB::sysUpTimeInstance",
		"HOST-RESOURCES-MIB::hrSystemUptime.0",
		"1.3.6.1.2.1.2.ifIndex.1",
		"not.a.number",
		"..1.3.6.1",
	}
	for _, oid := range invalid {
		if isValidNumericOID(oid) {
			t.Errorf("expected %q to be invalid numeric OID", oid)
		}
	}
}

func TestSolveSNMPQuery_InvalidOID(t *testing.T) {
	ctx := context.Background()
	_, _, err := SolveSNMPQuery(ctx, "192.168.1.1", "public", "2c", 1000, "nonexistent-mib::unresolvable.0", "", "", "", "", "", "")
	if err == nil {
		t.Fatal("expected error for unresolvable text OID, got nil")
	}
	if !errors.Is(err, ErrSNMPInvalidOID) {
		t.Errorf("expected ErrSNMPInvalidOID, got %v", err)
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

	// Skip if the MIB engine was not able to load the required MIB files
	MibEngineMu.RLock()
	engine := MibEngine
	MibEngineMu.RUnlock()
	if engine == nil || engine.Module("IF-MIB") == nil || engine.Module("IP-MIB") == nil || engine.Module("UCD-SNMP-MIB") == nil {
		t.Skip("Skipping TestDynamicOIDTranslation: required SNMP MIB modules not loaded (IF-MIB, IP-MIB, UCD-SNMP-MIB)")
	}

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
