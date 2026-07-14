package monitor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golangsnmp/gomib"
	"github.com/golangsnmp/gomib/mib"
	"github.com/gosnmp/gosnmp"
)

var (
	MibEngine   *mib.Mib
	MibEngineMu sync.RWMutex
)

// InitMIBs initializes the dynamic MIB parser engine.
// It loads MIB files from the specified directories or from system paths if empty.
func InitMIBs(dirs []string) error {
	MibEngineMu.Lock()
	defer MibEngineMu.Unlock()

	var sources []gomib.Source
	for _, d := range dirs {
		if d == "" {
			continue
		}
		if _, err := os.Stat(d); os.IsNotExist(err) {
			LogMsg(LogErr, "MIB directory %s does not exist", d)
			continue
		}
		src, err := gomib.Dir(d)
		if err != nil {
			LogMsg(LogErr, "Failed to parse MIB directory %s: %v", d, err)
			continue
		}
		sources = append(sources, src)
	}

	var m *mib.Mib
	var err error

	cfg := mib.DefaultConfig()
	cfg.FailAt = mib.SeverityFatal

	if len(sources) == 0 {
		// Use system paths for auto-discovery
		m, err = gomib.Load(context.Background(), gomib.WithSystemPaths(), gomib.WithDiagnosticConfig(cfg))
		if err != nil {
			LogMsg(LogWarning, "No MIB directories specified, and failed to load system MIBs: %v", err)
			MibEngine = nil
			return nil
		}
		MibEngine = m
		LogMsg(LogInfo, "Auto-discovered and loaded system SNMP MIBs successfully")
		return nil
	}

	var finalSource gomib.Source
	if len(sources) == 1 {
		finalSource = sources[0]
	} else {
		finalSource = gomib.Multi(sources...)
	}

	m, err = gomib.Load(context.Background(), gomib.WithSource(finalSource), gomib.WithDiagnosticConfig(cfg))
	if err != nil {
		LogMsg(LogErr, "Failed to compile MIB files from directories: %v", err)
		MibEngine = nil
		return err
	}

	MibEngine = m
	LogMsg(LogInfo, "Loaded custom SNMP MIBs from %v successfully", dirs)
	return nil
}

var oidMap = map[string]string{
	"UCD-SNMP-MIB::laLoad.1":       ".1.3.6.1.4.1.2021.10.1.3.1",
	"UCD-SNMP-MIB::laLoad.2":       ".1.3.6.1.4.1.2021.10.1.3.2",
	"UCD-SNMP-MIB::laLoad.3":       ".1.3.6.1.4.1.2021.10.1.3.3",
	"UCD-SNMP-MIB::laLoad":         ".1.3.6.1.4.1.2021.10.1.3",
	"UCD-SNMP-MIB::ssCpuUser.0":     ".1.3.6.1.4.1.2021.11.9.0",
	"UCD-SNMP-MIB::ssCpuSystem.0":   ".1.3.6.1.4.1.2021.11.10.0",
	"UCD-SNMP-MIB::ssCpuIdle.0":     ".1.3.6.1.4.1.2021.11.11.0",
	"UCD-SNMP-MIB::memTotalReal.0":  ".1.3.6.1.4.1.2021.4.5.0",
	"UCD-SNMP-MIB::memAvailReal.0":  ".1.3.6.1.4.1.2021.4.6.0",
	"UCD-SNMP-MIB::memTotalSwap.0":  ".1.3.6.1.4.1.2021.4.3.0",
	"UCD-SNMP-MIB::memAvailSwap.0":  ".1.3.6.1.4.1.2021.4.4.0",
	"UCD-SNMP-MIB::memBuffer.0":     ".1.3.6.1.4.1.2021.4.14.0",
	"UCD-SNMP-MIB::memShared.0":     ".1.3.6.1.4.1.2021.4.13.0",
	"UCD-SNMP-MIB::memCached.0":     ".1.3.6.1.4.1.2021.4.15.0",
	"UCD-SNMP-MIB::dskPercent.1":    ".1.3.6.1.4.1.2021.9.1.9.1",

	"laLoad.1":       ".1.3.6.1.4.1.2021.10.1.3.1",
	"laLoad.2":       ".1.3.6.1.4.1.2021.10.1.3.2",
	"laLoad.3":       ".1.3.6.1.4.1.2021.10.1.3.3",
	"laLoad":         ".1.3.6.1.4.1.2021.10.1.3",
	"ssCpuUser.0":     ".1.3.6.1.4.1.2021.11.9.0",
	"ssCpuSystem.0":   ".1.3.6.1.4.1.2021.11.10.0",
	"ssCpuIdle.0":     ".1.3.6.1.4.1.2021.11.11.0",
	"memTotalReal.0":  ".1.3.6.1.4.1.2021.4.5.0",
	"memAvailReal.0":  ".1.3.6.1.4.1.2021.4.6.0",
	"memTotalSwap.0":  ".1.3.6.1.4.1.2021.4.3.0",
	"memAvailSwap.0":  ".1.3.6.1.4.1.2021.4.4.0",
	"memBuffer.0":     ".1.3.6.1.4.1.2021.4.14.0",
	"memShared.0":     ".1.3.6.1.4.1.2021.4.13.0",
	"memCached.0":     ".1.3.6.1.4.1.2021.4.15.0",
	"dskPercent.1":    ".1.3.6.1.4.1.2021.9.1.9.1",
}

var prefixMap = map[string]string{
	"SNMPv2-SMI::enterprises.": ".1.3.6.1.4.1.",
	"SNMPv2-SMI::mib-2.":      ".1.3.6.1.2.1.",
	"IF-MIB::":                ".1.3.6.1.2.1.2.",
	"IP-MIB::":                ".1.3.6.1.2.1.4.",
	"TCP-MIB::":               ".1.3.6.1.2.1.6.",
	"UDP-MIB::":               ".1.3.6.1.2.1.7.",
	"HOST-RESOURCES-MIB::":    ".1.3.6.1.2.1.25.",
	"UCD-SNMP-MIB::":          ".1.3.6.1.4.1.2021.",
}

// TranslateOID maps textual OID strings to numeric representation.
func TranslateOID(oidStr string) string {
	oidStr = strings.TrimSpace(oidStr)

	// First try using the parsed MIB engine
	MibEngineMu.RLock()
	engine := MibEngine
	MibEngineMu.RUnlock()

	if engine != nil {
		// Split index suffix (e.g. ifIndex.1 -> object name "ifIndex", suffix ".1")
		parts := strings.SplitN(oidStr, ".", 2)
		objectName := parts[0]
		var suffix string
		if len(parts) > 1 {
			suffix = "." + parts[1]
		}

		var obj *mib.Object
		if strings.Contains(objectName, "::") {
			oparts := strings.SplitN(objectName, "::", 2)
			modName := oparts[0]
			symName := oparts[1]
			if mod := engine.Module(modName); mod != nil {
				obj = mod.Object(symName)
			}
			if obj == nil {
				obj = engine.Object(symName)
			}
		} else {
			// Otherwise search globally in the engine
			obj = engine.Object(objectName)
		}

		if obj != nil {
			resolvedOid := obj.OID().String()
			if !strings.HasPrefix(resolvedOid, ".") {
				resolvedOid = "." + resolvedOid
			}
			return resolvedOid + suffix
		}
	}

	// Fallback to static mapping
	if val, ok := oidMap[oidStr]; ok {
		return val
	}
	for prefix, replacement := range prefixMap {
		if strings.HasPrefix(oidStr, prefix) {
			result := replacement + strings.TrimPrefix(oidStr, prefix)
			result = strings.ReplaceAll(result, "..", ".")
			result = strings.TrimPrefix(result, ".")
			return result
		}
	}
	return oidStr
}

// setupSNMPParams creates and configures a GoSNMP instance with the given credentials and timeouts.
func setupSNMPParams(ctx context.Context, ip, community, version string, timeoutMs int,
	username, securityLevel, authProtocol, authPassword, privProtocol, privPassword string) *gosnmp.GoSNMP {
	var snmpVer gosnmp.SnmpVersion
	switch strings.ToLower(version) {
	case "1":
		snmpVer = gosnmp.Version1
	case "3":
		snmpVer = gosnmp.Version3
	default:
		snmpVer = gosnmp.Version2c
	}

	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	params := &gosnmp.GoSNMP{
		Target:    ip,
		Port:      161,
		Community: community,
		Version:   snmpVer,
		Timeout:   time.Duration(timeoutMs) * time.Millisecond,
		Retries:   2,
		Context:   ctx,
	}

	if snmpVer == gosnmp.Version3 {
		var secLevel gosnmp.SnmpV3MsgFlags
		switch strings.ToLower(securityLevel) {
		case "authpriv":
			secLevel = gosnmp.AuthPriv
		case "authnopriv":
			secLevel = gosnmp.AuthNoPriv
		default:
			secLevel = gosnmp.NoAuthNoPriv
		}
		params.MsgFlags = secLevel

		authProtoLower := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(authProtocol), "-", ""), "_", ""), " ", "")
		var authProto gosnmp.SnmpV3AuthProtocol
		switch authProtoLower {
		case "md5":
			authProto = gosnmp.MD5
		case "sha", "sha1":
			authProto = gosnmp.SHA
		case "sha224":
			authProto = gosnmp.SHA224
		case "sha256":
			authProto = gosnmp.SHA256
		case "sha384":
			authProto = gosnmp.SHA384
		case "sha512":
			authProto = gosnmp.SHA512
		default:
			authProto = gosnmp.NoAuth
		}

		privProtoLower := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(privProtocol), "-", ""), "_", ""), " ", "")
		var privProto gosnmp.SnmpV3PrivProtocol
		switch privProtoLower {
		case "des":
			privProto = gosnmp.DES
		case "aes", "aes128":
			privProto = gosnmp.AES
		case "aes192":
			privProto = gosnmp.AES192
		case "aes256":
			privProto = gosnmp.AES256
		case "aes192c":
			privProto = gosnmp.AES192C
		case "aes256c":
			privProto = gosnmp.AES256C
		default:
			privProto = gosnmp.NoPriv
		}

		params.SecurityParameters = &gosnmp.UsmSecurityParameters{
			UserName:                 username,
			AuthoritativeEngineID:    "",
			AuthenticationProtocol:   authProto,
			AuthenticationPassphrase: authPassword,
			PrivacyProtocol:          privProto,
			PrivacyPassphrase:        privPassword,
		}
		params.SecurityModel = gosnmp.UserSecurityModel
	}

	return params
}

// SolveSNMPQuery performs a standard SNMP GET query using gosnmp.
func SolveSNMPQuery(ctx context.Context, ip, community, version string, timeoutMs int, oidStr string,
	username, securityLevel, authProtocol, authPassword, privProtocol, privPassword string) (string, float64, error) {
	translatedOid := TranslateOID(oidStr)

	params := setupSNMPParams(ctx, ip, community, version, timeoutMs,
		username, securityLevel, authProtocol, authPassword, privProtocol, privPassword)

	err := params.Connect()
	if err != nil {
		return "0", 0, err
	}
	defer func() {
		if params.Conn != nil {
			params.Conn.Close()
		}
	}()

	result, err := params.Get([]string{translatedOid})
	if err != nil {
		return "0", 0, err
	}

	if len(result.Variables) == 0 {
		return "0", 0, fmt.Errorf("no variables returned in SNMP response")
	}

	pdu := result.Variables[0]
	strVal, floatVal := decodeSNMPPDU(pdu)
	return strVal, floatVal, nil
}

// SolveSNMPWalk performs an SNMP Walk (or BulkWalk) and returns all results joined by a semicolon.
func SolveSNMPWalk(ctx context.Context, ip, community, version string, timeoutMs int, oidStr string,
	username, securityLevel, authProtocol, authPassword, privProtocol, privPassword string) (string, error) {
	translatedOid := TranslateOID(oidStr)

	params := setupSNMPParams(ctx, ip, community, version, timeoutMs,
		username, securityLevel, authProtocol, authPassword, privProtocol, privPassword)

	err := params.Connect()
	if err != nil {
		return "", err
	}
	defer func() {
		if params.Conn != nil {
			params.Conn.Close()
		}
	}()

	var results []string
	walkFunc := func(pdu gosnmp.SnmpPDU) error {
		strVal, _ := decodeSNMPPDU(pdu)
		results = append(results, strVal)
		return nil
	}

	if params.Version == gosnmp.Version1 {
		err = params.Walk(translatedOid, walkFunc)
	} else {
		err = params.BulkWalk(translatedOid, walkFunc)
	}
	if err != nil {
		return "", err
	}

	return strings.Join(results, ";"), nil
}

func decodeSNMPPDU(pdu gosnmp.SnmpPDU) (string, float64) {
	switch pdu.Type {
	case gosnmp.OctetString:
		var bytes []byte
		if b, ok := pdu.Value.([]byte); ok {
			bytes = b
		} else if s, ok := pdu.Value.(string); ok {
			bytes = []byte(s)
		}
		s := string(bytes)
		if s == "" {
			return "0", 0
		}
		f, _ := strconv.ParseFloat(s, 64)
		return s, f
	case gosnmp.Integer, gosnmp.Counter32, gosnmp.Gauge32, gosnmp.TimeTicks:
		val := gosnmp.ToBigInt(pdu.Value).Int64()
		return strconv.FormatInt(val, 10), float64(val)
	case gosnmp.Counter64:
		// gosnmp returns Counter64 value as uint64
		var val uint64
		if u, ok := pdu.Value.(uint64); ok {
			val = u
		} else {
			val = uint64(gosnmp.ToBigInt(pdu.Value).Int64())
		}
		return strconv.FormatUint(val, 10), float64(val)
	default:
		valStr := fmt.Sprintf("%v", pdu.Value)
		f, _ := strconv.ParseFloat(valStr, 64)
		return valStr, f
	}
}
