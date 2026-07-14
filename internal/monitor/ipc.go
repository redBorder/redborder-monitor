package monitor

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// DefaultSocketPath checks if the systemd RuntimeDirectory exists and is writable.
// If so, it returns /run/redborder-monitor/redborder-monitor.sock, bypassing PrivateTmp isolation.
// Otherwise, it falls back to /tmp/redborder-monitor.sock.
func DefaultSocketPath() string {
	runDir := "/run/redborder-monitor"
	if fi, err := os.Stat(runDir); err == nil && fi.IsDir() {
		testFile := filepath.Join(runDir, ".test_write")
		if f, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666); err == nil {
			f.Close()
			_ = os.Remove(testFile)
			return filepath.Join(runDir, "redborder-monitor.sock")
		}
	}
	return "/tmp/redborder-monitor.sock"
}

// ParseIPCAddress parses the address string into network type and address.
// Supports:
// - "/path/to/socket.sock" -> network: "unix", address: "/path/to/socket.sock"
// - "tcp://127.0.0.1:8000" -> network: "tcp", address: "127.0.0.1:8000"
// - ":8000" or "127.0.0.1:8000" -> network: "tcp", address: "127.0.0.1:8000"
func ParseIPCAddress(addr string) (network, address string) {
	if addr == "" {
		return "unix", DefaultSocketPath()
	}
	if strings.HasPrefix(addr, "tcp://") {
		return "tcp", strings.TrimPrefix(addr, "tcp://")
	}
	if strings.Contains(addr, ":") {
		return "tcp", addr
	}
	return "unix", addr
}

// StartIPCServer starts a UDS or TCP server listening for status queries.
// It returns the listener and runs connection handling in a background goroutine.
func StartIPCServer(socketPath string) (net.Listener, error) {
	network, address := ParseIPCAddress(socketPath)

	if network == "unix" {
		// Remove existing socket file if it exists
		_ = os.Remove(address)
	}

	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	if network == "unix" {
		// Adjust socket permissions to make it writable
		_ = os.Chmod(address, 0666)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleIPCConnection(conn)
		}
	}()

	return listener, nil
}

func handleIPCConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		return
	}

	cmd := "stats"
	if n > 0 {
		cmd = string(bytes.TrimSpace(buf[:n]))
	}

	if cmd == "stats" || cmd == "status" {
		stats := GetStats()
		data, err := json.MarshalIndent(stats, "", "  ")
		if err != nil {
			_, _ = conn.Write([]byte(`{"error": "failed to marshal stats"}`))
			return
		}
		_, _ = conn.Write(data)
		_, _ = conn.Write([]byte("\n"))
	} else if cmd == "reset" {
		ResetStats()
		_, _ = conn.Write([]byte(`{"status": "ok", "message": "stats reset successfully"}`))
		_, _ = conn.Write([]byte("\n"))
	} else {
		_, _ = conn.Write([]byte(`{"error": "unknown command"}`))
	}
}
