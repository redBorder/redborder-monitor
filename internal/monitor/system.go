package monitor

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"time"
)

// SolveSystemCommand executes a shell command via /bin/sh -c and returns the trimmed first line of output.
// If execution fails or runs out of time, it returns "0" and the error.
func SolveSystemCommand(ctx context.Context, command string, timeoutSec int) (string, error) {
	if timeoutSec <= 0 {
		timeoutSec = 5
	}
	
	// Create context with timeout
	cmdCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "/bin/sh", "-c", command)
	
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	// Execute command
	err := cmd.Run()
	if err != nil {
		return "0", err
	}

	output := stdout.String()
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return trimmed, nil
		}
	}

	return "0", nil
}
