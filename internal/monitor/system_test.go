package monitor

import (
	"context"
	"testing"
)

func TestSolveSystemCommand_Success(t *testing.T) {
	ctx := context.Background()
	out, err := SolveSystemCommand(ctx, "echo 42", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "42" {
		t.Errorf("expected '42', got '%s'", out)
	}
}

func TestSolveSystemCommand_MultiLine(t *testing.T) {
	ctx := context.Background()
	cmd := `echo "" && echo "  " && echo "  hello_world  " && echo "ignored"`
	out, err := SolveSystemCommand(ctx, cmd, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "hello_world" {
		t.Errorf("expected 'hello_world', got '%s'", out)
	}
}

func TestSolveSystemCommand_ExitFailure(t *testing.T) {
	ctx := context.Background()
	out, err := SolveSystemCommand(ctx, "false", 5)
	if err == nil {
		t.Fatal("expected error from failing command, got nil")
	}
	if out != "0" {
		t.Errorf("expected '0' on error, got '%s'", out)
	}
}

func TestSolveSystemCommand_Nonexistent(t *testing.T) {
	ctx := context.Background()
	out, err := SolveSystemCommand(ctx, "this_command_surely_does_not_exist_12345", 5)
	if err == nil {
		t.Fatal("expected error from non-existent command, got nil")
	}
	if out != "0" {
		t.Errorf("expected '0' on error, got '%s'", out)
	}
}

func TestSolveSystemCommand_Timeout(t *testing.T) {
	ctx := context.Background()
	out, err := SolveSystemCommand(ctx, "sleep 3", 1)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if out != "0" {
		t.Errorf("expected '0' on error, got '%s'", out)
	}
}
