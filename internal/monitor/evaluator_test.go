package monitor

import (
	"testing"
)

func resNormal(val float64) bool {
	return !resIsNaN(val) && !resIsInf(val)
}

func resIsNaN(val float64) bool {
	return val != val
}

func resIsInf(val float64) bool {
	return val > 1e300 || val < -1e300
}

func TestEvaluateExpression(t *testing.T) {
	tests := []struct {
		expr     string
		vars     map[string]float64
		expected float64
		wantErr  bool
	}{
		{"100-cpu_idle", map[string]float64{"cpu_idle": 30}, 70, false},
		{"100*(memory_total-memory_free)/memory_total", map[string]float64{"memory_total": 1000, "memory_free": 200}, 80, false},
		{"-5 + 10", map[string]float64{}, 5, false},
		{"(10 + 20) * 3", map[string]float64{}, 90, false},
		{"load_1 + load_5", map[string]float64{"load_1": 1.5, "load_5": 2.5}, 4, false},
		{"100 / zero", map[string]float64{"zero": 0}, 0, true},
	}

	for _, tt := range tests {
		res, err := EvaluateExpression(tt.expr, tt.vars)
		if tt.wantErr {
			if err != nil {
				continue
			}
			t.Logf("Result for %s was %v", tt.expr, res)
			if !resNormal(res) {
				continue
			}
			t.Errorf("EvaluateExpression(%s) expected error/NaN/Inf, got %f", tt.expr, res)
		} else {
			if err != nil {
				t.Errorf("EvaluateExpression(%s) unexpected error: %v", tt.expr, err)
			}
			if res != tt.expected {
				t.Errorf("EvaluateExpression(%s) = %f, expected %f", tt.expr, res, tt.expected)
			}
		}
	}
}

func TestEvaluateExpression_ComplexSuccess(t *testing.T) {
	tests := []struct {
		expr     string
		vars     map[string]float64
		expected float64
	}{
		{"(2 + 3) * 4", map[string]float64{}, 20},
		{"10 - -5", map[string]float64{}, 15},
		{"a * b + c", map[string]float64{"a": 2, "b": 3, "c": 4}, 10},
		{"1.5 + 2.5", map[string]float64{}, 4},
		{"-x", map[string]float64{"x": 10}, -10},
		{"-(a + b)", map[string]float64{"a": 2, "b": 3}, -5},
	}

	for _, tt := range tests {
		res, err := EvaluateExpression(tt.expr, tt.vars)
		if err != nil {
			t.Errorf("unexpected error evaluating '%s': %v", tt.expr, err)
		}
		if res != tt.expected {
			t.Errorf("EvaluateExpression('%s') = %f, expected %f", tt.expr, res, tt.expected)
		}
	}
}

func TestEvaluateExpression_UndefinedVariable(t *testing.T) {
	_, err := EvaluateExpression("a + b", map[string]float64{"a": 1})
	if err == nil {
		t.Fatal("expected error for undefined variable 'b', got nil")
	}
}

func TestEvaluateExpression_SyntaxErrors(t *testing.T) {
	invalidExprs := []string{
		"1 + @2",
		"1 + $foo",
		"1 # 2",
	}

	for _, expr := range invalidExprs {
		_, err := EvaluateExpression(expr, nil)
		if err == nil {
			t.Errorf("expected error for invalid expression '%s', got nil", expr)
		}
	}
}

func TestEvaluateExpression_MismatchedParentheses(t *testing.T) {
	invalidExprs := []string{
		"(1 + 2",
		"1 + 2)",
		"((1 + 2)",
	}

	for _, expr := range invalidExprs {
		_, err := EvaluateExpression(expr, nil)
		if err == nil {
			t.Errorf("expected error for mismatched parentheses in '%s', got nil", expr)
		}
	}
}

func TestEvaluateExpression_MissingOperands(t *testing.T) {
	invalidExprs := []string{
		"1 +",
		"* 2",
		"+",
		"1 - * 2",
	}

	for _, expr := range invalidExprs {
		_, err := EvaluateExpression(expr, nil)
		if err == nil {
			t.Errorf("expected error for missing operands in '%s', got nil", expr)
		}
	}
}

func TestExtractVariables_Success(t *testing.T) {
	vars, err := ExtractVariables("100 * (cpu_idle + cpu_system) / memory_total")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := map[string]bool{
		"cpu_idle":     true,
		"cpu_system":   true,
		"memory_total": true,
	}

	if len(vars) != len(expected) {
		t.Fatalf("expected %d variables, got %d", len(expected), len(vars))
	}

	for _, v := range vars {
		if !expected[v] {
			t.Errorf("unexpected variable extracted: %s", v)
		}
	}
}

func TestExtractVariables_Failure(t *testing.T) {
	_, err := ExtractVariables("100 + @invalid")
	if err == nil {
		t.Fatal("expected error on invalid expression, got nil")
	}
}
