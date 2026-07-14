package monitor

import (
	"fmt"
	"math"
	"strconv"
	"unicode"
)

type TokenType int

const (
	TokenNumber TokenType = iota
	TokenVariable
	TokenOperator
	TokenLParen
	TokenRParen
)

type Token struct {
	Type  TokenType
	Value string
}

func getPrecedence(op string) int {
	switch op {
	case "+", "-":
		return 1
	case "*", "/":
		return 2
	case "u-": // Unary minus
		return 3
	default:
		return 0
	}
}

func tokenize(expr string) ([]Token, error) {
	var tokens []Token
	i := 0
	n := len(expr)

	for i < n {
		ch := expr[i]
		if unicode.IsSpace(rune(ch)) {
			i++
			continue
		}

		if unicode.IsDigit(rune(ch)) || ch == '.' {
			start := i
			for i < n && (unicode.IsDigit(rune(expr[i])) || expr[i] == '.') {
				i++
			}
			tokens = append(tokens, Token{Type: TokenNumber, Value: expr[start:i]})
			continue
		}

		if unicode.IsLetter(rune(ch)) || ch == '_' {
			start := i
			for i < n && (unicode.IsLetter(rune(expr[i])) || unicode.IsDigit(rune(expr[i])) || expr[i] == '_') {
				i++
			}
			tokens = append(tokens, Token{Type: TokenVariable, Value: expr[start:i]})
			continue
		}

		if ch == '+' || ch == '-' || ch == '*' || ch == '/' {
			tokens = append(tokens, Token{Type: TokenOperator, Value: string(ch)})
			i++
			continue
		}

		if ch == '(' {
			tokens = append(tokens, Token{Type: TokenLParen, Value: "("})
			i++
			continue
		}

		if ch == ')' {
			tokens = append(tokens, Token{Type: TokenRParen, Value: ")"})
			i++
			continue
		}

		return nil, fmt.Errorf("unexpected character in math expression: %c", ch)
	}

	return tokens, nil
}

func shuntingYard(tokens []Token) ([]Token, error) {
	var queue []Token
	var stack []Token
	prevWasOpOrStart := true

	for _, tok := range tokens {
		switch tok.Type {
		case TokenNumber, TokenVariable:
			queue = append(queue, tok)
			prevWasOpOrStart = false
		case TokenLParen:
			stack = append(stack, tok)
			prevWasOpOrStart = true
		case TokenRParen:
			found := false
			for len(stack) > 0 {
				top := stack[len(stack)-1]
				if top.Type == TokenLParen {
					stack = stack[:len(stack)-1]
					found = true
					break
				}
				queue = append(queue, top)
				stack = stack[:len(stack)-1]
			}
			if !found {
				return nil, fmt.Errorf("mismatched parentheses")
			}
			prevWasOpOrStart = false
		case TokenOperator:
			op := tok.Value
			if op == "-" && prevWasOpOrStart {
				tok.Value = "u-"
			}

			for len(stack) > 0 {
				top := stack[len(stack)-1]
				if top.Type == TokenLParen {
					break
				}

				p1 := getPrecedence(tok.Value)
				p2 := getPrecedence(top.Value)

				// Unary minus is right-associative, others are left-associative
				if p1 < p2 || (p1 == p2 && tok.Value != "u-") {
					queue = append(queue, top)
					stack = stack[:len(stack)-1]
				} else {
					break
				}
			}
			stack = append(stack, tok)
			prevWasOpOrStart = true
		}
	}

	for len(stack) > 0 {
		top := stack[len(stack)-1]
		if top.Type == TokenLParen {
			return nil, fmt.Errorf("mismatched parentheses")
		}
		queue = append(queue, top)
		stack = stack[:len(stack)-1]
	}

	return queue, nil
}

func evaluateRPN(rpn []Token, vars map[string]float64) (float64, error) {
	var stack []float64

	for _, tok := range rpn {
		switch tok.Type {
		case TokenNumber:
			val, err := strconv.ParseFloat(tok.Value, 64)
			if err != nil {
				return 0, err
			}
			stack = append(stack, val)
		case TokenVariable:
			val, ok := vars[tok.Value]
			if !ok {
				return 0, fmt.Errorf("undefined variable: %s", tok.Value)
			}
			stack = append(stack, val)
		case TokenOperator:
			if tok.Value == "u-" {
				if len(stack) < 1 {
					return 0, fmt.Errorf("invalid unary minus operator context")
				}
				val := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				stack = append(stack, -val)
				continue
			}

			if len(stack) < 2 {
				return 0, fmt.Errorf("missing operands for operator %s", tok.Value)
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]

			var res float64
			switch tok.Value {
			case "+":
				res = a + b
			case "-":
				res = a - b
			case "*":
				res = a * b
			case "/":
				if b == 0 {
					res = math.NaN()
				} else {
					res = a / b
				}
			}
			stack = append(stack, res)
		}
	}

	if len(stack) != 1 {
		return 0, fmt.Errorf("invalid math expression evaluation result stack size: %d", len(stack))
	}
	return stack[0], nil
}

// EvaluateExpression tokenizes, converts to postfix, and evaluates a mathematical expression.
func EvaluateExpression(expr string, vars map[string]float64) (float64, error) {
	tokens, err := tokenize(expr)
	if err != nil {
		return 0, err
	}
	rpn, err := shuntingYard(tokens)
	if err != nil {
		return 0, err
	}
	return evaluateRPN(rpn, vars)
}

// ExtractVariables finds all variables mentioned in a math expression.
func ExtractVariables(expr string) ([]string, error) {
	tokens, err := tokenize(expr)
	if err != nil {
		return nil, err
	}
	var vars []string
	seen := make(map[string]bool)
	for _, tok := range tokens {
		if tok.Type == TokenVariable {
			if !seen[tok.Value] {
				seen[tok.Value] = true
				vars = append(vars, tok.Value)
			}
		}
	}
	return vars, nil
}
