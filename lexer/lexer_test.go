package lexer

import (
	"fmt"
	"testing"

	"github.com/Dynograph/token"
)

func TestNextToken(t *testing.T) {
	input := "((3*(7-3)   * 4)*2) + (5+2)*-8*2)*3"

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.LPAREN, "("},
		{token.LPAREN, "("},
		{token.INT, "3"},
		{token.MULTIPLY, "*"},
		{token.LPAREN, "("},
		{token.INT, "7"},
		{token.MINUS, "-"},
		{token.INT, "3"},
		{token.RPAREN, ")"},
		{token.MULTIPLY, "*"},
		{token.INT, "4"},
		{token.RPAREN, ")"},
		{token.MULTIPLY, "*"}, // 10
		{token.INT, "2"},
		{token.RPAREN, ")"},
		{token.PLUS, "+"},
		{token.LPAREN, "("},
		{token.INT, "5"}, //15
		{token.PLUS, "+"},
		{token.INT, "2"}, //20
		{token.RPAREN, ")"},
		{token.MULTIPLY, "*"},
		{token.MINUS, "-"},
		{token.INT, "8"}, //20
		{token.MULTIPLY, "*"},
		{token.INT, "2"},
		{token.RPAREN, ")"},
		{token.MULTIPLY, "*"},
		{token.INT, "3"},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()
		//	fmt.Printf("%v\n", tok)
		fmt.Println(tok.Literal)
		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q Error: %s",
				i, tt.expectedType, tok.Type, l.Error())
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q Error: %s",
				i, tt.expectedLiteral, tok.Literal, l.Error())
		}
	}
}
