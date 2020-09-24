package token

import (
	"strings"
)

type TokenType string
type TokenCat string

const (
	IDENT TokenType = "IDENT"
)
const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	// GQL Input Values types
	ID        = "ID"
	INT       = "Int"    // 1343456
	FLOAT     = "Float"  // 3.42
	STRING    = "String" // contents between " or """
	RAWSTRING = "RAWSTRING"
	NULL      = "Null"
	ENUM      = "Enum"
	LIST      = "List"
	BOOLEAN   = "Boolean"
	FILTER    = "filter"

	// Category
	VALUE    = "VALUE"
	NONVALUE = "NONVALUE"

	// Operators
	ASSIGN   = "="
	PLUS     = "+"
	MINUS    = "-"
	BANG     = "!"
	MULTIPLY = "*"
	DIVIDE   = "/"

	ATSIGN = "@"

	// Boolean operators

	AND = "AND"
	OR  = "OR"
	NOT = "NOT"

	TRUE  = "true"
	FALSE = "false"

	LPAREN   = "("
	RPAREN   = ")"
	LBRACE   = "{"
	RBRACE   = "}"
	LBRACKET = "["
	RBRACKET = "]"

	EXPAND = "..."
	// delimiters
	RAWSTRINGDEL = `"""`

	STRINGDEL = `"`

	BOM = "BOM"

	// functions

	EQ         = "eq"
	LE         = "le"
	GE         = "ge"
	LT         = "lt"
	GT         = "gt"
	ALLOFTERMS = "allofterms"

	// Keywords

)

type Pos struct {
	Line int
	Col  int
}

// Token is exposed via token package so lexer can create new instanes of this type as required.
type Token struct {
	Cat          TokenCat
	Type         TokenType
	IsScalarType bool
	Literal      string // string value of token - rune, string, int, float, bool
	Loc          Pos    // start position of token
	Illegal      bool
}

var keywords = map[string]struct {
	Type TokenType
}{
	"id":         {ID},
	"and":        {AND},
	"or":         {OR},
	"not":        {NOT},
	"filter":     {FILTER},
	"eq":         {EQ},
	"lt":         {LT},
	"le":         {LE},
	"gt":         {GT},
	"ge":         {GE},
	"allofterms": {ALLOFTERMS},
	"true":       {TRUE},
	"false":      {FALSE},
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok.Type
	}
	return IDENT
}
