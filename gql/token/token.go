package token

import (
	"strings"
)

type TokenType string

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
)
const (
	IDENT TokenType = "IDENT"

	INT       = "Int"    // 1343456
	FLOAT     = "Float"  // 3.42
	STRING    = "String" // contents between " or """
	RAWSTRING = "RAWSTRING"
	NULL      = "Null"

	// GQL Input Values types
	FUNC    = "func"
	FILTER  = "filter"
	BOOLEAN = "B"
	// Operators
	ASSIGN   = "="
	PLUS     = "+"
	MINUS    = "-"
	BANG     = "!"
	MULTIPLY = "*"
	DIVIDE   = "/"

	// Boolean operators

	AND = "AND"
	OR  = "OR"
	NOT = "NOT"

	AS = "as"

	TRUE  = "true"
	FALSE = "false"

	LPAREN   = "("
	RPAREN   = ")"
	LBRACE   = "{"
	RBRACE   = "}"
	LBRACKET = "["
	RBRACKET = "]"

	ATSIGN = "@"
	DOLLAR = "$"
	EXPAND = "..."
	COLON  = ":"
	// delimiters
	RAWSTRINGDEL = `"""`
	STRINGDEL    = `"`

	BOM = "BOM"

	// predicate
	UID = "uid"

	// Functions
	RFUNC      = "RF"
	EQ         = "eq"
	LE         = "le"
	GE         = "ge"
	LT         = "lt"
	GT         = "gt"
	HAS        = "has"
	ANYOFTERMS = "anyofterms"
	ALLOFTERMS = "allofterms"
	// modifiers

	VAL   = "val"
	COUNT = "count"
	//
	AGFUNC = "AGGR"

	AVG = "avg"
	MIN = "min"
	MAX = "max"
	SUM = "sum"
)

type Pos struct {
	Line int
	Col  int
}

// Token is exposed via token package so lexer can create new instanes of this type as required.
type Token struct {
	Type    TokenType
	Literal string // string value of token - rune, string, int, float, bool
	Loc     Pos    // start position of token
	Illegal bool
}

var keywords = map[string]struct {
	Type TokenType
}{
	"and":    {AND},
	"or":     {OR},
	"not":    {NOT},
	"filter": {FILTER},
	"func":   {FUNC},
	// "eq":         {EQ},
	// "lt":         {LT},
	// "le":         {LE},
	// "gt":         {GT},
	// "ge":         {GE},
	// "allofterms": {ALLOFTERMS},
	"true":  {BOOLEAN},
	"false": {BOOLEAN},
	// suppored functions
	"eq":         {RFUNC},
	"le":         {RFUNC},
	"ge":         {RFUNC},
	"lt":         {RFUNC},
	"gt":         {RFUNC},
	"anyofterms": {RFUNC},
	"allofterms": {RFUNC},
	// supported modifer funcs
	"count": {COUNT},
	"uid":   {UID},
	"val":   {VAL},
	"has":   {HAS},
	"avg":   {AGFUNC},
	"sum":   {AGFUNC},
	"min":   {AGFUNC},
	"max":   {AGFUNC},
	//
	"as": {AS},
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok.Type
	}
	return IDENT
}
