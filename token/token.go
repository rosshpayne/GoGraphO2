package token

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
	OBJECT    = "Object"

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

	// Boolean operators

	AND = "AND"
	OR  = "OR"
	NOT = "NOT"

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
	Type         TokenType
	Cat          TokenCat
	IsScalarType bool
}{}

func LookupIdent(ident string) (TokenType, TokenCat, bool) {
	if tok, ok := keywords[ident]; ok {
		return tok.Type, tok.Cat, tok.IsScalarType
	}
	return IDENT, NONVALUE, false
}
