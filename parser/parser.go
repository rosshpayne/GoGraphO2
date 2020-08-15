package parser

import (
	"errors"
	"fmt"
	_ "os"
	"strings"

	"github.com/DynamoGraph/lexer"
	"github.com/DynamoGraph/token"
)

const (
	cErrLimit  = 8 // how many parse errors are permitted before processing stops
	Executable = 'E'
	TypeSystem = 'T'
	defaultDoc = "DefaultDoc"
)

type (
	Parser struct {
		l *lexer.Lexer

		extend bool

		abort     bool
		stmtType  string
		CurToken  *token.Token
		PeekToken *token.Token

		perror []error
	}
)

var (
//	enumRepo      ast.EnumRepo

)

func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		l: l,
	}

	// Read two tokens, to initialise CurToken and PeekToken
	p.NextToken()
	p.NextToken()
	//
	// remove cacheClar before releasing..
	//
	//ast.CacheClear()
	return p
}

// astsitory of all types defined in the graph

func (p *Parser) printToken(s ...string) {
	if len(s) > 0 {
		fmt.Printf("** Current Token: [%s] %s %s %s %v %s %s [%s]\n", s[0], p.CurToken.Type, p.CurToken.Literal, p.CurToken.Cat, p.CurToken.IsScalarType, "Next Token:  ", p.PeekToken.Type, p.PeekToken.Literal)
	} else {
		fmt.Println("** Current Token: ", p.CurToken.Type, p.CurToken.Literal, p.CurToken.Cat, "Next Token:  ", p.PeekToken.Type, p.PeekToken.Literal)
	}
}

func (p *Parser) hasError() bool {
	if len(p.perror) > 17 || p.abort {
		return true
	}
	return false
}

// addErr appends to error slice held in parser.
func (p *Parser) addErr(s string) error {
	if strings.Index(s, " at line: ") == -1 {
		s += fmt.Sprintf(" at line: %d, column: %d", p.CurToken.Loc.Line, p.CurToken.Loc.Col)
	}
	e := errors.New(s)
	p.perror = append(p.perror, e)
	return e
}

func (p *Parser) NextToken(s ...string) {
	p.CurToken = p.PeekToken

	p.PeekToken = p.l.NextToken() // get another token from lexer:    [,+,(,99,Identifier,keyword etc.
	if len(s) > 0 {
		fmt.Printf("** Current Token: [%s] %s %s %s %s %s %s\n", s[0], p.CurToken.Type, p.CurToken.Literal, p.CurToken.Cat, "Next Token:  ", p.PeekToken.Type, p.PeekToken.Literal)
	}
	if p.CurToken != nil {
		if p.CurToken.Illegal {
			p.addErr(fmt.Sprintf("Illegal %s token, [%s]", p.CurToken.Type, p.CurToken.Literal))
		}
	}
}
