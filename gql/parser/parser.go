package parser

import (
	"errors"
	"fmt"
	_ "os"
	"strconv"
	"strings"

	"github.com/DynamoGraph/expression"

	"github.com/DynamoGraph/gql/ast"
	"github.com/DynamoGraph/gql/lexer"
	"github.com/DynamoGraph/gql/token"
)

type (
	gqlFunc map[token.TokenType]ast.Gqlf

	Parser struct {
		l *lexer.Lexer

		extend bool

		abort     bool
		stmtType  string
		curToken  *token.Token
		peekToken *token.Token

		perror []error
	}
)

var (
	opt bool = true

	rootFunc gqlFunc
)

//
func init() {

	rootFunc = make(gqlFunc)
	// regiser Parser methods for each statement type
	registerFn(token.EQ, eq)
	registerFn(token.HAS, has)
}

func registerFn(t token.TokenType, f ast.Gqlf) {
	rootFunc[t] = f
}

func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		l: l,
	}

	// Read two tokens, to initialise curToken and peekToken
	p.nextToken()
	p.nextToken()
	//
	// remove cacheClar before releasing..
	//
	//ast.CacheClear()
	return p
}

// astsitory of all types defined in the graph

func (p *Parser) loc() *ast.Loc {
	//l,c  := p.l.Loc()
	loc := p.curToken.Loc
	return &ast.Loc{Line: loc.Line, Column: loc.Col}
}

func (p *Parser) printToken(s ...string) {
	if len(s) > 0 {
		fmt.Printf("** Current Token: [%s] %s %s %v %s [%s]\n", s[0], p.curToken.Type, p.curToken.Literal, "Next Token:  ", p.peekToken.Type, p.peekToken.Literal)
	} else {
		fmt.Println("** Current Token: ", p.curToken.Type, p.curToken.Literal, "Next Token:  ", p.peekToken.Type, p.peekToken.Literal)
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
		s += fmt.Sprintf(" at line: %d, column: %d", p.curToken.Loc.Line, p.curToken.Loc.Col)
	}
	e := errors.New(s)
	p.perror = append(p.perror, e)
	return e
}

func (p *Parser) nextToken(s ...string) {
	p.curToken = p.peekToken

	p.peekToken = p.l.NextToken() // get another token from lexer:    [,+,(,99,Identifier,keyword etc.
	if len(s) > 0 {
		fmt.Printf("** Current Token: [%s] %s %s %s %s %s\n", s[0], p.curToken.Type, p.curToken.Literal, "Next Token:  ", p.peekToken.Type, p.peekToken.Literal)
	}
	if p.curToken != nil {
		if p.curToken.Illegal {
			p.addErr(fmt.Sprintf("Illegal %s token, [%s]", p.curToken.Type, p.curToken.Literal))
		}
	}
}

// ===========================================================================================================

func (p *Parser) ParseInput() (*ast.RootStmt, []error) { // TODO: turn into ParseDocument

	if p.curToken.Type == token.LBRACE {
		p.nextToken("read over LBRACE")
	} else {
		p.addErr("document must start with a {")
		return nil, p.perror
	}

	x := p.parseRootStmt()

	return x, p.perror

}

func (p *Parser) parseRootStmt() *ast.RootStmt {
	// Types: query, mutation, subscription

	stmt := &ast.RootStmt{}

	p.parseName(stmt, opt).parseFunction(stmt).parseFilter(stmt) //.parseSelectionSet(stmt)

	if p.hasError() {
		return nil
	}

	return stmt

}

// parseName will validate input data against GraphQL name requirement and assign to a field called Name
func (p *Parser) parseName(f ast.NameAssigner, optional ...bool) *Parser { // type f *ast.Executable,  f=passedInArg converts argument to f
	// check if appropriate thing to do
	if p.hasError() {
		return p
	}
	//p.nextToken()
	// alternative tokens, LPAREN+variableDef, ATSIGN+directive, LBRACE-selectionSet
	if p.curToken.Type == token.IDENT {

		f.AssignName(p.curToken.Literal, p.loc(), &p.perror)
		p.nextToken() // read over name

	} else if len(optional) == 0 {
		p.addErr(fmt.Sprintf(`Expected an identifer got %s of %s`, p.curToken.Type, p.curToken.Literal))

	}

	return p
}

// eq(predicate, value)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

//me(func: eq(count(genre), 13)) {
//  name@en
//  genre {
//    name@en
//  }
//}
func (p *Parser) parseFunction(s *ast.RootStmt) *Parser {

	// 	type GQLFunc struct {
	// 	Name      string      // eq, le, lt, anyofterms, someofterms
	//  Lang	  string
	// 	Predicate string      // Name,
	// 	Value     interface{} // scalar int, bool, float, string. List of string. List of $var, string.
	// 	Modifier  string      // count(), val()
	// }

	if p.hasError() {
		return p
	}

	rf := &s.RootFunc

	//	tok = p.nextToken()

	// (func:
	for _, v := range []token.TokenType{token.LPAREN, token.FUNC, token.COLON} {
		if p.curToken.Type != v {
			p.addErr(fmt.Sprintf(`Expected a %s got %s instead`, v, p.curToken.Literal))
			return p
		}
		p.nextToken()
	}
	//
	fmt.Printf("1: %#v\n", p.curToken)
	// eq, lt, gt, has, anyofterms, someofterms...
	if p.curToken.Type != token.RFUNC {
		p.addErr(fmt.Sprintf(`Expected a function  got %s instead`, p.curToken.Literal))
		return p
	}
	switch x := p.curToken.Literal; x {
	case token.EQ:
		rf.Name = x
		if f, ok := rootFunc[token.TokenType(x)]; !ok {
			p.addErr(fmt.Sprintf(`func %q is not registered`, p.curToken.Literal))
			return p
		} else {
			rf.F = f
		}
	case token.LT:
		rf.Name = x
		if f, ok := rootFunc[token.TokenType(x)]; !ok {
			p.addErr(fmt.Sprintf(`func %q is not registered`, p.curToken.Literal))
			return p
		} else {
			rf.F = f
		}
	}
	p.nextToken() // read over func
	// (
	if p.curToken.Type != token.LPAREN {
		p.addErr(fmt.Sprintf(`Expected (  got %s`, p.curToken.Literal))
		return p
	}
	p.nextToken() // read over (
	switch p.curToken.Type {

	case token.IDENT:
		rf.Predicate = p.curToken.Literal
		p.nextToken() // read over identier

	case token.MFUNC:
		rf.Modifier = p.curToken.Literal
		p.nextToken() // read over modifier
		if p.curToken.Type != token.LPAREN {
			p.addErr(fmt.Sprintf(`Expected (  got %s`, p.curToken.Literal))
			return p
		}
		p.nextToken() // read over (
		if p.curToken.Type != token.IDENT {
			p.addErr(fmt.Sprintf(`Expected identifier got %s`, p.curToken.Literal))
			return p
		}
		rf.Predicate = p.curToken.Literal
		p.nextToken() // read over identier
		p.nextToken() // read over )

	default:
		p.addErr(fmt.Sprintf(`Expected an identifier or modifer function got %s`, p.curToken.Literal))
		return p
	}
	// set default language
	//	s.Lang = "en"
	// if p.curToken.Type == token.ATSIGN {
	// 	p.nextToken() // read over @
	// 	if p.curToken.Type != token.IDENT {
	// 		p.addErr(fmt.Sprintf(`Expected a language identifer got %s instead`, p.curToken.Literal))
	// 		return
	// 	}
	// 	s.Lang = p.curToken.Literal
	// 	p.nextToken() // read over language
	// }
	//
	switch p.curToken.Type {
	case token.STRING:
		rf.Value = p.curToken.Literal
	case token.INT:
		i, _ := strconv.Atoi(p.curToken.Literal)
		rf.Value = i
	case token.FLOAT:
		f, err := strconv.ParseFloat(p.curToken.Literal, 64)
		if err != nil {
			p.addErr(fmt.Sprintf(`Errored in converting literal, %q, to float64. %s`, p.curToken.Literal, err.Error()))
			return p
		}
		rf.Value = f
	default:
		p.addErr(fmt.Sprintf(`Expected a string or number got %s instead`, p.curToken.Literal))
		return p
	}
	p.nextToken() // read over value

	for i := 0; i < 2; i++ {
		if p.curToken.Type != token.RPAREN {
			p.addErr(fmt.Sprintf(`Expected )  got %s instead`, p.curToken.Literal))
			return p
		}
		p.nextToken() // read over )
	}
	return p

}

func (p *Parser) parseFilter(s *ast.RootStmt) *Parser {

	// @filter(allofterms(name@en, "jones indiana") OR allofterms(name@en, "jurassic park"))
	fmt.Println("in parseFilter: ", p.curToken.Literal)
	if p.hasError() || p.curToken.Type != token.ATSIGN {
		return p
	}
	p.nextToken() // read over @
	exprInput := p.l.Remaining()

	if p.curToken.Type != token.FILTER {
		p.addErr(fmt.Sprintf(`Expected (  got %s instead`, p.curToken.Literal))
		return p
	}
	p.nextToken() // read over filter
	fmt.Println("Input REmaining2: ", exprInput)
	if p.curToken.Type != token.LPAREN {
		p.addErr(fmt.Sprintf(`Expected (  got %s instead`, p.curToken.Literal))
		return p
	}
	s.Filter = expression.New(exprInput) // TODO should return new rLoc, cLoc
	return p
}
