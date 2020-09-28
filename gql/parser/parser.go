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
	"github.com/DynamoGraph/gql/variable"
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

	p.parseName(stmt, opt).parseFunction(stmt).parseFilter(&stmt.Filter).parseSelection(stmt.Select)

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

	// eq(predicate, value)
	// eq(val(varName), value)
	// eq(predicate, val(varName))
	// eq(count(predicate), value)
	// eq(predicate, [val1, val2, ..., valN])
	// eq(predicate, [$var1, "value", ..., $varN])

	// type GQLFunc struct {
	// 	FName Name_ // for String() purposes
	// 	F     funcs.FuncT
	// 	Farg  funcs.Arg1
	// 	//inner function val(<variable>), count(<uidpred>)
	// 	IFName Name_            // for String() purposes
	// 	IF     funcs.InnerFuncT // either count, var
	// 	IFarg  funcs.InnerArg   // either uidPred, variable
	// 	//
	// 	Value interface{} // scalar int, bool, float, string. List of string. List of $var, string.
	// }

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
		rf.FName = x
		rf.F = funcs.RootEQ

	case token.LT:
		rf.FName = x
		rf.F = funcs.RootLT
		//
		// ...
		//
	}
	p.nextToken() // read over func
	// (
	if p.curToken.Type != token.LPAREN {
		p.addErr(fmt.Sprintf(`Expected (  got %s`, p.curToken.Literal))
	}
	p.nextToken() // read over (

	switch p.curToken.Type {

	case token.IDENT:
		if !cache.IsScalarPred(p.curToken.Literal) {
			p.addErr(fmt.Sprintf(`Predicate %s is not a scalar in any type`, p.curToken.Literal))
			return
		}
		rf.Farg = funcs.ScalarPred(p.curToken.Literal)
		p.nextToken() // read over identier

	case token.MFUNC:
		switch p.curToken.Literal {
		case token.COUNT:
			rf.IFName = token.COUNT
			rf.IF = funcs.Count
		case token.VAL:
			rf.IFName = token.VAL
			rf.IF = funcs.Val
		}
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
		if rf.IF == funcs.Count {
			rf.IFarg = funcs.UIDPred(p.curToken.Literal)
		} else {
			rf.IFarg = funcs.Variable(p.curToken.Literal)
		}
		p.nextToken() // read over identier
		p.nextToken() // read over )

	default:
		p.addErr(fmt.Sprintf(`Expected an identifier or modifer function got %s`, p.curToken.Literal))
		return p
	}
	// parse value
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

	case token.LBRACKET:
		var vs []interface{} // int, float, string, $var, []int,float,string,$var

		for p.nextToken(); p.curToken.Type != token.RBRACKET; p.nextToken() {
			switch p.curToken.Type {
			case token.STRING:
				vs = append(vs, p.curToken.Literal)
			case token.INT:
				v, _ := strconv.Atoi(p.curToken.Literal)
				vs = append(vs, v)
			case token.FLOAT:
				v, err := strconv.ParseFloat(p.curToken.Literal, 64)
				if err != nil {
					p.addErr(fmt.Sprintf(`Errored in converting literal, %q, to float64. %s`, p.curToken.Literal, err.Error()))
					return p
				}
				vs = append(vs, v)
			case token.DOLLAR:
				p.nextToken() // read over $
				if p.curToken.Type == token.IDENT {
					vs = append(vs, ast.VarName(p.curToken.Literal))
				} else {
					p.addErr(fmt.Sprintf(`Expected variable name got %s`, p.curToken.Literal))
				}
			}
		}
		p.nextToken() // read over ]
	}

	for i := 0; i < 2; i++ {
		if p.curToken.Type != token.RPAREN {
			p.addErr(fmt.Sprintf(`Expected )  got %s instead`, p.curToken.Literal))
			return p
		}
		p.nextToken() // read over )
	}
	return p

}

func (p *Parser) parseFilter(s **expr.Expression) *Parser {

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
	*s = expression.New(exprInput) // TODO should return new rLoc, cLoc
	return p
}

func (p *Parser) parseSelection(s ast.SelectList) *Parser {

	for p.curToken.NextToken; p.curToken.Type != token.RBRACE; p.curToken.NextToken {

		e = &ast.EdgeT{}

		p.parseVarAlias(e).parseEdge(e)

		s = append(s, e)

	}
}

func (p *Parser) parseVarAlias(e *ast.EdgeT) *Parser {

	if p.curToken.Type == token.IDENT && p.peekToken.Type == token.COLON {

		e.AssignName(p.curToken.Literal, p.curToken.Loc())

	}
	if p.curToken.Type == token.IDENT && p.peekToken.Type == token.AS {

		e.AssignVarName(p.curToken.Literal, p.curToken.Loc())

		st := &variable.Item{Name: p.curToken.Literal, Edge: e}
		st.Add()
	}

	p.nextToken() // read over IDENT
	p.nextToken() // read over : or as

	return p
}

// name@en
// count(actor.film)
// directors(func: gt(count(director.film), 5)) {
//     totalDirectors : count(uid) //counts the number of UIDs matched in the enclosing block.

// var(func: allofterms(name@en, "sin city")) {
//     starring {
//       actors as performance.actor {
//         totalRoles as count(actor.film)
//       }
//     }
//   }

//   edmw(func: uid(actors), orderdesc: val(totalRoles)) {
//     name@en
//     totalRoles : val(totalRoles)
//   }

//me(func: allofterms(name@en, "Jean-Pierre Jeunet")) {
//  name@fr
//  director.film(orderasc: initial_release_date) {
//    name@fr                                                // scalar pred
//    name@en
//    initial_release_date
//  }
//}

// {
//   genres as var(func: has(~genre)) {
//     ~genre {                                             //uidpred
//       numGenres as count(genre)
//     }
//   }

//   genres(func: uid(genres), orderasc: name@en) {
//     name@en
//     ~genre (orderdesc: val(numGenres), first: 5) {
//       name@en
//       genres : val(numGenres)
//     }
//   }
// }

// {
//   ID as var(func: allofterms(name@en, "Steven")) @filter(has(director.film)) {
//     director.film {
//       num_actors as count(starring)
//     }
//     average as avg(val(num_actors))
//   }

//   films(func: uid(ID)) {
//     director_id : uid
//     english_name : name@en
//     average_actors : val(average)
//     num_films : count(director.film)

//     films : director.film {
//       name : name@en
//       english_name : name@en
//       french_name : name@fr
//     }
//   }
// }
// {
//   me(func: eq(name@en, "Steven Spielberg")) @filter(has(director.film)) {
//     name@en
//     director.film @filter(allofterms(name@en, "jones indiana"))  {
//       name@en
//     }
//   }
// }

func (p *Parser) parseEdge(e *ast.EdgeT) *Parser {
	// edge can be
	// * <scalar-predicate>
	// * <uid-predicate> { SelectList }
	// * <uid predicate> @filter { SelectList }
	// * avg(val(<variable>)), sum, min, max
	// * val(<variable>)
	// * variable as <uidPred>     // query variable
	// * variable as <scalarPred>  // value variable
	// * uid
	switch p.curToken.Type {

	case IDENT:
		// * <scalar-predicate>
		// * <uid-predicate> { SelectList }
		// * <uid predicate> @filter { SelectList }
		ident := p.curToken.Literal
		p.nextToken() // read over IDENT
		if p.peekToken.Type == token.ATSIGN || p.peekToken.Type == token.LBRACE {
			if !cache.IsUidPred(ident) {
				p.addErr("%q is not a uid-predicate", ident)
			}
			//
			e.Edge = &ast.UidPred{}
			p.parseFilter(&e.Filter).parseSelection(e.Select)

		} else {
			if !cache.IsScalarPred(ident) {
				p.addErr("%q is not a uid-pred", x)
			}
			//
			e.Edge = &ast.ScalarPred{}
			p.parseName(e.Edge)
		}

	case AGFUNC:
		// * avg(val(<variable>)), sum, max, min,
		// * val(<variable>)

		e.Edge = &ast.AggrFunc{}
		e.Edge.AssignName(p.nextToken.Literal, p.nextToken.Loc())
		p.nextToken() // read over count
		if p.curToken.Type != token.LPAREN {
			p.addErr("Expected ( got %s", p.curToken.Literal)
		}
		p.nextToken() // read over (
		//
		if p.curToken.Type != token.VAL {
			p.addErr(`Expected "val" got %s`, p.curToken.Literal)
		}
		p.nextToken() // read over val
		p.nextToken() // read over (
		//
		v := ast.Variable{}
		v.AssignName(p.curToken.Literal, p.curToken.Loc())
		e.Edge.Arg = v
		//to execute variable.Count(p.nextToken.Literal)
		p.nextToken() // read over )

	case COUNT:
		// actors as performance.actor {
		//   totalRoles as count(actor.film)
		// }
		// * abc as count(<uid predicate>|<UID>)
		// totalDirectors : count(uid)
		e.Edge = &ast.CountFunc{}
		e.Edge.AssignName(p.nextToken.Literal, p.nextToken.Loc())
		p.nextToken() // read over count
		switch p.curToken.Type {

		case token.IDENT:
			if !cache.IsUidPred(p.curToken.Literal) {
				p.addErr("%q is not a uid-predicate", ident)
			}
			//
			pred := &UidPred{}
			pred.AssignName(p.curToken.Literal, p.curToken.Loc())
			e.Edge.Arg = pred

		case token.UID:
			e.Edge.Arg = UID{}
		}

	case VAL:
		// numRoles : val(roles)
		p.nextToken() // read over val
		p.nextToken() // read ove (
		if p.curToken.Type != token.DOLLAR {
			p.addErr("Expected ( got %s", p.curToken.Literal)
		}
		p.nextToken() // read over variable
		e.Edge = &ast.Variable{}
		e.Edge.AssignName(p.nextToken.Literal, p.nextToken.Loc())
		p.nextToken() // read ove )

	case UID:
		e.Edge = &ast.UID{}
		p.nextToken() // read over uid
	}
}
