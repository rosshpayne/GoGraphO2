package parser

import (
	"errors"
	"fmt"
	_ "os"
	"strconv"
	"strings"

	expr "github.com/DynamoGraph/gql/expression"

	"github.com/DynamoGraph/cache"

	"github.com/DynamoGraph/gql/ast"
	"github.com/DynamoGraph/gql/lexer"
	"github.com/DynamoGraph/gql/token"
	"github.com/DynamoGraph/gql/variable"
)

type (
	gqlFunc map[string]ast.FuncT

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
	registerFn(token.EQ, ast.EQ)
	registerFn(token.GT, ast.GT)
	registerFn(token.ALLOFTERMS, ast.ALLOFTERMS)
	//	registerFn(token.HAS, has)
}

func registerFn(t string, f ast.FuncT) {
	rootFunc[t] = f
}

func New(input string) *Parser {

	l := lexer.New(input)
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

// func New(l *lexer.Lexer) *Parser {
// 	p := &Parser{
// 		l: l,
// 	}

// 	// Read two tokens, to initialise curToken and peekToken
// 	p.nextToken()
// 	p.nextToken()
// 	//
// 	// remove cacheClar before releasing..
// 	//
// 	//ast.CacheClear()
// 	return p
// }

// astsitory of all types defined in the graph

// func (p *Parser) loc() *token.Pos. {
// 	//l,c  := p.l.loc()
// 	loc := p.curToken.Loc
// 	return &ast.Loc{Line: loc.Line, Column: loc.Col}
// }

func (p *Parser) printToken(s ...string) {
	if len(s) > 0 {
		fmt.Printf("** Current Token: [%s] %s %s %v %s [%s]\n", s[0], p.curToken.Type, p.curToken.Literal, "Next Token:  ", p.peekToken.Type, p.peekToken.Literal)
	} else {
		fmt.Println("** Current Token: ", p.curToken.Type, p.curToken.Literal, "Next Token:  ", p.peekToken.Type, p.peekToken.Literal)
	}
}

func (p *Parser) hasError() bool {
	if len(p.perror) > 3 || p.abort {
		return true
	}
	return false
}

// addErr appends to error slice held in parser.
func (p *Parser) addErr(s string) error {
	fmt.Println("addErr: ", s)
	if strings.Index(s, " at line: ") == -1 {
		s += fmt.Sprintf(" at line: %d, column: %d", p.curToken.Loc.Line, p.curToken.Loc.Col)
	}
	e := errors.New(s)
	p.perror = append(p.perror, e)
	return e
}

func (p *Parser) nextToken(s ...string) {
	p.curToken = p.peekToken
	if len(s) > 0 {
		fmt.Println("nextToken: ", s[0])
	}
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

func (p *Parser) ParseDocument() (*ast.RootStmt, []error) {
	return p.ParseInput()
}

func (p *Parser) ParseInput() (*ast.RootStmt, []error) { // TODO: turn into ParseDocument

	if p.curToken.Type == token.LBRACE {
		p.nextToken("read over LBRACE")
	} else {
		p.addErr("document must start with a {")
		return nil, p.perror
	}

	blk := p.parseRootStmt()

	if len(blk) > 0 {
		return blk[0], p.perror
	}
	return nil, p.perror

}

func (p *Parser) parseRootStmt() []*ast.RootStmt {
	// Types: query, mutation, subscription
	var block []*ast.RootStmt

	for p.curToken.Type != token.EOF {
		stmt := &ast.RootStmt{}

		p.parseVarName(stmt, opt).parseFunction(stmt).parseFilter(stmt).parseSelection(stmt)

		if p.hasError() {
			return nil
		}
		fmt.Printf("stmt: %#v\n\n", *stmt)
		fmt.Printf("curToken: %#v\n", p.curToken)
		if p.curToken.Type == token.RBRACE {
			p.nextToken()
		}
		//preds := stmt.RetrievePredicates()
		// for _, v := range preds {
		// 	fmt.Println("predicates: ", v)
		// }
		block = append(block, stmt)
	}

	return block

}

// func (p *Parser) parsePredicates(r *ast.RootStmt) {

// 	r.RetrievePredicates()
// }

func (p *Parser) parseName(f ast.NameAssigner, optional ...bool) *Parser { // type f *ast.Executable,  f=passedInArg converts argument to f
	return p.parseVarName(f, optional...)
}

// parseName will validate input data against GraphQL name requirement and assign to a field called Name
func (p *Parser) parseVarName(f ast.NameAssigner, optional ...bool) *Parser { // type f *ast.Executable,  f=passedInArg converts argument to f
	// check if appropriate thing to do
	if p.hasError() {
		return p
	}
	//p.nextToken()
	// alternative tokens, LPAREN+variableDef, ATSIGN+directive, LBRACE-selectionSet
	if p.curToken.Type == token.IDENT && p.peekToken.Type == token.AS {
		// var specified

		var v = &ast.Variable{}
		v.AssignName(p.curToken.Literal, p.curToken.Loc)
		if x, ok := f.(*ast.RootStmt); !ok {
			panic(fmt.Errorf("pareVarName: Not a  RootStmt"))
		} else {
			x.Var = v
		}
		p.nextToken() // read over var name
		p.nextToken() // read over as

	}
	if p.curToken.Type == token.IDENT {

		f.AssignName(p.curToken.Literal, p.curToken.Loc)
		p.nextToken() // read over name

	} else {

		p.addErr(fmt.Sprintf("expected query name got %q", p.curToken.Literal))
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
	// root only ...
	// eq(predicate, value)
	// eq(count(predicate), value)
	//
	// in filter clause
	// eq(val(varName), value)
	// eq(predicate, val(varName))
	// eq(predicate, [val1, val2, ..., valN])
	// eq(predicate, [$var1, "value", ..., $varN])

	// type RootStmt struct {
	// 	Name    name_
	// 	Var.    ast.Variable
	// 	Lang    string
	// 	// (func: eq(name@en, "Steven Spielberg”))
	// 	F GQLFunc // generates []uid from GSI data io.Writer Write([]byte) (int, error)
	// 	// @filter( has(director.film) )
	// 	Filter *expr.Expression //
	// 	Select SelectList
	// 	//
	// 	PredList []string
	// }

	// 	type GQLFunc struct {
	// 	FName name_ // for String() purposes
	// 	F     FuncT
	// 	Farg  FargI // either predicate, count, var
	// 	//	IFarg InnerArgI   // either uidPred, variable
	// 	Value interface{} //  string,int,float,$var, List of string,int,float,$var
	// }

	if p.hasError() {
		return p
	}

	rf := &s.RootFunc
	rf.AssignName(p.curToken.Literal, p.curToken.Loc)
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
	//
	// root function
	//
	switch p.curToken.Type {

	case token.RFUNC:
		rf.AssignName(p.curToken.Literal, p.curToken.Loc)
		rf.F = rootFunc[p.curToken.Literal]

		// case token.EQ:
		// 	rf.AssignName(p.curToken.Literal, p.curToken.Loc)
		// 	rf.F = ast.EQRoot

		// case token.GT:
		// 	rf.AssignName(p.curToken.Literal, p.curToken.Loc)
		// 	rf.F = ast.GtRoot

		// case token.LT:
		// 	rf.AssignName(p.curToken.Literal, p.curToken.Loc)
		//rf.F = lt
		//
		// ...
		//
	}
	p.nextToken() // read over eq,lt...
	// (
	if p.curToken.Type != token.LPAREN {
		p.addErr(fmt.Sprintf(`Expected (  got %s`, p.curToken.Literal))
	}
	p.nextToken() // read over (
	///
	// argument to func
	//
	fmt.Printf("2: %#v\n", p.curToken)
	switch p.curToken.Type {

	case token.IDENT:
		if !cache.IsScalarPred(p.curToken.Literal) {
			p.addErr(fmt.Sprintf(`Predicate %s is not a scalar in any type`, p.curToken.Literal))
			// return p
		}
		s := ast.ScalarPred{}
		s.AssignName(p.curToken.Literal, p.curToken.Loc)
		rf.Farg = s

		p.nextToken("read over identifer") // read over identier

	case token.COUNT:

		cfunc := &ast.CountFunc{}
		p.nextToken() // read over count

		if p.curToken.Type != token.LPAREN {
			p.addErr(fmt.Sprintf(`Expected (  got %s`, p.curToken.Literal))
			return p
		}
		p.nextToken() // read over (
		//
		// arg to Count
		///
		if p.curToken.Type != token.IDENT {
			if p.curToken.Literal == "UID" {
				p.addErr(`UID is not appropriate for a root function. Use as predicate in selection list or in filter expressions`)
			} else {
				p.addErr(fmt.Sprintf(`Expected identifier got %s`, p.curToken.Literal))
			}
		}
		if !cache.IsUidPred(p.curToken.Literal) {
			p.addErr(fmt.Sprintf(`%q must be a UID predicate in some type`, p.curToken.Literal))
		}
		// assign to CountFunc
		a := &ast.UidPred{Parent: s}
		a.AssignName(p.curToken.Literal, p.curToken.Loc)
		cfunc.Arg = a
		rf.Farg = cfunc

		p.nextToken() // read over identier
		p.nextToken() // read over )

	default:
		p.addErr(fmt.Sprintf(`Expected an identifier or modifer function got %s`, p.curToken.Literal))
		return p
	}
	//
	// parse value
	//
	fmt.Printf("3: %#v\n", p.curToken)
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
					v := ast.Variable{}
					v.AssignName(p.curToken.Literal, p.curToken.Loc)
					vs = append(vs, v)
				} else {
					p.addErr(fmt.Sprintf(`Expected variable name got %s`, p.curToken.Literal))
				}
			}
		}

		rf.Value = vs
	}
	p.nextToken("read over value...")
	fmt.Printf("4: %#v\n", p.curToken)
	//	  me(func: eq(count(Siblings),2) @filter(has(Friends)) ) {
	for i := 0; i < 2; i++ {
		if p.curToken.Type != token.RPAREN {
			if p.curToken.Type == token.ATSIGN {
				return p
			}
			p.addErr(fmt.Sprintf(`Expected ) got %s instead`, p.curToken.Literal))
			return p
		}
		p.nextToken() // read over )
	}
	fmt.Printf("End of parseFunction..  %#v\n", rf)
	return p

}

func (p *Parser) parseFilter(r ast.FilterI) *Parser {

	if p.hasError() {
		return p
	}

	// @filter(allofterms(name@en, "jones indiana") OR allofterms(name@en, "jurassic park"))
	fmt.Printf("in parseFilter: %#v\n", p.curToken)
	if p.hasError() || p.curToken.Type != token.ATSIGN {
		fmt.Printf("in parseFilter: return..\n")
		return p
	}
	p.nextToken() // read over @
	exprInput := p.l.Remaining()
	//  me(func: .......  @filter(has(Friends)) ) {
	//                                                     ^ ^ ^
	exprInput = exprInput[:strings.IndexByte(exprInput, '{')]
	exprInput = exprInput[:strings.LastIndexByte(exprInput, ')')]
	exprInput = exprInput[:strings.LastIndexByte(exprInput, ')')]

	if p.curToken.Type != token.FILTER {
		p.addErr(fmt.Sprintf(`Expected keyword "filter" got %s instead`, p.curToken.Literal))
		return p
	}

	p.nextToken() // read over filter
	fmt.Println("Input REmaining2: ", exprInput)
	if p.curToken.Type != token.LPAREN {
		p.addErr(fmt.Sprintf(`Expected (  got %s instead`, p.curToken.Literal))
		return p
	}
	//
	r.AssignFilterStmt(exprInput)
	r.AssignFilter(expr.New(exprInput)) // TODO should return new rLoc, cLoc
	//
	for ; p.curToken.Type != token.LBRACE; p.nextToken() {
	}
	fmt.Printf("******************************** in parseFilter: %#v\n", p.curToken)
	return p
}

func (p *Parser) parseSelection(r ast.SelectI) *Parser {

	if p.hasError() {
		return p
	}
	var s ast.SelectList
	fmt.Printf("in parseSelection: %#v\n", p.curToken)
	if p.curToken.Type != token.LBRACE {
		p.addErr(fmt.Sprintf(`expected a "{" got a %q`, p.curToken.Literal))
	}
	for p.nextToken(); p.curToken.Type != token.RBRACE; {

		e := &ast.EdgeT{}

		p.parseVarAlias(e).parseEdge(e, r)

		s = append(s, e)

		fmt.Printf("in parseSelection loop: %s\n", p.curToken.Type)

		if p.curToken.Type == token.RBRACE {
			break
		}

	}
	r.AssignSelectList(s)
	fmt.Printf("end parseSelection %#v\n", s)
	p.nextToken() // read over }
	//	panic(fmt.Errorf("XX"))
	return p
}

func (p *Parser) parseVarAlias(e *ast.EdgeT) *Parser {

	fmt.Printf("in parseVarAlias: %#v\n", p.curToken)
	if p.curToken.Type == token.RBRACE {
		p.addErr("No predicates specified in selection set")
		return p // must return
	}

	switch {
	case p.curToken.Type == token.IDENT && p.peekToken.Type == token.COLON:
		fmt.Println("Alias ", p.curToken.Literal)
		e.AssignName(p.curToken.Literal, p.curToken.Loc)
		p.nextToken() // read over IDENT
		p.nextToken() // read over :
		fmt.Printf("Next %#v\n", p.curToken)

	case p.curToken.Type == token.IDENT && p.peekToken.Type == token.AS:
		fmt.Println("Variable ", p.curToken.Literal)
		e.AssignVarName(p.curToken.Literal, p.curToken.Loc)

		st := &variable.Item{Name: p.curToken.Literal, Edge: e}
		st.Add()
		p.nextToken() // read over IDENT
		p.nextToken() // read over as

		//default:

		// not a variable or alias...

	}

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

func (p *Parser) parseEdge(e *ast.EdgeT, parentEdge ast.SelectI) *Parser {
	// edge can be
	// * <scalar-predicate>
	// * <uid-predicate> { SelectList }
	// * <uid predicate> @filter { SelectList }
	// * totalDirectors : count(uid)
	// * avg(val(<variable>)), sum, min, max
	// * val(<variable>)
	// * variable as <uidPred>     // query variable
	// * variable as <scalarPred>  // value variable
	// * uid

	if p.hasError() {
		return p
	}
	fmt.Printf("In parseEdge: %#v  %#v\n", p.curToken, p.peekToken)

	switch p.curToken.Type {

	case token.IDENT:
		// * <scalar-predicate>
		// * <uid-predicate> { SelectList }
		// * <uid predicate> @filter { SelectList }
		ident := p.curToken.Literal
		if p.peekToken.Type == token.ATSIGN || p.peekToken.Type == token.LBRACE {
			// must be a uid-pred - confirm there is a type that exists with this uid-pred
			if !cache.IsUidPred(ident) {
				p.addErr(fmt.Sprintf("%q is not a uid-predicate", ident))
			}
			//
			uidpred := &ast.UidPred{Parent: parentEdge}
			uidpred.AssignName(p.curToken.Literal, p.curToken.Loc)
			e.Edge = uidpred
			//p.parseFilter(uidpred.Filter).parseSelection(uidpred.Select) // TODO: remove comment...
			p.nextToken() // read over uid-pred
			if p.curToken.Type == token.ATSIGN {
				p.parseFilter(uidpred)
			}
			p.parseSelection(uidpred)

		} else {
			// scalar type
			fmt.Println("parseEdge: IDENT scalar-pred")
			if !cache.IsScalarPred(ident) {
				p.addErr(fmt.Sprintf("%q is not a scalar-pred", ident))
			}
			//
			spred := &ast.ScalarPred{Parent: parentEdge}
			spred.AssignName(p.curToken.Literal, p.curToken.Loc)
			e.Edge = spred
			p.nextToken() // read over predicate
		}
		fmt.Printf("XXEdge: %#v\n", e.Edge)
	// case token.AGFUNC:  // not applicable to root func
	// 	// * avg(val(<variable>)), sum, max, min,
	// 	// * val(<variable>)

	// 	agf := &ast.AggrFunc{}
	// 	agf.AssignName(p.curToken.Literal, p.curToken.Loc)
	// 	e.Edge = agf
	// 	p.nextToken() // read over count
	// 	if p.curToken.Type != token.LPAREN {
	// 		p.addErr(fmt.Sprintf("Expected ( got %s", p.curToken.Literal))
	// 	}
	// 	p.nextToken() // read over (
	// 	//
	// 	if p.curToken.Type != token.VAL {
	// 		p.addErr(fmt.Sprintf(`Expected "val" got %s`, p.curToken.Literal))
	// 	}
	// 	p.nextToken() // read over val
	// 	p.nextToken() // read over (
	// 	//
	// 	v := ast.Variable{}
	// 	v.AssignName(p.curToken.Literal, p.curToken.Loc)
	// 	e.Edge.Arg = v
	// 	//to execute variable.Count(p.nextToken.Literal)
	// 	p.nextToken() // read over )

	case token.COUNT:
		// actors as performance.actor {
		//   totalRoles as count(actor.film)
		// }
		// * abc as count(<uid predicate>|<UID>)
		// totalDirectors : count(uid)
		fmt.Println("COUNT..............")
		cf := &ast.CountFunc{}
		e.Edge = cf
		p.nextToken() // read over count
		if p.curToken.Type != token.LPAREN {
			p.addErr(fmt.Sprintf("expected ( got %s", p.curToken.Literal))
		}
		p.nextToken() // read over (
		switch p.curToken.Type {

		case token.IDENT:
			if !cache.IsUidPred(p.curToken.Literal) {
				p.addErr(fmt.Sprintf("%q is not a uid-predicate", p.curToken.Literal))
			}
			//
			fmt.Printf("COUNT IDENT- create uidPred: %#v\n", p.curToken)
			pred := &ast.UidPred{Parent: parentEdge}
			pred.AssignName(p.curToken.Literal, p.curToken.Loc)
			cf.Arg = pred
			p.nextToken() // read over uid-pred
			fmt.Printf("COUNT IDENT: %#v\n", p.curToken)
			if p.curToken.Type != token.RPAREN {
				p.addErr(fmt.Sprintf("expected ) got %s", p.curToken.Literal))
			}
			p.nextToken("") // read over )
			fmt.Printf("COUNT IDENT 3: %#v\n", p.curToken)

		case token.UID:
			cf.Arg = ast.UID{}
			p.nextToken() // read over uid
			if p.curToken.Type != token.RPAREN {
				p.addErr(fmt.Sprintf("expected ) got %s", p.curToken.Literal))
			}
			p.nextToken() // read over )
		}

	case token.VAL:
		// numRoles : val(roles)
		p.nextToken() // read over val
		if p.curToken.Type != token.LPAREN {
			p.addErr(fmt.Sprintf("Expected ( got %s", p.curToken.Literal))
		}
		p.nextToken() // read over (
		v := &ast.Variable{}
		v.AssignName(p.curToken.Literal, p.curToken.Loc)
		e.Edge = v
		p.nextToken() // read over )
		if p.curToken.Type != token.RPAREN {
			p.addErr(fmt.Sprintf("expected ( got %s", p.curToken.Literal))
		}
	case token.UID:
		e.Edge = &ast.UID{}
		p.nextToken() // read over uid
	}

	return p
}
