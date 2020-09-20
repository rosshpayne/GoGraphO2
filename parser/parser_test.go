package parser

import (
	"testing"
)

func TestSimple(t *testing.T) {

	input := `{
  me(func: eq(name@en, "Steven Spielberg”)) ([UID-ty]) @filter( has(director.film) )([UID-person]) {
    name@en (A#:N,S)
    director.film (A#G#:D,Nd) @filter(allofterms(name@en (A#G#:D#:N,LS) , "jones indiana”) [index]  {
      name@en. (A#G#:D#:N,LS)
    }
  }
}
`

	l := lexer.New(input)
	p := New(l)

	doc, errs := p.parseQuery()
}

func (p *Parser) parseQuery() {

	p.parseOpen().p.parseName(stmt, opt).parseVariables(stmt, opt).parseFunction(stmt, opt).parseFilter(stmt, opt).parseSelectionSet(stmt).parseClose()

}
