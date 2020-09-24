package parser

import (
	"testing"
)

func TestSimple(t *testing.T) {

	input := `{
  me(func: eq(name@en, "Steven Spielberg”)) @filter( has(director.film) ) {
    name 
    director.film (A#G#:D,Nd) @filter(allofterms(name, "jones indiana”)   {
      name
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
