package parser

import (
	"testing"

	"github.com/DynamoGraph/gql/lexer"
)

func TestParseStmt(t *testing.T) {

	// 	input := `{
	//   me(func: eq(name@en, "Steven Spielberg")) @filter(has(director.film)) {
	//     name@en
	//     director.film @filter(allofterms(name@en, "jones indiana") OR allofterms(name@en, "jurassic park"))  {
	//       uid
	//       name@en
	//     }
	//   }
	// }`

	input := `{
  me(func: eq(name@en, "Steven Spielberg"))  {
}`

	var parseErrs []string

	l := lexer.New(input)
	p := New(l)

	doc, errs := p.ParseDocument()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//

}
