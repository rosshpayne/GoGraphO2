package parser

import (
	"strings"
	"testing"

	"github.com/DynamoGraph/gql/lexer"
)

func compare(doc, expected string) bool {

	return trimWS(doc) != trimWS(expected)

}

// trimWS trims whitespaces from input string. Objective is to compare strings real content - not influenced by whitespaces
func trimWS(input string) string {

	var out strings.Builder
	for _, v := range input {
		if !(v == '\u0009' || v == '\u0020' || v == '\u000A' || v == '\u000D' || v == ',') {
			out.WriteRune(v)
		}
	}
	return out.String()

}

// checkErrors compares actual errors from test against slice of expected errors
func checkErrors(errs []error, expectedErr []string, t *testing.T) {

	for _, ex := range expectedErr {
		if len(ex) == 0 {
			break
		}
		found := false
		for _, err := range errs {
			if trimWS(err.Error()) == trimWS(ex) {
				found = true
			}
		}
		if !found {
			t.Errorf(`Expected Error = [%q]`, ex)
		}
	}
	for _, got := range errs {
		found := false
		for _, exp := range expectedErr {
			if trimWS(got.Error()) == trimWS(exp) {
				found = true
			}
		}
		if !found {
			t.Errorf(`Unexpected Error = [%q]`, got.Error())
		}
	}
}

func TestStmt0(t *testing.T) {

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
  me(func: eq(name, "Steven Spielberg"))  {
}`

	expectedDoc := `{
  me(func: eq(name, "Steven Spielberg"))  {
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

func TestStringPred(t *testing.T) {

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
  me(func: eq(count(name), "Steven Spielberg"))  {
}`

	expectedDoc := `{
  me(func: eq(count(name), "Steven Spielberg"))   {
}`

	var parseErrs []string

	l := lexer.New(input)
	p := New(l)

	doc, errs := p.ParseDocument()
	t.Log(doc.String())
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

func TestStmtIntPred(t *testing.T) {

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
  me(func: eq(age, 67 ) ) {
}`

	expectedDoc := `{
 me(func: eq(age, 67 )) {
}`

	var parseErrs []string

	l := lexer.New(input)
	p := New(l)

	doc, errs := p.ParseDocument()
	t.Log(doc.String())
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

func TestStmtFloatPred(t *testing.T) {

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
  me(func: eq(age, 6732234.23 ) ) {
}`

	expectedDoc := `{
 me(func: eq(age, 6.73223423E+06 )) {
}`

	var parseErrs []string

	l := lexer.New(input)
	p := New(l)

	doc, errs := p.ParseDocument()
	t.Log(doc.String())
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

func TestFilter(t *testing.T) {

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
  me(func: eq(count(name), "Steven Spielberg")) @filter(eq(count(name)) {
}`

	expectedDoc := `{
  me(func: eq(count(name), "Steven Spielberg")) @filter(eq(count(name))  {
}`

	var parseErrs []string

	l := lexer.New(input)
	p := New(l)

	doc, errs := p.ParseDocument()
	t.Log(doc.String())
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
