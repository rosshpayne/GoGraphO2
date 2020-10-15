package parser

import (
	"fmt"
	"strings"
	"testing"
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
  me(func: eq(Name, "Steven Spielberg"))  {
    Name
    date_of_birth:DOB
    cn as count(Siblings)
    brothers: Siblings {
    		Name
  			Friends {
  				Name
  			}
	}
  }
}`

	expectedDoc := `{
  me(func: eq(Name, "Steven Spielberg"))  {
  Name
  date_of_birth: DOB
  cn as count(Siblings)
  brothers: Siblings {
  	Name
  	Friends {
  		Name
  	}
  }
}
}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	fmt.Println(errs)
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	fmt.Println(doc.String())
	//

}

func TestStmt1(t *testing.T) {

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
  me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
	}
	}`

	expectedDoc := `{
 me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	fmt.Println(errs)
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	fmt.Println(doc.String())
	//

}

func TestStmt2(t *testing.T) {

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
  me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
} }`

	expectedDoc := `{
 me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
} }`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	fmt.Println(errs)
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	fmt.Println(doc.String())
	//

}

func TestStmt3(t *testing.T) {

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
  var(func: allofterms(Name, "eat drink man woman")) {
    Film.performance {
      actors as performance.actor {
         Name
      	totalRoles as count(actor.film)

      }
    }
  }
}`

	expectedDoc := `{
  var(func: allofterms(Name, "eat drink man woman")) {
    Film.performance {
      actors as performance.actor {
       Name
        totalRoles as count(actor.film)
      }
    }
  }
}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	fmt.Println(errs)
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//fmt.Println(doc.String())
	//

}

func TestFilter0(t *testing.T) {

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
  me(func: eq(Name, "Steven Spielberg")) @filter(has(Friends)) {
   UID
   Name
}}`

	expectedDoc := `{
  me(func: eq(Name, "Steven Spielberg")) @filter(has(Friends)) {
   UID
   Name
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	fmt.Println(errs)
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	fmt.Println(doc.String())
	//

}

func TestCounPred(t *testing.T) {

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

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
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
  me(func: eq(Age, 67 ) ) {
  Name
  Jobs3
}}`

	expectedDoc := `{
 me(func: eq(Age, 67 )) {
   Name
  Jobs
}}`
	parseErrs := []string{`"Jobs3" is not a scalar-pred at line: 4, column: 3`}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
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

func TestStmtFloatPredErr(t *testing.T) {

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

	parseErrs := []string{`Predicate age is not a scalar in any type at line: 2, column: 15`}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
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
  me(func: eq(Age, 6732234.23 ) ) {
}`

	expectedDoc := `{
 me(func: eq(Age, 6.73223423E+06 )) {
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
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

func TestFilter1(t *testing.T) {

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
  me(func:eq(count(Siblings),2) @filter(has(Friends)) ) {
	Name
}}`

	expectedDoc := `{
  me(func: eq(count(Siblings),2) @filter(has(Friends)) ) {
  Name
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
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

func TestFilterAnd(t *testing.T) {

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
  me(func:eq(count(Siblings),2) @filter(has(Friends) and (has(Siblings) or has(Name))) ) {
	Name
}}`

	expectedDoc := `{
  me(func: eq(count(Siblings),2) @filter(has(Friends) and (has(Siblings) or has(Name))) ) {
  Name
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
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
