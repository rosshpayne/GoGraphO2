package gql

import (
	"fmt"
	"strings"
	"testing"
	"time"
	//	"github.com/DynamoGraph/gql/monitor"
)

func compare(doc, expected string) int {

	return strings.Compare(trimWS(doc), trimWS(expected))

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

func TestSimpleRootQuery1a(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2)) {
    Age
    Name
  }
 }`

	expected := ` 
        {
        data: [
                {
                Age : 62,
                Name : "Ross Payne",
                }, 
                {
                Age : 67,
                Name : "Ian Payne",
                }, 
                {
                Age : 58,
                Name : "Paul Payne",
                } 
        ]
        }`

	t0 := time.Now()
	stmt := Execute_(input)
	t1 := time.Now()
	t.Log(fmt.Sprintf("TExecute duration: %s \n", t1.Sub(t0)))

	result := stmt.MarshalJSON()

	if compare(result, expected) != 0 {
		t.Fatal(fmt.Sprintf("result not equal to expected: result = %s", result))
	}
	t.Log(result)
}

func TestSimpleRootQuery1b(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 1)) {
    Age
    Name
    Siblings {
    	Name
    }
  }
}`

	expected := `      {
        data: [
                {
                Age : 36,
                ],
                Name : "Phil Smith",
                ],
                Siblings : [ 
                        { 
                        Name: Jenny Jones,
                        }, 
                ],
                } 
                ],
                } 
        }`

	t0 := time.Now()
	stmt := Execute_(input)
	t1 := time.Now()
	t.Log(fmt.Sprintf("TExecute duration: %s \n", t1.Sub(t0)))

	result := stmt.MarshalJSON()

	if compare(result, expected) != 0 {
		t.Fatal(fmt.Sprintf("result not equal to expected: result = %s", result))
	}
	t.Log(result)

}

func TestSimpleRootQuery1c(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 1)) {
    Age
    Name
    Friends {
    	Name
    	Age
    	Siblings {
    		Name
    	}
    }
  }
}`

	Execute(input)

}

func TestSimpleRootQuery1d(t *testing.T) {

	// Friends {
	// 	Age
	// }
	input := `{
  directors(func: eq(count(Siblings), 1)) {
    Age
    Name
    Friends {
    	Name
    	Age
    	Siblings {
    		Name
    		Friends {
    			Name
    			Age
    		}
    	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestRootQuery1e1(t *testing.T) {

	// Friends {
	// 	Age
	// }
	input := `{
  directors(func: anyofterms(Comment,"sodium Germany Chris")) {
    Age
    Name
    Comment
    Friends {
    	Name
    	Age
    	Siblings {
    		Name
    		Friends {
    			Name
    			Age
    		}
    	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestRootQuery1e2(t *testing.T) {

	// Friends {
	// 	Age
	// }
	input := `{
  directors(func: anyofterms(Comment,"sodium Germany Chris"))  @filter(gt(Age,60)) {
    Age
    Name
    Comment
    Friends {
    	Name
    	Age
    	Siblings {
    		Name
    		Friends {
    			Name
    			Age
    			Comment
    		}
    	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}
func TestRootQuery1f(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2)) {
    Age
    Name
    Friends {
      Age
    	Name
    	Friends {
    	  Name
		    Age
	    }
	    Siblings {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}
func TestRootQuery1g(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2)) {
    Age
    Name
    Friends {
    	Name
    	Age
    	Siblings {
    		Name
    		Friends {
    			Name
    			Age
    		}
    	}
    }
    Siblings {
    		Name
    		Age
	  }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestRootFilter1(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2)) @filter(gt(Age,60)) {
    Age
    Name
    Friends {
      Age
    	Name
    	Friends {
    	  Name
		    Age
	    }
	    Siblings {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter1(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,61)) {
      Age
    	Name
    	Friends {
    	  Name
		    Age
	    }
	    Siblings {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter2(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,60)) {
      Age
    	Name
    	Friends @filter(gt(Age,60)) {
    	  Name
		    Age
	    }
	    Siblings {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter3a(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends {
      Age
    	Name
    	Siblings {
    		Name
    		Age
	   	}
    	Friends  {
    	  Name
	    }
  }
}
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter3b(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,50)) {
      Age
    	Name
    	Siblings @filter(gt(Age,5)) {
    		Name
    		Age
	   	}
    	Friends @filter(gt(Age,50)) {
    	  Age
    	  Name
	    }
  }
}
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter3c(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,60)) {
      Age
    	Name
    	Siblings @filter(gt(Age,60)) {
    		Name
    		Age
	   	}
    	Friends @filter(gt(Age,50)) {
    	  Age
    	  Name
	    }
  }
}
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter4a(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,62) or le(Age,40) or eq(Name,"Ross Payne")) {
      Age
    	Name
    	Comment
    	Friends   {
    	  Name
    	  Age
	    }
    	Siblings {
    		Age
    		Name
    		Comment
	   	}
  }
}
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter4b(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,60)) {
      Age
    	Name
    	Siblings @filter(gt(Age,60)) {
    		Name
	   	}
    	Friends @filter(gt(Age,60)) {
    	  Name
	    }

    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilter5(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(gt(Age,60)) {
      Age
    	Name
    	Friends @filter(gt(Age,62)) {
    	  Name
	    }
	    Siblings @filter(gt(Age,60)) {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestRootFilter2(t *testing.T) {

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

	Execute(input)
	//

}

func TestRootFilteranyofterms1_(t *testing.T) {

	input := `{
	  me(func: eq(count(Siblings),2)) @filter(anyofterms(Comment,"sodium Germany Chris")) {
	    Name
		Comment
	    }
	  }`

	expected := `{
data: [
        {
        Name : "Ross Payne",
        Comment : "Another fun  video. Loved it my Payne Grandmother was from Passau. Dad was over in Germany but there was something going on over there at the time we won't discuss right now. Thanks for posting it. Have a great weekend everyone.",
        },
        {
        Name : "Paul Payne",
        Comment : "A foggy snowy morning lit with Smith sodium lamps is an absolute dream",
        }
   ]
}`

	t0 := time.Now()
	stmt := Execute_(input)
	t1 := time.Now()
	t.Log(fmt.Sprintf("TExecute duration: %s \n", t1.Sub(t0)))

	result := stmt.MarshalJSON()

	if compare(result, expected) != 0 {
		t.Log("Error: result JSON does not match expected JSONs")
		t.Fail()
	}
	t.Log(result)

}

func TestRootFilteranyofterms1a(t *testing.T) {

	input := `{
	  me(func: eq(count(Siblings),2)) @filter(anyofterms(Comment,"sodium Germany Chris")) {
	    Name
	    }
	  }`
	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))

}

func TestRootFilteranyofterms1b(t *testing.T) {

	input := `{
	  me(func: eq(count(Siblings),2)) @filter(anyofterms(Comment,"sodium Germany Chris") and eq(Name,"Ian Payne")) {
	    Name
	    }
	  }`
	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))

}

func TestRootFilteranyofterms1c(t *testing.T) {

	input := `{
	  me(func: eq(count(Siblings),2)) @filter(anyofterms(Comment,"sodium Germany Chris") or eq(Name,"Ross Payne")) {
	    Name
	    }
	  }`
	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))

}

func TestRootFilteranyofterms1d(t *testing.T) {

	input := `{
	  me(func: eq(count(Siblings),2)) @filter(anyofterms(Comment,"sodium Germany Chris") and eq(Name,"Ross Payne")) {
	    Name
	    }
	  }`
	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))

}

func TestUPredFilterterms1a(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends  {
      Age
    	Name
    	Comment
    	Friends{
    	  Name
	    }
	    Siblings {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))
}

func TestUPredFilterterms1b1(t *testing.T) {

	expected := `        
	{
        data: [
                {
                Age : 62,
                Name : "Ross Payne",
                Friends : [ 
                ]
                },
                {
                Age : 67,
                Name : "Ian Payne",
                Friends : [ 
                        { 
                        Age: 62,
                        Name: Ross Payne,
                        Friends : [ 
                                { 
                                Age: 67,
                                Name: Ian Payne,
                                }, 
                        ],
                        Siblings : [ 
                                { 
                                Name: Ross Payne,
                                }, 
                                { 
                                Name: Ian Payne,
                                }, 
                        ],
                        }, 
                ]
                },
                {
                Age : 58,
                Name : "Paul Payne",
                Friends : [ 
                        { 
                        Age: 62,
                        Name: Ross Payne,
                        Friends : [ 
                                { 
                                Age: 67,
                                Name: Ian Payne,
                                }, 
                        ],
                        Siblings : [ 
                                { 
                                Name: Paul Payne,
                                }, 
                                { 
                                Name: Ian Payne,
                                }, 
                        ],
                        }, 
                ]
                }
           ]
        }`

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(anyofterms(Comment,"sodium Germany Chris")) {
        Age
    	Name
    	Friends @filter(gt(Age,62)) {
    	  Age
    	  Name
	    }
	    Siblings @filter(gt(Age,55)) {
    	  Name
	   	}
    }
  }
}`

	t0 := time.Now()
	stmt := Execute_(input)
	t1 := time.Now()
	t.Log(fmt.Sprintf("TExecute duration: %s \n", t1.Sub(t0)))

	result := stmt.MarshalJSON()

	t.Log(result)
	if compare(result, expected) != 0 {
		t.Log("Error: result JSON does not match expected JSONs")
		t.Fail()
	}

}

func TestUPredFilterterms1b2(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2) ) {
    Age
    Name
    Friends @filter(anyofterms(Comment,"sodium Germany Chris")) {
        Age
    	Name
    	Friends @filter(gt(Age,62)) {
    	  Age
    	  Name
	    }
	    Siblings @filter(gt(Age,55)) {
    		Name
	   	}
    }
  }
}`

	t0 := time.Now()
	Execute(input)
	t1 := time.Now()
	fmt.Printf("TExecute duration: %s \n", t1.Sub(t0))

}
