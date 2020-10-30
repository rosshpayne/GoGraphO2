package gql

import (
	"fmt"
	"testing"
	"time"
)

func TestSimpleRootQuery1a(t *testing.T) {

	input := `{
  directors(func: eq(count(Siblings), 2)) {
  Age
    Name
  }
}`

	Execute(input)

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

	Execute(input)

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

func TestSimpleRootQuery1e(t *testing.T) {

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
func TestSimpleRootQuery1f(t *testing.T) {

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
  directors(func: eq(count(Siblings), 2) @filter(gt(Age,60))) {
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
    Friends @filter(gt(Age,62) or le(Age,40) or eq(Name,"Ross Payne"))) {
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

func TestSimpleRootQuery2(t *testing.T) {

	input := `{
  directors(func: gt(count(director.film), 5)) {
    totalDirectors : count(uid)
  }
}`

	Execute(input)

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

	Execute(input)
	//

}
