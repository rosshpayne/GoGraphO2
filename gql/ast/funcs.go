package ast

import (
	"fmt"
	"strings"

	"github.com/DynamoGraph/gql/internal/db"
	"github.com/DynamoGraph/gql/internal/es"
	slog "github.com/DynamoGraph/syslog"
)

const (
	logid = "gqlAstFunc: "
)
const (
	esIndex    = "myidx001"
	allofterms = " AND "
	anyofterms = " OR "
)

func syslog(s string) {
	slog.Log(logid, s)
}

// eq(predicate, value)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

// type QResultT struct {
// 	PKey  util.UID
// 	SortK string
// 	Ty    string
// }

// ==========================================================================================================

// func Count(p Predicate) int {
// 	// parsing will have confirmed p is a uid-pred only
// 	cnt, err := db.GetCount(p)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return cnt

// }

// type ValOut interface{}

// func Val(v Variable) ValOut {
// 	if v, ok := variable[v]; !ok {
// 		panic(fmt.Errorf("%q does not exist as variable", v))
// 	} else {
// 		return v
// 	}
// 	return nil
// }

// eq function for root query called during execution-root-query phase
// Each QResult will be Fetched then Unmarshalled (via UnmarshalCache) into []NV for each predicate.
// The []NV will then be processed by the Filter function if present to reduce the number of elements in []NV
func EQ(a FargI, value interface{}) db.QResult {

	var (
		err    error
		result db.QResult
	)
	fmt.Println("in EQ...............................")
	switch x := a.(type) {

	case *CountFunc:
		fmt.Println("in CountFuncQ...............................")
		// for root stmt only this signature is valid: Count(<uid-pred>)

		if y, ok := x.Arg.(*UidPred); ok {

			fmt.Printf("in Arg......%T\n", y)
			switch v := value.(type) {
			case int:
				fmt.Printf("in int......%v/n", v)
				result, err = db.GSIQueryN(y.Name(), float64(v), db.EQ)
			case float64:
				result, err = db.GSIQueryN(y.Name(), v, db.EQ)
			case string:
				result, err = db.GSIQueryS(y.Name(), v, db.EQ)
			case []interface{}:
				//case Variable: // not on root func
			}
			if err != nil {
				panic(fmt.Errorf("GSIQueryNum error: %s", err.Error()))
			}

		}

	case ScalarPred:

		switch v := value.(type) {
		case int:
			result, err = db.GSIQueryN(x.Name(), float64(v), db.EQ)
		case float64:
			result, err = db.GSIQueryN(x.Name(), v, db.EQ)
		case string:
			result, err = db.GSIQueryS(x.Name(), v, db.EQ)
		case []interface{}:
			//case Variable: // not on root func
		}

	}

	return result
}

// The []NV will then be processed by the Filter function if present to reduce the number of elements in []NV
func GT(a FargI, value interface{}) db.QResult {
	return nil

}

func ALLOFTERMS(a FargI, value interface{}) db.QResult {
	return terms(allofterms, a, value)
}

func ANYOFTERMS(a FargI, value interface{}) db.QResult {
	return terms(anyofterms, a, value)
}

func terms(termOpr string, a FargI, value interface{}) db.QResult {

	// a => predicate
	// value => space delimited list of terms

	type data struct {
		field string
		query string
	}

	var (
		qs strings.Builder
		t  ScalarPred
		ok bool
	)
	ss := strings.Split(value.(string), " ")
	for i, v := range ss {
		qs.WriteString(v)
		if i < len(ss)-1 {
			qs.WriteString(termOpr)
		}
	}
	if t, ok = a.(ScalarPred); !ok {
		panic(fmt.Errorf("Error in all|any ofterms func: expected a scalar predicate"))
	}
	return es.Query(t.Name(), qs.String())
}
