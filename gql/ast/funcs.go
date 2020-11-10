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
	allofterms = " AND "
	anyofterms = " OR "
)

func syslog(s string) {
	slog.Log(logid, s)
}

// eq function for root query called during execution-root-query phase
// Each QResult will be Fetched then Unmarshalled (via UnmarshalCache) into []NV for each predicate.
// The []NV will then be processed by the Filter function if present to reduce the number of elements in []NV
func EQ(a FargI, value interface{}) db.QResult {
	return ieq(db.EQ, a, value)
}
func GT(a FargI, value interface{}) db.QResult {
	return ieq(db.GT, a, value)
}
func GE(a FargI, value interface{}) db.QResult {
	return ieq(db.GE, a, value)
}
func LT(a FargI, value interface{}) db.QResult {
	return ieq(db.LT, a, value)
}
func LE(a FargI, value interface{}) db.QResult {
	return ieq(db.LE, a, value)
}

func ieq(opr db.Equality, a FargI, value interface{}) db.QResult {

	var (
		err    error
		result db.QResult
	)

	switch x := a.(type) {

	case *CountFunc:
		fmt.Println("in CountFuncQ...............................")
		// for root stmt only this signature is valid: Count(<uid-pred>)

		if y, ok := x.Arg.(*UidPred); ok {

			fmt.Printf("in Arg......%T\n", y)
			switch v := value.(type) {
			case int:
				fmt.Printf("in int......%v/n", v)
				result, err = db.GSIQueryN(y.Name(), float64(v), opr)
			case float64:
				result, err = db.GSIQueryN(y.Name(), v, opr)
			case string:
				result, err = db.GSIQueryS(y.Name(), v, opr)
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
			result, err = db.GSIQueryN(x.Name(), float64(v), opr)
		case float64:
			result, err = db.GSIQueryN(x.Name(), v, opr)
		case string:
			result, err = db.GSIQueryS(x.Name(), v, opr)
		case []interface{}:
			//case Variable: // not on root func
		}

	}

	return result
}

//func Has(a FargI, value interface{}) db.QResult {)

//
// these funcs are used in filter condition only. At the root search ElasticSearch is used to retrieve relevant UIDs.
//
func AllOfTerms(a FargI, value interface{}) db.QResult {
	return terms(allofterms, a, value)
}

func AnyOfTerms(a FargI, value interface{}) db.QResult {
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

func Has(a FargI, value interface{}) db.QResult {

	if value != nil {
		panic(fmt.Errorf("Expected nil value. Second argument to has() should be empty"))
	}

	return db.QResult{}
}
