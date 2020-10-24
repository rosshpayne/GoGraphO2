package ast

import (
	"fmt"
	"github.com/DynamoGraph/db"
)

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
	return nil

}

// Filter EQ function called during execution-filter phase
// predfunc can be either the expression predicate or a modifying function (val, count)
// value is the predicate value as appears in the expression e.g. eq(<pred>, 5) where 5 is the value.
// nv is the contents of the cache (predicate,value) for the UID returned from the root func
// ty is the type of the cache entry
// bool dictates whether QResult element (represented by nv argument) will be ignored or displayeda
//

//func eqFilter(predfunc Arg1, value interface{}, nv []ds.NV, ty string) bool {
// 	var (
// 		pTy    block.TyAttrD
// 		ok     bool
// 		nvPred string
// 		nvVal  interface{}
// 	)
// 	// _, err := cache.FetchType(ty) // performed outside of FilterEQ maybe
// 	// if err != nil {
// 	// 	panic(fmt.Errorf("Type %q not found", ty))
// 	// }

// 	switch x := predfunc.(type) {

// 	case InnerFuncT:

// 		var c interface{}

// 		c := x() // var(<variable>), count(<predicate>)

// 		switch x.(type) {
// 		case int64:
// 			// check value is an int
// 			if v, ok := value.(int64); !ok {

// 			} else {
// 				if v == x {
// 					return true
// 				}
// 				return false
// 			}
// 		case float:
// 		}

// 	case ScalarPred:
// 		// find value for this predicate
// 		var (
// 			found bool
// 		)
// 		// search NV for expression predicate
// 		for _, v := range nv {
// 			if x == v.Name {
// 				found = true
// 				nvPred = v.Name
// 				nvVal = v.Value
// 				break
// 			}
// 		}
// 		if !found {
// 			panic(fmt.Errorf("predicate %q not found in ds.NV", x))
// 		}
// 		//
// 		// get type of predicate from type info
// 		if pTy, ok = cache.TyAttrC[ty+":"+x]; !ok { // TODO is this concurrent safe??
// 			panic(fmt.Errorf("predicate %q not found in type map", x))
// 		}
// 		switch pTy.DT {
// 		case "S":
// 			var (
// 				ok       bool
// 				cacheVal string
// 				exprVal  string
// 			)
// 			if cacheVal, ok = nvVal.(string); !ok {
// 				panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", nvPred, nvVal, ty))
// 			}
// 			// compare
// 			if exprVal, ok = value.(string); !ok {
// 				panic(fmt.Errorf("value %q for predicate %q does not match type %q", cacheVal, nvPred, ty))
// 			}
// 			//
// 			//
// 			//
// 			if exprVal == cacheVal {
// 				return true
// 			}
// 			return false
// 		case "I":
// 		case "F":
// 		case "Bl":
// 		}

// 	}
// 	return false
// }

// func lt() []ast.Result  { return nil }
// func has() []ast.Result { return nil }

// funcs Var(varNm Variable) ValOut {
// 	item:=variable.Get(varNm)
// 	switch x:=item.(type);x {
// 	case ast.EdgeT:
// 	default:

// 	}
// }
