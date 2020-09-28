package ast

import (
	"fmt"

	"github.com/DynamoGraph/block"
	"github.com/DynamoGraph/cache"
	"github.com/DynamoGraph/db"
	"github.com/DynamoGraph/ds"
	"github.com/DynamoGraph/gql/varcache"
	"github.com/DynamoGraph/util"
)

// eq(predicate, value)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

//   eq(count(genre), 13))

// type Arg1 interface {
// 	arg1()
// }

// type InnerFuncT interface {
// 	innerFunc()
// }

// type InnerArg interface {
// 	innerArg()
// }

// type ScalarPred string

// func (p ScalarPred) arg1() {}

// type UIDPred string

// func (p UIDPred) innerArg() {}

// type Variable string

// func (v Variable) innerArg() {}

// type ValFuncT func(v Variable) ValOut

// func (p ValFuncT) arg1()      {}
// func (p ValFuncT) innerFunc() {}

// type CountFuncT func(predicate UIDPred) int

// func (p CountFuncT) arg1()      {}
// func (p CountFuncT) innerFunc() {}

// type FuncT func(predfunc Farg1, value interface{}, nv []ds.NV, ty string) bool

// type TyAttrCache map[Ty_Attr]blk.TyAttrD // map[Ty_Attr]blk.TyItem

// var TyAttrC TyAttrCache

type RootResultT struct {
	PKey  util.UID
	SortK string
	Ty    string
}

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

// Root EQ function called during execution-root-query phase
// Each RootResult will be Fetched then Unmarshalled (via UnmarshalCache) into []NV for each predicate.
// The []NV will then be processed by the Filter function if present to reduce the number of elements in []NV
func EQRootfunc(a FargI, value interface{}, nv []ds.NV, ty string) []RootResultT {
	switch a.(type) {
//case CountFunc:
	case ScalarPred:
	//case Variable:
	}

	return nil
}

// Filter EQ function called during execution-filter phase
// predfunc can be either the expression predicate or a modifying function (val, count)
// value is the predicate value as appears in the expression e.g. eq(<pred>, 5) where 5 is the value.
// nv is the contents of the cache (predicate,value) for the UID returned from the root func
// ty is the type of the cache entry
// bool dictates whether RootResult element (represented by nv argument) will be ignored or displayeda
func EQFilter(predfunc Arg1, value interface{}, nv []ds.NV, ty string) bool {
	var (
		pTy    block.TyAttrD
		ok     bool
		nvPred string
		nvVal  interface{}
	)
	// _, err := cache.FetchType(ty) // performed outside of FilterEQ maybe
	// if err != nil {
	// 	panic(fmt.Errorf("Type %q not found", ty))
	// }

	switch x := predfunc.(type) {

	case InnerFuncT:

		var c interface{}

		c := x() // var(<variable>), count(<predicate>)
		

		switch x.(type) {
		case int64:
			// check value is an int
			if v, ok := value.(int64); !ok {

			} else {
				if v == x {
					return true
				}
				return false
			}
		case float:
		}

	case ScalarPred:
		// find value for this predicate
		var (
			found bool
		)
		// search NV for expression predicate
		for _, v := range nv {
			if x == v.Name {
				found = true
				nvPred = v.Name
				nvVal = v.Value
				break
			}
		}
		if !found {
			panic(fmt.Errorf("predicate %q not found in ds.NV", x))
		}
		//
		// get type of predicate from type info
		if pTy, ok = cache.TyAttrC[ty+":"+x]; !ok { // TODO is this concurrent safe??
			panic(fmt.Errorf("predicate %q not found in type map", x))
		}
		switch pTy.DT {
		case "S":
			var (
				ok       bool
				cacheVal string
				exprVal  string
			)
			if cacheVal, ok = nvVal.(string); !ok {
				panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", nvPred, nvVal, ty))
			}
			// compare
			if exprVal, ok = value.(string); !ok {
				panic(fmt.Errorf("value %q for predicate %q does not match type %q", cacheVal, nvPred, ty))
			}
			//
			//
			//
			if exprVal == cacheVal {
				return true
			}
			return false
		case "I":
		case "F":
		case "Bl":
		}

	}
	return false
}

// func lt() []ast.Result  { return nil }
// func has() []ast.Result { return nil }

funcs Var(varNm Variable) ValOut {
	item:=variable.Get(varNm)
	switch x:=item.(type);x {
	case ast.EdgeT:
	default:
	
	}
} 