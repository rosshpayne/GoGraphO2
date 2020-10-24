package ast

import (
	"fmt"

	"github.com/DynamoGraph/block"
	"github.com/DynamoGraph/cache"
	//"github.com/DynamoGraph/db"
	"github.com/DynamoGraph/ds"
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

// Filter EQ function called during execution-filter phase
// predfunc can be either the expression predicate or a modifying function (val, count)
// value is the predicate value as appears in the expression e.g. eq(<pred>, 5) where 5 is the value.
// nv is the contents of the cache (predicate,value) for the UID returned from the root func
// ty is the type of the cache entry
// i,k index into slice ([][]) of edge interface values (overflow representation of edge values)
// bool dictates whether QResult element (represented by nv argument) will be ignored or displayeda
func EQ(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	var (
		pTy block.TyAttrD
		ok  bool
	)

	switch x := predfunc.(type) {

	case CountFunc:

		switch x.Arg.(type) {

		case *UidPred:
			var nds [][][]byte
			// get data from nv
			data := nv[x.Name()] //data.Value [][][]byte

			if nds, ok = data.Value.([][][]byte); !ok {
				panic(fmt.Errorf("Expression EQ: Expected [][][]byte for NV value"))
			}
			// count child nodes
			n := 0
			for i, k := range nds {
				n += len(k[i])
			}
			if v, ok := value.(int); ok {
				return v == n
			}

			// case Variable: //TODO: implement
		}

	case ScalarPred:
		// find value for this predicate
		var (
			found bool
		)
		// get predicate data
		data := nv[x.Name()]

		if !found {
			panic(fmt.Errorf("predicate %q not found in ds.NV", x))
		}
		//
		// get type of predicate from type info
		if pTy, ok = cache.TypeC.TyAttrC[ty+":"+x.Name()]; !ok { // TODO is this concurrent safe??
			panic(fmt.Errorf("predicate %q not found in type map", x))
		}
		switch pTy.DT {
		case "S":

			switch ty {
			case "":
				var (
					ok      bool
					dataVal string
					exprVal string
				)
				// root query filter expression - node scalar data
				// data value type
				if dataVal, ok = data.Value.(string); !ok {
					panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", x.Name(), dataVal, ty))
				}
				// expression value
				if exprVal, ok = value.(string); !ok {
					panic(fmt.Errorf("value %q for predicate %q does not match type %q", exprVal, x.Name(), ty))
				}
				// compare values
				if exprVal == dataVal {
					return true
				}
			default:
				var (
					ok      bool
					dataVal [][]string
					exprVal string
				)
				// uid-pred filter expression - multiple child scalar data (propagated data format)
				// check if null flag set in data
				if data.Null[j][k] {
					return false
				}
				// data value type
				if dataVal, ok = data.Value.([][]string); !ok {
					panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", x.Name(), data.Value, ty))
				}
				// expression value
				if exprVal, ok = value.(string); !ok {
					panic(fmt.Errorf("value %q for predicate %q does not match type %q", exprVal, x.Name(), ty))
				}
				// compare values
				if exprVal == dataVal[j][k] {
					return true
				}
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

// funcs Var(varNm Variable) ValOut {
// 	item:=variable.Get(varNm)
// 	switch x:=item.(type);x {
// 	case ast.EdgeT:
// 	default:

// 	}
// }
func GT(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool     { return true }
func HAS(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool    { return false }
func UID(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool    { return false }
func UID_IN(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool { return false }
func VAL(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool    { return false }
func ANYOFTERMS(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return false
}
func ALLOFTERMS(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return false
}
