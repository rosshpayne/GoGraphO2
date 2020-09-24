package ast

import (
	expr "github.com/DynamoGraph/expression"
	"github.com/DynamoGraph/util"
	"strconv"
	"strings"
)

// ( eq(predicate, value) and eq(..)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

//   eq(count(genre), 13))

// type Document struct {
// 	Stmt []GQLstmt
// }

type FilterFunc struct {
	parent *Expression
	//value  bool   // for testing only
	name string // for debug purposes - not used yet
	//
	// fname(predicate, value)
	// fname(predFunc(predicate), value)
	predicate string // used to point to particular nv value at execution time
	nv        *ds.NV // data from cache for all predicates
	//
	value bool // maybe useful for dummy expression where roperand is nil a bool rather than a function
	//
	fname    funcs.FuncT // funcs.Eq  func( arg1 interface{},  value interface{}) bool // eq,le,lt,gt,ge,allofterms, someofterms
	predFunc funcs.FuncT // count(predicate), val(predicate)
}

func (f *FilterFunc) Oper() {} // ???

func (f *FilterFunc) GetParent() *Expression {
	return f.parent
}

func (f *FilterFunc) Type_() string {
	return "func"
}

func (f *FilterFunc) GetResult(nv *ds.NV) bool {
	return f.f(f.uAttrName, f.sAttrName, f.attrData)
	//return f.value
	//return f.f(uid, ty)
}

func (f *FilterFunc) PrintName() string {
	return f.name
}

func (f *FilterFunc) GetPredicate() string {
	return f.predicate
}
