// package expression only used for filter operations
// rootfunc needs only lexer/parser to determine func to execute, which returns []{[]util.UID, Type}, where each item is passed to expression to return true/false

package expression

import (
	"fmt"

	"github.com/DynamoGraph/ds"
	"github.com/DynamoGraph/expression/token"
	"github.com/DynamoGraph/funcs"
	"github.com/DynamoGraph/util"
)

//
// literal Value used in functions to specify search criteria value
//

//
// rootFunc appears at top of GraphQL+- query. Returns initial candidate UID set.
//

type operator = string

// Connectives AND, OR and NOT join filters and can be built into arbitrarily complex filters, such as (NOT A OR B) AND (C AND NOT (D OR E)).
// Note that, NOT binds more tightly than AND which binds more tightly than OR.

type Item struct {
	uid util.UID
	ty  string
}

func (e *Expression) Execute(nv []ds.NV) bool {
	// nc, err := cache.FetchNode(d.uid, "A#")
	// if err != nil {
	// 	panic(err) //TODO ; how to handle errors - errmgr routine?
	// }
	// err = nc.UnmarshalCache(nv)
	walk(e, nv)
	return e.getResult(nv)
}

func walk(e operand, nv []ds.NV) {

	if e, ok := e.(*Expression); ok {

		walk(e.left, nv)
		walk(e.right, nv)

		switch e.opr {
		case token.AND:

			e.result = e.left.getResult(nv) && e.right.getResult(nv)

		case token.OR:

			e.result = e.left.getResult(nv) || e.right.getResult(nv)

		case token.NOT:

			e.result = !e.right.getResult(nv)

		default: //  represents ( )

			e.result = e.right.getResult(nv)

		}
		fmt.Printf("Result: %v %v\n", e.opr, e.result)
	}

}

var pred []string

func (e *Expression) GetPredicates(op operand) []string {

	pred = nil

	walkPreds(op)

	return pred

}

func walkPreds(e operand) {

	if e, ok := e.(*Expression); ok {

		walkPreds(e.left)
		walkPreds(e.right)

		pred = append(pred, e.left.getPredicate())
		pred = append(pred, e.right.getPredicate())

	}

}

func findRoot(e *Expression) *Expression {

	for e.parent != nil {
		e = e.parent
	}
	return e
}

// operand interface.
// So far type num (integer), Expression satisfy, but this can of course be extended to floats, complex numbers, functions etc.
type operand interface {
	getParent() *Expression
	getPredicate() string
	type_() string
	printName() string
	getResult(nv []ds.NV) bool
}

type Expression struct { // expr1 and expr2     expr1 or expr2       exp1 or (expr2 and expr3). (expr1 or expr2) and expr3
	id     uint8           // type of Expression. So far used only to identify the NULL Expression, representing the "(" i.e the left parameter or LPARAM in a mathematical Expression
	name   string          // optionally give each Expression a name. Maybe useful for debugging purposes.
	result bool            // store result of "left operator right. Walking the graph will interrogate each operand for its result.
	left   operand         //
	opr    token.TokenType // for Boolean: AND OR NOT NULL (aka "(")
	right  operand         //
	parent *Expression
}

func (e *Expression) getParent() *Expression {
	return e.parent
}
func (e *Expression) gype_() string {
	return "Expression"
}
func (e *Expression) printName() string {
	return e.name
}

func (f *Expression) getPredicate() string {
	return ""
}

func (e *Expression) type_() string {
	return "expression"
}

func (e *Expression) getResult(nv []ds.NV) bool {

	return e.result
}

// =============================================================================
// ( eq(predicate, value) and eq(..)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

//   eq(count(genre), 13))

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

func (f *FilterFunc) oper() {} // ???

func (f *FilterFunc) getParent() *Expression {
	return f.parent
}

func (f *FilterFunc) type_() string {
	return "func"
}

func (f *FilterFunc) getResult(nv []ds.NV) bool {
	return true
	//return f.f(f.uAttrName, f.sAttrName, f.attrData)
	//return f.value
	//return f.f(uid, ty)
}

func (f *FilterFunc) printName() string {
	return f.name
}

func (f *FilterFunc) getPredicate() string {
	return f.predicate
}

func makeExpr(l operand, op token.TokenType, r operand) (*Expression, token.TokenType) {

	fmt.Println("MakeExpression: ", op)

	e := &Expression{left: l, opr: op, right: r}

	return e, ""
}

// ExtendRight for Higher Precedence operators or open braces - parsed:   *,/, (
// c - current op node, n is the higer order op we want to extend right
func (c *Expression) extendRight(n *Expression) *Expression {

	c.right = n
	n.parent = c

	fmt.Printf("++++++++++++++++++++++++++ extendRight  FROM %s  -> [%s]  \n", c.opr, n.opr)
	return n
}

func (c *Expression) addParent(n *Expression) *Expression {
	//
	fmt.Println("addParent on ", c.opr, n.opr)
	if c.parent != nil {
		//  current node must now point to the new node being added, and similar the new node must point back to the current node.
		c.parent.right = n
		n.parent = c.parent
	}
	// set old parent to new node
	c.parent = n
	n.left = c

	fmt.Printf("\n++++++++++++++++++++++++++ addParent  %s on %s \n\n", n.opr, c.opr)
	return n
}
