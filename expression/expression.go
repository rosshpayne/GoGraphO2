package expression

import (
	"fmt"

	"github.com/DynamoGraph/expression/token"
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

func (e *Expression) Execute() bool {
	Walk(e)
	return e.getResult()
}

func Walk(e operand) {

	if e, ok := e.(*Expression); ok {

		Walk(e.left)
		Walk(e.right)

		switch e.opr {
		case token.AND:

			e.result = e.left.getResult() && e.right.getResult()

		case token.OR:

			e.result = e.left.getResult() || e.right.getResult()

		case token.NOT:

			e.result = !e.right.getResult()

		default: //  represents ( )

			e.result = e.right.getResult()

		}
		fmt.Printf("Result: %v %v\n", e.opr, e.result)
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
	type_() string
	printName() string
	getResult() bool
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
func (e *Expression) type_() string {
	return "Expression"
}
func (e *Expression) printName() string {
	return e.name
}

func (e *Expression) getResult() bool {

	return e.result
}

type dGfunc struct {
	parent *Expression
	value  bool
	name   string // debug purposes
	// uAttrName AttrName
	// sAttrName AttrName
	// attrData  litVal
	// 	cache     []DynaValue                                              // string, int, float, time.Time, bool
	// 	f         func(AttrName, AttrName, litVal, []DynaGValue, int) bool // eq,le,lt,gt,ge,allofterms, someofterms
}

func (f *dGfunc) oper() {}

func (f *dGfunc) getParent() *Expression {
	return f.parent
}

func (f *dGfunc) type_() string {
	return "func"
}

func (f *dGfunc) getResult() bool {
	//return f.f(f.uAttrName, f.sAttrName, f.attrData, f.cache, i)
	return f.value
}

func (f *dGfunc) printName() string {
	if f == nil {
		return "NoName"
	}
	return f.name
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

