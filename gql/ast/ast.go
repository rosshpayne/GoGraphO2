package ast

import (
	"fmt"
	"strings"

	expr "github.com/DynamoGraph/expression"
)

// ( eq(predicate, value) and eq(..)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

//   eq(count(genre), 13))

type Document struct {
	Stmt []GQLstmt
}

type Result struct {
	UID util.UID
	Ty  string
}
type Gqlfunc func() []Result

type GQLFunc struct {
	Name      string // eq, le, lt, anyofterms, someofterms
	F         Gqlfunc
	Predicate string      // Name,
	Value     interface{} // scalar int, bool, float, string. List of string. List of $var, string.
	Modifier  string      // count(), val()
}

func (f *GQLFunc) String() string {
	var s strings.Builder
	s.WriteString(f.Name)
	s.WriteByte('(')
	if len(f.Modifier) > 0 {
		s.WriteString(f.Modifier)
		s.WriteByte('(')
		s.WriteString(f.Predicate)
		s.WriteByte(')')
	} else {
		s.WriteString(f.Predicate)
	}
	s.WriteByte(',')
	switch x := f.Value.(type); x {
	case string:
		s.WriteString(x)
	case int:
		s.WriteString(strconv.Itoa(x))
	case float64:
		s.WriteString(strconv.FormatFloat(x, 'G', -1, 64))
	}
	s.WriteString("))")

	return s.String()
}

// ==============. RootStmt. ==============

type RootStmt struct {
	name    Name_
	varName Name_
	// (func: eq(name@en, "Steven Spielberg”))
	rootFunc GQLFunc // generates []uid from GSI data io.Writer Write([]byte) (int, error)
	// @filter( has(director.film) )
	filter *expr.Expression // io.Pipe between Selection and Filter io.Reader Read([]byte) (int, error_)
	edge   []EdgeSet
}

func (r *RootStmt) AssignName(input string, loc *ast.Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	r.Name = Name_{Name: input, Loc: loc}
}

func (r *RootStmt) String() string {
	var s strings.Builder

	s.WriteByte('{')
	s.WriteByte('\n')
	s.WriteString(r.Name)
	s.WriteString("(func: ")
	s.WriteString(r.rootFunc.String())
	if r.filter != nil {
		s.WriteString("@filter( ")
		s.WriteString(r.filter.String())
	}
	s.WriteString("{\n")
	s.WriteString(r.edge.String())
	s.WriteByte('}')

	return s.String()
}

// ==============. EdgeSet. ==============

type EdgeSet interface {
	Edge()
}

type scalar struct {
	name  string
	sortk string      // "<partition>#<shortname>" source from type
	value interface{} //value is sourced during execution phase
}

func (s *scalar) Edge() {}

type uid struct {
	name
	sortk  string // where to source []uid "<partition>#G#<shortname>" source from type
	filter *expr.Expression
	edge   []EdgeSet
}

func (u *uid) Edge() {}

type filterExpression struct {
	e *expr.Expression
}

type selection struct {
	e *expr.Expression
}

func (s selection) Read(p []bytes) (int, error) {
	p, n, err := e.Execute()
	return n, error

}

// ============== NameI  ========================

type NameAssigner interface {
	AssignName(name string, Loc *Loc_, errS *[]error)
}

// ===============  NameValue_  =========================

// type NameValue_ string

// func (n NameValue_) String() string {
// 	return string(n)
// }

// func (a NameValue_) Equals(b NameValue_) bool {
// 	return string(a) == string(b)
// }

// func (a NameValue_) EqualString(b string) bool {
// 	return string(a) == b
// }

// ===============  Name_  =========================

type Name_ struct {
	Name string
	Loc  *Loc
}

func (n Name_) String() string {
	return string(n.Name)
}

func (a Name_) Equals(b Name_) bool {
	return a.Name.Equals(b.Name)
}

func (a Name_) EqualString(b string) bool {
	return a.Name.EqualString(b)
}

func (n Name_) AtPosition() string {
	if n.Loc == nil {
		//panic(fmt.Errorf("Error in AtPosition(), Loc not set"))
		return "Loc not set"
	}
	return n.Loc.String()
}

func (n Name_) Exists() bool {
	if len(n.Name) > 0 {
		return true
	}
	return false
}

// =========== Loc_ =============================

type Loc struct {
	Line   int
	Column int
}

func (l Loc_) String() string {
	return "at line: " + strconv.Itoa(l.line) + " " + "column: " + strconv.Itoa(l.column)
	//return "" + strconv.Itoa(l.Line) + " " + strconv.Itoa(l.Column) + "] "
}

type HasName interface {
	AssignName(string, Loc_, err *[]error)
}
