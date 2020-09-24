package ast

import (
	"strconv"
	"strings"

	expr "github.com/DynamoGraph/expression"
	"github.com/DynamoGraph/funcs"
	"github.com/DynamoGraph/util"
)

type GQLFunc struct {
	Name      string // eq, le, lt, anyofterms, someofterms
	F         funcs.funcT
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
	switch x := f.Value.(type) {
	case string:
		s.WriteByte('"')
		s.WriteString(x)
		s.WriteByte('"')
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
	Name    Name_
	VarName Name_
	Lang    string
	// (func: eq(name@en, "Steven Spielberg”))
	RootFunc GQLFunc // generates []uid from GSI data io.Writer Write([]byte) (int, error)
	// @filter( has(director.film) )
	Filter *expr.Expression //
	Edge   []EdgeSet
	//
	preds []string
}

func (r *RootStmt) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	r.Name = Name_{Name: input, Loc: loc}
}

func (r *RootStmt) String() string {
	var s strings.Builder

	s.WriteByte('{')
	s.WriteByte('\n')
	s.WriteString(r.Name.String())
	s.WriteString("(func: ")
	s.WriteString(r.RootFunc.String())
	// if r.filter != nil {
	// 	s.WriteString("@filter( ")
	// 	s.WriteString(r.filter.String())
	// }
	s.WriteString("{\n")
	//s.WriteString(r.Edge.String())
	s.WriteByte('}')

	return s.String()
}

// Predicates lists all predicates involved in the root stmt i.e. in RootFunc filter, and edges
func (r *RootStmt) RetrievePredicates() {
	var s []string
	s = append(s, r.RootFunc.Predicate)
	s = append(s, r.Filter.GetPredicates()...)
	s = append(s, r.Edge.GetPredicates()...)

	r.preds = dedup(s)
}

func dedup(s []string) []string {
	var ss []string
	var found bool
	ss = append(ss, s[0])
	for _, e := range s[1:] {
		found = false
		for _, d := range ss {
			if d == e {
				found = true
				break
			}
		}
		if !found {
			ss = append(ss, e)
		}
	}
	return ss
}

// ============== RootResult ==============

type RootResult struct {
	UID   util.UID
	SortK string
	Ty    string
}

func (r *RootStmt) Execute() []RootResult {}

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

// type uid struct {
// 	name
// 	sortk  string // where to source []uid "<partition>#G#<shortname>" source from type
// 	filter *expr.Expression
// 	edge   []EdgeSet
// }

// func (u *uid) Edge() {}

// type filterExpression struct {
// 	e *expr.Expression
// }

// type selection struct {
// 	e *expr.Expression
// }

// func (s selection) Read(p []bytes) (int, error) {
// 	p, n, err := e.Execute()
// 	return n, error

// }

// ============== NameI  ========================

type NameAssigner interface {
	AssignName(name string, Loc *Loc, errS *[]error)
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

func (l Loc) String() string {
	return "at line: " + strconv.Itoa(l.Line) + " " + "column: " + strconv.Itoa(l.Column)
	//return "" + strconv.Itoa(l.Line) + " " + strconv.Itoa(l.Column) + "] "
}

type HasName interface {
	AssignName(string, Loc_, err *[]error)
}
