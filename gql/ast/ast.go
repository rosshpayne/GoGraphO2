package ast

import (
	"strconv"
	"strings"

	expr "github.com/DynamoGraph/expression"
	"github.com/DynamoGraph/funcs"
	"github.com/DynamoGraph/util"
)

type FargI interface {
	farg()
}

type InnerFuncI interface {
	innerFunc()
}

// type InnerArgI interface {
// 	innerArg()
// }

type SelectList []*EdgeT

type EdgeT struct {
	Alias   name_
	VarName name_
	Edge    EdgeI
	//f         aggrFunc - now in predicate (edgeT)
}

func (e *EdgeT) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	e.Alias = name_{Name: input, Loc: loc}
}

func (e *EdgeT) AssignVarName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	e.VarName = name_{Name: input, Loc: loc}
}

type EdgeI interface {
	edge()
}

// type Arg1 interface {
// 	farg()
// }

type ScalarPred struct {
	Name name_
}

func (s ScalarPred) edge() {}
func (s ScalarPred) farg() {}

func (s *ScalarPred) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	s.Name = name_{Name: input, Loc: loc}
}

type UidPred struct {
	Name   name_
	Filter *expr.Expression
	Select SelectList
}

func (u *UidPred) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	u.Name = name_{Name: input, Loc: loc}
}

func (p *UIDPred) edge() {}

//func (p *UIDPred) innerArg() {}
func (p *UIDPred) aggrArg() {}
func (p *UIDPred) count_()  {}

type Variable struct {
	Name name_
}

func (u *Variable) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	u.Name = name_{Name: input, Loc: loc}
}

//func (r *Variable) innerArg()  {}
func (r *Variable) edge() {}
func (r *Variable) farg() {}

//func (r *Variable) innerFunc() {}
func (r *Variable) aggrArg() {}

//type ValFuncT func(v Variable) ValOut

type AggrArg interface {
	aggrArg()
}
type AggrFunc struct {
	FName name_ // count, avg takes either a variable argument or a uid-pred argument
	Arg   AggrArg
}

func (e *AggrFunc) edge() {}

//func (e *AggrFunc) innerFunc() {}

type Counter interface {
	count_()
}

type CountFunc struct {
	Arg Counter // uidPred, UID
}

func (e *CountFunc) farg() {}

type UID struct{}

func (e *UID) edge()   {}
func (e *UID) count_() {}

type Values []interface{} // int,float,string,$var

// =========================  GQLFunc  =============================================

type FuncT func(predfunc FargI, value interface{}, nv []ds.NV, ty string) bool

type GQLFunc struct {
	FName name_ // for String() purposes
	F     FuncT
	Farg  FargI // either predicate, count, var
	//	IFarg InnerArgI   // either uidPred, variable
	Value interface{} //  string,int,float,$var, List of string,int,float,$var
}

func (g *GQLFunc) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	g.Name = name_{Name: input, Loc: loc}
}

func (g *GQLFunc) Execute() {
	//
	if g.IF != nil {
		return g.F(g.IF(g.IFarg), g.Value)
	}
	return g.F(g.Farg, g.Value)
}

func (f *GQLFunc) String() string {
	//
	var s strings.Builder
	s.WriteString(f.FName)
	s.WriteByte('(')
	if len(f.IFName) > 0 {
		s.WriteString(f.IFName)
		s.WriteByte('(')
		//
		switch x := f.IFarg.(type) {
		case UidPred:
			s.WriteString(x.Name)
		case Variable:
			s.WriteString(x.Name)
		}
		//
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
		// list of literals, list of $varN...
	}
	s.WriteString("))")

	return s.String()
}

// ============== Select/edge List ==============

// {
//   ID as var(func: allofterms(name@en, "Steven")) @filter(has(director.film)) {
//     director.film {
//       num_actors as count(starring)
//     }
//     average as avg(val(num_actors))
//   }

//   films(func: uid(ID)) {
//     director_id : uid
//     english_name : name@en
//     average_actors : val(average)
//     num_films : count(director.film)

//     films : director.film {
//       name : name@en
//       english_name : name@en
//       french_name : name@fr
//     }
//   }
// }
// {
//   me(func: eq(name@en, "Steven Spielberg")) @filter(has(director.film)) {
//     name@en
//     director.film @filter(allofterms(name@en, "jones indiana"))  {
//       name@en
//     }
//   }
// }

//func (s *ScalarPred) farg() {}

// ============== RootStmt ==============

type RootStmt struct {
	Name    name_
	VarName name_
	Lang    string
	// (func: eq(name@en, "Steven Spielberg”))
	RootFunc GQLFunc // generates []uid from GSI data io.Writer Write([]byte) (int, error)
	// @filter( has(director.film) )
	Filter *expr.Expression //
	Select SelectList
	//
	PredList []string
}

func (r *RootStmt) AssignName(input string, loc *Loc, err *[]error) {
	//ValidateName(input, err, Loc)
	r.Name = name_{Name: input, Loc: loc}
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

// ===============  name_  =========================

type name_ struct {
	Name string
	Loc  *Loc
}

func (n name_) String() string {
	return string(n.Name)
}

func (n name_) AtPosition() string {
	if n.Loc == nil {
		//panic(fmt.Errorf("Error in AtPosition(), Loc not set"))
		return "Loc not set"
	}
	return n.Loc.String()
}

func (n name_) Exists() bool {
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
