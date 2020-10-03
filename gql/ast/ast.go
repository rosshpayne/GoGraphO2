package ast

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/DynamoGraph/db"
	expr "github.com/DynamoGraph/gql/expression"
	"github.com/DynamoGraph/gql/token"
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

type SelectI interface {
	AssignSelectList(SelectList)
}

type SelectList []*EdgeT

func (sl SelectList) String() string {
	var s strings.Builder
	for _, e := range sl {
		s.WriteString(e.String())
		s.WriteByte('\n')
	}
	return s.String()
}

func (sl SelectList) GetPredicates() []string {
	var ps []string
	for _, e := range sl {
		ps = append(ps, e.GetPredicates()...)
	}
	return ps
}

type EdgeT struct {
	Alias   name_
	VarName name_
	Edge    EdgeI
	//f         aggrFunc - now in predicate (edgeT)
}

func (e EdgeT) String() string {
	var s strings.Builder
	if len(e.Alias.Name) > 0 {
		s.WriteString(e.Alias.Name)
		s.WriteString(" : ")
	}
	if len(e.VarName.Name) > 0 {
		s.WriteString(e.VarName.Name)
		s.WriteString(" as ")
	}
	s.WriteString(e.Edge.String())
	return s.String()
}

func (e *EdgeT) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	e.Alias = name_{Name: input, Loc: loc}
}

func (e *EdgeT) AssignVarName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	e.VarName = name_{Name: input, Loc: loc}
}

func (e *EdgeT) GetPredicates() []string {
	return e.Edge.GetPredicates()
}

type EdgeI interface {
	edge()
	String() string
	GetPredicates() []string
}

// type Arg1 interface {
// 	farg()
// }

type ScalarPred struct {
	Name name_
}

func (s ScalarPred) edge() {}
func (s ScalarPred) farg() {}

func (s *ScalarPred) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	s.Name = name_{Name: input, Loc: loc}
}

func (s ScalarPred) String() string {
	return s.Name.Name
}

func (s ScalarPred) GetPredicates() []string {
	return []string{s.Name.Name}
}

type UidPred struct {
	Name   name_
	Filter *expr.Expression
	Select SelectList
}

func (u *UidPred) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	u.Name = name_{Name: input, Loc: loc}
}
func (u *UidPred) AssignSelectList(s SelectList) {
	u.Select = s
}

func (u *UidPred) GetPredicates() []string {
	ps := []string{}
	ps = append(ps, u.Name.Name)
	// 	ps = append(ps, u.Filter.GetPredicates()...)
	ps = append(ps, u.Select.GetPredicates()...)
	return ps
}

func (p *UidPred) edge() {}

//func (p *UIDPred) innerArg() {}
func (p *UidPred) aggrArg() {}
func (p *UidPred) count_()  {}
func (p *UidPred) String() string {
	var s strings.Builder
	s.WriteString(p.Name.Name)
	// Filter
	if p.Select != nil {
		s.WriteString("{\n")
		s.WriteString(p.Select.String())
		s.WriteByte('}')
	}
	return s.String()
}

type Variable struct {
	Name name_
}

func (u *Variable) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	u.Name = name_{Name: input, Loc: loc}
}
func (u *Variable) String() string {
	return "var(" + u.Name.Name + ")"
}

//func (r *Variable) innerArg()  {}
func (r *Variable) edge() {}

// not for root func: func (r *Variable) farg() {}

//func (r *Variable) innerFunc() {}
func (r *Variable) aggrArg() {}
func (r *Variable) GetPredicates() []string {
	return nil
}

//type ValFuncT func(v Variable) ValOut

type AggrArg interface {
	aggrArg()
}
type AggrFunc struct {
	Name name_ // count, avg takes either a variable argument or a uid-pred argument
	Arg  AggrArg
}

func (u *AggrFunc) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	u.Name = name_{Name: input, Loc: loc}
}

func (e *AggrFunc) edge() {}

//func (e *AggrFunc) innerFunc() {}

type CounterI interface {
	count_()
	String() string
	GetPredicates() []string
}

type CountFunc struct {
	Arg CounterI // uidPred, UID
}

func (e *CountFunc) farg() {}
func (e *CountFunc) edge() {}
func (e *CountFunc) String() string {
	var s strings.Builder
	s.WriteString("count(")
	s.WriteString(e.Arg.String())
	s.WriteByte(')')
	return s.String()
}

func (e *CountFunc) GetPredicates() []string {
	return e.Arg.GetPredicates()
}

type UID struct{}

func (e UID) edge()   {}
func (e UID) count_() {}
func (e UID) String() string {
	return "uid"
}
func (e UID) GetPredicates() []string {
	return nil
}

type Values []interface{} // int,float,string,$var

// =========================  GQLFunc  =============================================

type FuncT func(FargI, interface{}) []db.QResult

//type FuncT func(predfunc FargI, value interface{}, nv []ds.NV, ty string) []db.QResult

type GQLFunc struct {
	Name name_ // for String() purposes
	F    FuncT
	Farg FargI // either predicate, count, var
	//	IFarg InnerArgI   // either uidPred, variable
	Value interface{} //  string,int,float,$var, List of string,int,float,$var
}

func (g *GQLFunc) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	g.Name = name_{Name: input, Loc: loc}
}

func (g *GQLFunc) Execute() []db.QResult {
	//
	return g.F(g.Farg, g.Value)
}

func (f *GQLFunc) String() string {
	//
	var s strings.Builder
	s.WriteString(f.Name.String())
	s.WriteByte('(')
	if f.Farg != nil {
		switch x := f.Farg.(type) {
		case *CountFunc:
			s.WriteString("count(")
			switch y := x.Arg.(type) {
			case *UidPred:
				s.WriteString(y.Name.String())
			case UID:
				s.WriteString("uid")
			}
			s.WriteString(")")
		case ScalarPred:
			s.WriteString(x.Name.String())
			//
		}
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
	Name name_
	Var  *Variable
	Lang string
	// (func: eq(name@en, "Steven Spielberg”))
	RootFunc GQLFunc // generates []uid from GSI data io.Writer Write([]byte) (int, error)
	// @filter( has(director.film) )
	Filter *expr.Expression //
	Select SelectList
	//
	PredList []string
}

func (r *RootStmt) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	r.Name = name_{Name: input, Loc: loc}
}

func (r *RootStmt) AssignSelectList(s SelectList) {
	r.Select = s
}

func (r *RootStmt) AssignVarName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	r.Var.Name = name_{Name: input, Loc: loc}
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
	s.WriteString(r.Select.String())
	s.WriteByte('}')

	s.WriteByte('}')

	return s.String()
}

// Predicates lists all predicates involved in the root stmt i.e. in RootFunc filter, and edges
func (r *RootStmt) RetrievePredicates() []string {
	var s []string
	if r.RootFunc.Farg != nil {
		switch x := r.RootFunc.Farg.(type) {
		case *CountFunc:
			switch y := x.Arg.(type) {
			case *UidPred:
				s = append(s, y.Name.Name)
			}
		case ScalarPred:
			s = append(s, x.Name.Name)
		}
	}
	// s = append(s, r.Filter.GetPredicates()...)
	s = append(s, r.Select.GetPredicates()...)

	return s
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

// ============== QResult ==============

// type QResult struct {
// 	UID   util.UID
// 	SortK string
// 	Ty    string
// }

func (r *RootStmt) Execute() []db.QResult {
	//
	// execute root func
	//
	uids := r.RootFunc.F(r.RootFunc.Farg, r.RootFunc.Value)

	for _, u := range uids {
		fmt.Printf("%#v", u)
	}
	return uids
}

// ============== NameI  ========================

type NameAssigner interface {
	AssignName(string, token.Pos)
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
	Loc  token.Pos
}

func (n name_) String() string {
	return string(n.Name)
}

func (n name_) AtPosition() string {
	if n.Loc.Col == 0 && n.Loc.Line == 0 {
		//panic(fmt.Errorf("Error in AtPosition(), Loc not set"))
		return "Loc not set"
	}
	return "" + strconv.Itoa(n.Loc.Line) + " " + strconv.Itoa(n.Loc.Col) + "] "
}

func (n name_) Exists() bool {
	if len(n.Name) > 0 {
		return true
	}
	return false
}

// // =========== Loc_ =============================

// type Loc struct {
// 	Line   int
// 	Column int
// }

// func (l Loc) String() string {
// 	return "at line: " + strconv.Itoa(l.Line) + " " + "column: " + strconv.Itoa(l.Column)
// 	//return "" + strconv.Itoa(l.Line) + " " + strconv.Itoa(l.Column) + "] "
// }

// type HasName interface {
// 	AssignName(string, Loc_,)
// }
