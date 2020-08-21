package ds

type NdShortNm = string

type ErrNodes map[NdShortNm]*Node

// attribute name-dynamo value place holder
type NV struct {
	Sortk string    // dynamodb sortk value  P#:C (for scalar) P#G#:C (for Nd)
	SName NdShortNm //  node alias name
	Name  string    // predicate name == type attribute name
	DT    string    // datatype N,S,Bl,B, LN,LS,LBL,LB, Nd, SN,SS,SBl, SB
	C     string    // type attribute short name
	Value interface{}
}

type Line struct {
	N    int
	Subj string // shortName "_a" representing a UUID - conversion takes place just before loading into db
	Pred string // two types of entries: 1) __type 2) Name of attribute in the type.
	Obj  string // typeName  or data (scalar, set/list, shortName for UUID )
}

// channel type
type Node struct {
	ID     NdShortNm
	TyName string
	Lines  []Line
	Err    []error // used by verification process to record any errors
}
