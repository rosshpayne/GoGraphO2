package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"strings"

	"github.com/DynamoGraph/db"

	"github.com/satori/go.uuid"
)

// type dgraphDT int

// const (
// 	String dgraphDT = iota
// 	Time
// 	Float
// 	Bool
// 	StringS
// 	TimeS
// 	FloatS
// 	BoolS
// 	NodeS
// )

// type DynaGType uint8

// const (
// 	//
// 	// scalar types
// 	//
// 	N  DynaGType = iota // number
// 	S                   // string
// 	Bl                  // bool
// 	B                   // []byte
// 	//
// 	// List (ordered set of any type but constrainted to a single type in DynaGraph)
// 	//
// 	LS  // []string
// 	LN  // []string
// 	LB  // [][]byte
// 	LBl // []bool
// 	Nd  // [][]byte // list of node UIDs
// 	//
// 	// Set (unordered set of a single type)
// 	//
// 	NS // []string
// 	SS // []string
// 	BS // [][]byte
// 	//
// )

// type gsiResult struct {
// 	PKey  []byte
// 	SortK string
// }

// // DynaGValue
// type DynaGValue struct {
// 	PKey  []byte
// 	SortK string
// 	//attrName string
// 	//
// 	// scalar types
// 	//
// 	N  string // numbers are represented by strings and converted on the fly to dynamodb number
// 	S  string
// 	Bl bool
// 	B  []byte
// 	//
// 	// List (ordered set of any type but constrainted to a single type in DynaGraph)
// 	//
// 	LS  []string
// 	LN  []string
// 	LB  [][]byte
// 	LBl []bool
// 	Nd  [][]byte // list of node UIDs
// 	//
// 	// Set (unordered set of a single type)
// 	//
// 	NS []string
// 	SS []string
// 	BS [][]byte
// }

// func (s *ScalarT) dtype() {}

// func (s *ScalarT) GetFloat() float64 {
// 	return s.f
// }

// func (s *ScalarT) GetFloatSet() []float64 {
// 	return s.nS
// }

// func (s *ScalarT) GetString() string {
// 	return s.str
// }

// func (s *ScalarT) String() {

// }

// //type DynaFunc func(AttrName, AttrName, LitVal, []DynaGValue, int) bool

// type UIDcache struct {
// 	uAttrName AttrName
// 	sAttrName AttrName
// 	attrData  LitVal
// 	f         func(AttrName, AttrName, LitVal, []DynaGValue, int) bool // eq,le,lt,gt,ge,allofterms, someofterms
// }

// func (c *UIDcache) GetResult(data []DynaGValue, i int) bool {
// 	return c.f(c.uAttrName, c.sAttrName, c.attrData, data, i)
// }

// func (c *UIDcache) Init(u AttrName, s AttrName, data LitVal) bool {
// 	c.uAttrName = u
// 	c.sAttrName = s
// 	c.attrData = data
// }

// type FacetNameT string

// type FacetT struct {
// 	name   FacetNameT
// 	destNd *NodeT
// 	// scalar single values
// 	n   float64
// 	str string
// 	bl  bool
// 	dt  time.Time
// 	bn  []byte // binary
// }
// type uidT = uuid.UUID

// type attrValueT interface {
// 	dtype()
// }

// type attributeT struct {
// 	predicate attrNameT  // predicate
// 	object    attrValueT // object .  Scalar, []NodeT
// }

// //type edge string // "predicate#object.UID"
// type facetMap map[string][]*FacetT

// //
// // Node - subject of spo
// //
// type NodeT struct {
// 	UID    uidT         // uid representation
// 	UIDs   string       // base64 of binary uid
// 	attr   []attributeT // predicate-object, Scalar, []NodeT
// 	facets facetMap
// }

// type NodeL []*NodeT

// func (nl NodeL) dtype() {}

// func CreateNode() (*NodeT, error) {
// 	u, err := uuid.NewV4()
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	var uuibin []byte
// 	uuibin, err = u.MarshalBinary()
// 	uuibins := base64.StdEncoding.EncodeToString(uuibin)

// 	node := NodeT{UID: u, UIDs: uuibins}
// 	// add to node map
// 	if _, ok := nodeMap[uuibins]; ok {
// 		return nil, fmt.Errorf("UID entry already exists in uid map")
// 	}
// 	nodeMap[uuibins] = &node
// 	return &node, nil
// }

// func (n *NodeT) AddEdgeNode(name attrNameT, destNd *NodeT) error {
// 	for _, v := range n.attr {
// 		if v.predicate == name {
// 			if vv, ok := v.object.(NodeL); !ok {
// 				return fmt.Errorf("Expected type []*NodeT")
// 			} else {
// 				vv = append(vv, destNd)
// 			}
// 		}
// 	}
// 	return nil
// }

// func (n *NodeT) AddEdgeFacet(name attrNameT, destNd *NodeT, facet *FacetT) error {
// 	var found bool
// 	for _, v := range n.attr {
// 		if v.predicate == name {
// 			found = true
// 			if vv, ok := v.object.(NodeL); !ok {
// 				log.Printf("Value is not appropriate type []*NodeT")
// 				return fmt.Errorf("Value is not appropriate type []*NodeT")
// 			} else {
// 				// search for edge ie combination of predicate + destNd
// 				var foundEdge bool
// 				for _, e := range vv {
// 					if e.UIDs == destNd.UIDs {
// 						foundEdge = true
// 						break
// 					}
// 				}
// 				if !foundEdge {
// 					vv = append(vv, destNd)
// 				}
// 			}
// 		}
// 	}
// 	if !found {
// 		return fmt.Errorf(fmt.Sprintf(`Attribute "%s" not found`, name))
// 	}
// 	//
// 	// add facet to edge
// 	//
// 	if n.facets == nil {
// 		n.facets = make(facetMap)
// 	}
// 	//
// 	// edge defined predicate#destNodeUID
// 	//
// 	edge := string(name) + "#" + destNd.UIDs
// 	var (
// 		facetS []*FacetT
// 		ok     bool
// 	)
// 	if facetS, ok = n.facets[edge]; !ok {
// 		// no map entry so
// 		facetS = make([]*FacetT, 1, 3) // allocate less storage than append
// 		//facetS = append(facetS, facet) - this will allocate approx 10 elements
// 		facetS[0] = facet
// 		n.facets[edge] = facetS
// 		return nil
// 	}
// 	// check facet doesn't already exist
// 	found = false
// 	for i, v := range facetS {
// 		if v.name == facet.name {
// 			// overwrite
// 			facetS[i] = facet
// 			found = true
// 			break
// 		}
// 	}
// 	if !found {
// 		facetS = append(facetS, facet)
// 		n.facets[edge] = facetS // reassign incase append has reallocated slice
// 	}
// 	return nil
// }

// func (n *NodeT) AddScalarAttr() {

// }
// func (n *NodeT) AddSetAttr() {

// }

// type rootNodes []*NodeT

// type rootNode *rootNodes

// type uidMap map[string]*NodeT

// var nodeMap uidMap

// type DictT struct {
// 	Attribute string
// 	Id        int // ?
// 	Type      dgraphDT
// }

// type dict map[attrNameT]dgraphDT

//type uidMap map[uidT]*NodeT

// func init() {
// 	nodeMap = make(uidMap)
// }

//type rootFunc func(attrName, litVal) uidS

// LitVal is literal value of input value. Equiv to token.Literal really, so LitVal may not be necessary.
//type LitVal string // DD hold uAttr.sAttr type which Go can then convert literal value from string e.g. String -> Bool

// type LitVal interface {
// 	literal()
// 	getValue() interface{}
// }

// type AttrName = string

// type LitvalS string

// func (s *litvalS) literal() {}
// func (s *litvalS) getValue() interface{} {
// 	return s
// }

// type LitvalF float64

// func (n *litvalF) literal() {}
// func (n *litvalS) getValue() interface{} {
// 	return n
// }

// type LitvalI int

// func (n *litvalI) literal() {}

// type LitvalBl bool

// func (b *litvalBl) literal() {}

// type DynaGType uint8

// func getDD(d string) DynaGType {
// }

// root filter:  @filter( le(name, "Ian Payne") )     { attrKey: name          i.e. film nodes with name = Ian Payne
//                                                      this is satisfied by GSI query in db package - which loads datacache
//
// uid filter:   @filter( le(name, "Lachlan Payne") ) { attrKey: siblings:name i.e. the person node child node (name=Lachlan Payne)
//      												this is satsified by code below

func eqRoot(ty Ty, uAttr AttrName, sAttr AttrName, lv litVal, r db.DataCache) bool {
	// uAttrName -> parent uid attribute name (starring)
	// scalarAttrName -> child scalar attribute name (name) ie. starring.name = 'Scott Ridley' ie. starring @filter(eq(name,'Scott Ridley'))
	// r -> node data from Dynamodb
	// i -> child node index into #A#G attribute data
	//
	// for i,v := range r {
	//	if v.SortK == "%A%A%<uidAttrName>" {    // "%A%D%Starring"
	//		for i,nd :=range v.Nd {
	// 			if le(<uidAttrName>,"name",r,i) {
	// 				output(v,i)
	// 		}
	//	}
	// }

	var (
		aty    AttrType
		attrDT string
		ok     bool
	)

	genAttrKey := func(u, s string) string {
		var searchKey string
		var pd strings.Builder

		// request for child attribute
		searchKey = "#G"
		for i := 0; i < 2; i++ {
			switch {
			case i == 0:
				if aty, ok = db.AttrTyCache[ty+":"+u]; !ok {
					return ""
				}
			case i == 1:
				if aty, ok = db.AttrTyCache[ty+":"+s]; !ok {
					return ""
				}
			}
			if i == 0 {
				pd.WriteString(aty.P) // item partition
				pd.WriteString(searchKey)
			}
			pd.WriteString("#:")
			pd.WriteString(aty.C) // attribute compressed identifier
		}
		if aty.DT != "Nd" {
			attrDT = "L" + aty.DT
		}
		fmt.Println(" attrDT, ty : ", attrDT, ty)

		// build a item clause

		return pd.String()
	}

	attrKey := genAttrKey(uattr, sattr)
	fmt.Printf("attrKey: [%s]\n", attrKey)

	for _, v := range r {
		// match attribute descriptor
		if v.SortK == attrKey {
			// we now know the attribute data type, populate interface value with attribute data
			switch attrDT {
			case "LS": // list string

				s := r.GetLS()
				if lv, ok := litval.(string); ok {
					if lv == s[i] {
						return true
					} else {
						return false
					}
				}
				panic(fmt.Errorf("Literval value passed to func eq should be a string"))

			case "LF": // list float

				f := r.GetLF()
				if lv, ok := litval.(float64); ok {
					if lv == f[i] {
						return true
					} else {
						return false
					}
				}
				panic(fmt.Errorf("Literval value passed to func eq should be a float"))

			case "LI": // list int

				i := r.GetLI()
				if lv, ok := litval.(int64); ok {
					if lv == i[i] {
						return true
					} else {
						return false
					}
				}
				panic(fmt.Errorf("Literval value passed to func eq should be an int"))

			case "LB": // List []byte
				a.Value = r.GetLB()

			case "LBl": // List bool

				bl := r.GetLBl()
				if lv, ok := litval.(bool); ok {
					if lv == bl[i] {
						return true
					} else {
						return false
					}
				}
				panic(fmt.Errorf("Literval value passed to func eq should be a bool"))

			default:
				panic(fmt.Errorf("Unsupported data type %q", attrDT))
			}

		}
	}
	return nil

}

var eqMap map[string][]bool

func init() {
	eqMap = make(map[string][]bool)
}

type litVal interface{}

type AttrName = string
type Ty = string

// eq_ function performs complete search on a uid attribute data comparing values against a literal value. Stores results in a cache for later querying.
// cache is destroyed when last item (child node) is queried.
// does not return result. Use eqFetch to get results one value at a time.
func eqUID(ty Ty, uAttr AttrName, sAttr AttrName, lv litVal, r db.DataCache) (string, error) {

	// func eq(i int, u ...uuid.UUID) (bool, error) {

	// uAttrName -> parent uid attribute name (starring)
	// scalarAttrName -> child scalar attribute name (name) ie. starring.name = 'Scott Ridley' ie. starring @filter(eq(name,'Scott Ridley'))
	// r -> node data from Dynamodb
	// i -> child node index into #A#G attribute data
	//
	var (
		aty    db.AttrType
		ok     bool
		err    error
		result []bool
	)

	genAttrKey := func(u, s string) string {
		var searchKey string
		var pd strings.Builder

		// request for child attribute
		searchKey = "#G"
		for i := 0; i < 2; i++ {
			switch {
			case i == 0:
				if aty, ok = db.AttrTyCache[ty+":"+u]; !ok {
					return ""
				}
			case i == 1:
				if aty, ok = db.AttrTyCache[ty+":"+s]; !ok {
					return ""
				}
			}
			if i == 0 {
				pd.WriteString(aty.P) // item partition
				pd.WriteString(searchKey)
			}
			pd.WriteString("#:")
			pd.WriteString(aty.C) // attribute compressed identifier
		}
		// build a item clause
		return pd.String()
	}
	fmt.Println(uAttr, sAttr)
	attrKey := genAttrKey(uAttr, sAttr)
	fmt.Printf("attrKey: [%s]\n", attrKey)

	for _, v := range r {
		// match attribute descriptor
		if v.SortK == attrKey {
			// we now know the attribute data type, populate interface value with attribute data
			switch "L" + aty.DT {

			case "LS": // list of strings

				s := v.GetLS()
				if lv, ok := lv.(string); !ok {
					panic(fmt.Errorf("Literval value passed to func eq should be a string"))
				} else {
					result = make([]bool, len(s), len(s))
					switch {
					case aty.N: // attribute may not exist. Use X attribute to determine if current attribute is populated.
						isNull := v.GetX()
						if len(isNull) != len(s) {
							panic(fmt.Errorf("data inconsistency. Len of nullable X attribute does not match len of data attribute"))
						}
						for i, ds := range s {
							if isNull[i] { // for this item attribute is not populated ie. does not exist, so set result to false.
								result[i] = false
							} else {
								result[i] = lv == ds
							}
						}
					default: // attribute will always exist
						for i, ds := range s {
							result[i] = lv == ds
						}
					}
				}

			case "LF": // list of floats

				s := v.GetLF()
				if lv, ok := lv.(float64); !ok {
					panic(fmt.Errorf("Literval value passed to func eq should be a float"))
				} else {
					result = make([]bool, len(s), len(s))
					switch {
					case aty.N: // attribute may not exist. Use X attribute to determine if current attribute is populated.
						isNull := v.GetX()
						if len(isNull) != len(s) {
							panic(fmt.Errorf("data inconsistency. Len of nullable X attribute does not match len of data attribute"))
						}
						for i, ds := range s {
							if isNull[i] { // for this item attribute is not populated ie. does not exist, so set result to false.
								result[i] = false
							} else {
								result[i] = lv == ds
							}
						}
					default: // attribute will always exist
						for i, ds := range s {
							result[i] = lv == ds
						}
					}
				}

			case "LI": // list int

				s := v.GetLI()
				if lv, ok := lv.(int64); !ok {
					panic(fmt.Errorf("Literval value passed to func eq should be a int64"))
				} else {
					result = make([]bool, len(s), len(s))
					switch {
					case aty.N: // attribute may not exist. Use X attribute to determine if current attribute is populated.
						isNull := v.GetX()
						if len(isNull) != len(s) {
							panic(fmt.Errorf("data inconsistency. Len of nullable X attribute does not match len of data attribute"))
						}
						for i, ds := range s {
							if isNull[i] { // for this item attribute is not populated ie. does not exist, so set result to false.
								fmt.Println("NULL VALUE presented - set to false.....")
								result[i] = false
							} else {
								result[i] = lv == ds
							}
						}
					default: // attribute will always exist
						for i, ds := range s {
							result[i] = lv == ds
						}
					}
				}

			case "LB": // List []byte

			case "LBl": // List bool

				s := v.GetLBl()
				if lv, ok := lv.(bool); !ok {
					panic(fmt.Errorf("Literval value passed to func eq should be a string"))
				} else {
					result = make([]bool, len(s), len(s))
					switch {
					case aty.N: // attribute may not exist. Use X attribute to determine if current attribute is populated.
						isNull := v.GetX()
						if len(isNull) != len(s) {
							panic(fmt.Errorf("data inconsistency. Len of nullable X attribute does not match len of data attribute"))
						}
						for i, ds := range s {
							if isNull[i] { // for this item attribute is not populated ie. does not exist, so set result to false.
								result[i] = false
							} else {
								result[i] = lv == ds
							}
						}
					default: // attribute will always exist
						for i, ds := range s {
							result[i] = lv == ds
						}
					}
				}

			default:
				panic(fmt.Errorf("Unsupported data type %q", aty.DT))
			}
			break
		}
	}
	// save result for later queries
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	var uuibin []byte
	uuibin, err = uid.MarshalBinary()
	uuibins := base64.StdEncoding.EncodeToString(uuibin)
	eqMap[uuibins] = result

	return uuibins, err
}

func eq(i int, uid string) bool {
	var (
		res []bool
		ok  bool
	)
	//
	// retrieve results array from cache
	//
	if res, ok = eqMap[uid]; !ok {
		panic(fmt.Errorf("UID passed to eq function does not exist in cache"))
	}
	r := res[i]
	if i == len(res)-1 {
		delete(eqMap, uid)
	}
	res = nil

	return r
}

func main() {}
