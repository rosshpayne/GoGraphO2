package cache

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/db"
	"github.com/DynamoGraph/ds"
	param "github.com/DynamoGraph/dygparam"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

func syslog(s string) {
	slog.Log("Cache: ", s)
}

// errors
var ErrCacheEmpty = errors.New("Cache is empty")

//  ItemCache struct is the transition between Dynamodb types and the actual attribute type defined in the DD.
//  Number (dynamodb type) -> float64 (transition) -> int (internal app & as defined in DD)
//  process: dynamodb -> ItemCache -> DD conversion if necessary to application variables -> ItemCache -> Dynamodb
//	types that require conversion from ItemCache to internal are:
//   DD:   int         conversion: float64 -> int
//   DD:   datetime    conversion: string -> time.Time
//  all the other datatypes do not need to be converted.

type SortKey = string

// type NodeCache struct {
// 	m map[SortKey]*blk.DataItem
// 	sync.Mutex
// }

//type block map[SortKey]*blk.DataItem

// data associated with a single node
type NodeCache struct {
	sync.RWMutex // used for querying the cache data items
	m            map[SortKey]*blk.DataItem
	Uid          util.UID
	ffuEnabled   bool // true for fetch-for-update operations
	locked       bool
	gc           *GraphCache // point back to graph-cache
}

type entry struct {
	ready chan struct{} // a channel for each entry - to synchronise access when the data is being sourced
	*NodeCache
}
type Rentry struct {
	ready chan struct{} // a channel for each entry - to synchronise access when the data is being sourced
	sync.RWMutex
}

// graph cache consisting of all nodes loaded into memory
type GraphCache struct {
	sync.RWMutex
	cache  map[util.UIDb64s]*entry
	rsync  sync.RWMutex
	cacheR map[util.UIDb64s]*Rentry // not used?
}

var GraphC GraphCache

func NewCache() *GraphCache {
	return &GraphC
}

func init() {
	GraphC = GraphCache{cache: make(map[util.UIDb64s]*entry)}
}

// func (g gsiResult) String() string {
// 	return string(g.PKey)
// }

func (g *GraphCache) IsCached(uid util.UID) (ok bool) {
	g.Lock()
	_, ok = g.cache[uid.String()]
	g.Unlock()
	return ok
}

func (np *NodeCache) GetOvflUIDs(sortk string) []util.UID {
	// TODO: replace A#G#:S" with generic term
	// get np.uidPreds
	if di, ok := np.m[sortk]; ok { // np.GetDataItem("A#G#:S"); ok {
		_, _, ofUID := di.GetNd()
		ids := len(ofUID)
		if ids > 0 {
			m := make([]util.UID, ids, ids)
			for i, v := range ofUID {
				m[i] = util.UID(v)
			}
			return m
		}
	}
	return nil
}

func (n *NodeCache) GetDataItem(sortk string) (*blk.DataItem, bool) {
	if x, ok := n.m[sortk]; ok {
		return x, ok
	}
	return nil, false
}

// // UpdatePropagationBlock func associated with Event processing
// func UpdatePropagationBlock(sortK string, pUID, cUID, targetUID util.UID, state byte) error {
// 	gc := NewCache()
// 	nd, err := gc.FetchForUpdate(pUID)
// 	if err != nil {

// 	}
// 	err = nd.UpdatePropagationBlock(sortK, pUID, cUID, targetUID, state)
// 	nd.Unlock()
// 	return err

// }

// SetUpredAvailable called from client as part of AttachNode operation SetUpredState
// targetUID is the propagation block that contains the child scalar data.
// id - overflow block id
// cnt - increment counter by 0 (if errored) or 1 (if node attachment successful)
func (nc *NodeCache) SetUpredAvailable(sortK string, pUID, cUID, targetUID util.UID, id int, cnt int) error {
	var (
		attachAttrNm string
		found        bool
		err          error
	)
	syslog(fmt.Sprintf("In SetUpredAvailable: pUid, tUID:  %s %s %s", pUID, targetUID, sortK))
	//
	// TyAttrC populated in NodeAttach(). Get Name of attribute that is the attachment point, based on sortk
	//
	i := strings.IndexByte(sortK, ':')
	for _, v := range TyAttrC {
		if v.C == sortK[i+1:] {
			attachAttrNm = v.Name
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf(fmt.Sprintf("Error in SetUpredAvailable. Attach point attribute not found in type map for sortk %q", sortK))
	}
	//
	// cache: update pUID block with latest propagation state; targetUID, XF state
	//
	di := nc.m[sortK]

	found = false
	// if target UID is the parent node UID
	if bytes.Equal(pUID, targetUID) {
		// target is current parent UID block
		// search from most recent (end of slice)
		for i := len(di.Nd); i > 0; i-- {
			if bytes.Equal(di.Nd[i-1], cUID) {
				di.XF[i-1] = blk.ChildUID
				err = db.SaveUpredState(di, cUID, blk.ChildUID, i-1, cnt, attachAttrNm)
				if err != nil {
					return err
				}
				found = true
				break
			}
		}
	} else {
		// target is an Overflow block
		// search from most recent (end of slice)
		for i := len(di.Nd); i > 0; i-- {
			if bytes.Equal(di.Nd[i-1], targetUID) {
				di.XF[i-1] = blk.OvflBlockUID
				di.Id[i-1] = id
				err = db.SaveUpredState(di, targetUID, blk.OvflBlockUID, i-1, cnt, attachAttrNm)
				if err != nil {
					return err
				}
				found = true
				break
			}
		}
	}
	if !found {
		return fmt.Errorf("AttachNode: target UID not found in Nd attribute of parent node")
	}

	return nil
}

var NoNodeTypeDefinedErr error = errors.New("No type defined in node data")

type NoTypeDefined struct {
	ty string
}

func (e NoTypeDefined) Error() string {
	return fmt.Sprintf("Type %q not defined", e.ty)
}

func NewNoTypeDefined(ty string) error {
	return NoTypeDefined{ty: ty}
}

// UnmarshalCache, unmarshalls the nodecache into the ds.ClientNV data structure
// currently it presumes all propagated scalar data must be prefixed with A#.
// TODO: extend to include G# prefix.
// nc must have been acquired using either API
// * FetchForUpdaate(uid)
// * FetchNode(uid)
func (nc *NodeCache) UnmarshalCache(nv ds.ClientNV) error {
	if nc == nil {
		return ErrCacheEmpty
	}
	var (
		ty     string
		attrDT string
		ok     bool
		err    error
	)
	// for k := range nc.m {
	// 	fmt.Println(" key: ", k)
	// }
	if ty, ok = nc.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err = FetchType(ty); err != nil {
		return err
	}

	genAttrKey := func(attr string) (string, error) {
		var (
			searchKey string
			pd        strings.Builder
			aty       blk.TyAttrD
		)
		attr_ := strings.Split(attr, ":")

		if len(attr_) > 1 {
			//
			// child attribute
			//
			searchKey = "A#G"
			for i := 0; i < len(attr_); i++ {
				// check type exists
				x := TyAttrC[ty+":"+attr_[0]]
				if _, ok := TyC[x.Ty]; !ok {
					return "", fmt.Errorf("Type %q not defined", x.Ty)
				}
				if aty, ok = TyAttrC[x.Ty+":"+attr_[i]]; !ok {
					return "", fmt.Errorf("Attribute %q does not belong to type %q", attr_[i], x.Ty)
				}
				if i == 0 {
					//	pd.WriteString(aty.P) // item partition
					pd.WriteString(searchKey)
				}
				pd.WriteString("#:")
				pd.WriteString(aty.C) // attribute compressed identifier
			}
			if aty.DT != "Nd" {
				attrDT = "UL" + aty.DT
			}

		} else {
			//
			// node only attribute
			//
			pd.WriteString("A#")
			if aty, ok = TyAttrC[ty+":"+attr]; !ok {
				return "", fmt.Errorf("Attribute %q does not belong to type %q", attr, ty)
			}
			if aty.DT == "Nd" {
				pd.WriteString("G#:")
			} else {
				pd.WriteString(":")
			}
			pd.WriteString(aty.C) // attribute compressed identifier
			attrDT = aty.DT
		}
		// build a item clause

		return pd.String(), nil
	}
	// This data is stored in uid-pred UID item that needs to be assigned to each child data item
	var State [][]int
	var OfUIDs [][]byte

	sortK := func(key string, i int) string {
		var s strings.Builder
		s.WriteString(key)
		s.WriteByte('#')
		s.WriteString(strconv.Itoa(i))
		return s.String()
	}

	for _, a := range nv { // a.Name = "Age"
		//
		// field name repesents a scalar. It has a type that we use to generate a sortk (predicate) G#:<uid-pred>#:<pred-scalar-type-abreviation>
		//
		attrKey, err := genAttrKey(a.Name)
		if err != nil {
			return err
		}
		//
		// grab the *blk.DataItem from the cache for the nominated sortk.
		// we could query the child node to get this data or query the #G data which is its copy of the data
		//
		if v, ok := nc.m[attrKey]; ok {
			// based on data type and whether its a node or uid-pred predicate
			switch attrDT {

			// Scalars
			case "I": // int
				a.Value = v.GetI()
			case "F": // float
				a.Value = v.GetF()
			case "S": // string
				a.Value = v.GetS()
			case "Bl": // bool
				a.Value = v.GetBl()
			case "DT": // DateTime - stored as string
				a.Value = v.GetDT()

			// Sets
			case "IS": // set int
				a.Value = v.GetIS()
			case "FS": // set float
				a.Value = v.GetFS()
			case "SS": // set string
				a.Value = v.GetSS()
			case "BS": // set binary
				a.Value = v.GetBS()

			// Lists
			case "LS": // list string
				a.Value = v.GetLS()
			case "LF": // list float
				a.Value = v.GetLF()
			case "LI": // list int
				a.Value = v.GetLI()
			case "LB": // List []byte
				a.Value = v.GetLB()
			case "LBl": // List bool
				a.Value = v.GetLBl()

			// Propagated Scalars
			case "ULS": // list string
				//a.Value = v.GetLBl()
				var allLS [][]string
				var allXbl [][]bool
				// data from root uid-pred block
				ls, xf := v.GetULS()

				allLS = append(allLS, ls)
				allXbl = append(allXbl, xf)
				// data from overflow blocks

				for _, v := range OfUIDs {
					nuid, err := nc.gc.FetchNode(util.UID(v))
					if err != nil {
						return err
					}
					// iterate over all overflow items in the overflow block for key attrKey
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							ls, xbl := di.GetULS()
							allLS = append(allLS, ls[1:])
							allXbl = append(allXbl, xbl[1:])
						}
					}
				}
				a.Value = allLS
				a.Null = allXbl
				a.State = State
				a.OfUIDs = OfUIDs

			case "ULF": // list float
				//a.Value = v.GetLBl()
				var allLF [][]float64
				var allXbl [][]bool
				// data from root uid-pred block
				lf, xf := v.GetULF()

				allLF = append(allLF, lf)
				allXbl = append(allXbl, xf)
				// data from overflow blocks
				for _, v := range OfUIDs {
					nuid, err := nc.gc.FetchNode(util.UID(v))
					if err != nil {
						return err
					}
					// iterate over all overflow items in the overflow block for key attrKey
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							lf, xbl := di.GetULF()
							allLF = append(allLF, lf[1:])
							allXbl = append(allXbl, xbl[1:])
						}
					}
				}
				a.Value = allLF
				a.Null = allXbl
				a.State = State
				a.OfUIDs = OfUIDs

			case "ULI": // list int

				var allLI [][]int64
				var allXbl [][]bool
				// data from root uid-pred block
				li, xf := v.GetULI()

				allLI = append(allLI, li)
				allXbl = append(allXbl, xf)
				// data from overflow blocks
				for _, v := range OfUIDs {
					nuid, err := nc.gc.FetchNode(util.UID(v))
					if err != nil {
						return err
					}
					// iterate over all overflow items in the overflow block for key attrKey
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							li, xbl := di.GetULI()
							allLI = append(allLI, li[1:])
							allXbl = append(allXbl, xbl[1:])
						}
					}
				}
				a.Value = allLI
				a.Null = allXbl
				a.State = State
				a.OfUIDs = OfUIDs

			case "ULB": // List []byte

				var allLB [][][]byte
				var allXbl [][]bool
				// data from root uid-pred block
				lb, xf := v.GetULB()

				allLB = append(allLB, lb)
				allXbl = append(allXbl, xf)
				// data from overflow blocks
				for _, v := range OfUIDs {
					nuid, err := nc.gc.FetchNode(util.UID(v))
					if err != nil {
						return err
					}
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							lb, xbl := di.GetULB()
							allLB = append(allLB, lb[1:])
							allXbl = append(allXbl, xbl[1:])
						}
					}
				}
				a.Value = allLB
				a.Null = allXbl
				a.State = State
				a.OfUIDs = OfUIDs

			case "ULBl": // List bool
				//a.Value = v.GetLBl()
				var allLBl [][]bool
				var allXbl [][]bool
				// data from root uid-pred block
				bl, xf := v.GetULBl()

				allLBl = append(allLBl, bl)
				allXbl = append(allXbl, xf)
				// data from overflow blocks
				for _, v := range OfUIDs {
					nuid, err := nc.gc.FetchNode(util.UID(v))
					if err != nil {
						return err
					}
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							bl, xbl := di.GetULBl()
							allLBl = append(allLBl, bl[1:])
							allXbl = append(allXbl, xbl[1:])
						}
					}
				}
				a.Value = allLBl
				a.Null = allXbl
				a.State = State
				a.OfUIDs = OfUIDs

			case "Nd": // uid-pred cUID+XF data
				var allcuid [][][]byte
				var xfall [][]int

				cuid, xf, ofuids := v.GetNd()
				// share ofuids amoungst all propgatated data types
				OfUIDs = ofuids

				allcuid = append(allcuid, cuid)
				xfall = append(xfall, xf)

				// data from overflow blocks
				for _, v := range OfUIDs {

					nuid, err := nc.gc.FetchNode(util.UID(v))
					if err != nil {
						return err
					}
					// for k := range nuid.m {
					// 	fmt.Println("Overflow keys : ", k)
					// }
					for i := 1; true; i++ {
						if di, ok := nuid.m[sortK(attrKey, i)]; !ok {
							break //return fmt.Errorf("UnmarshalCache: SortK %q does not exist in cache", attrKey)
						} else {
							uof, xof := di.GetOfNd()
							// check if target item is populated. Note: #G#:S#1 will always contain atleast one cUID but #G#:S#2 may not contain any.
							// this occurs as UID item target is created as item id is incremented but associated scalar data target items are created on demand.
							// so a UID target item may exist without any associated scalar data targets. Each scalar data target items will always contain data associated with each cUID attached to parent.
							if len(uof) > 0 {
								allcuid = append(allcuid, uof) // ignore first entry
								xfall = append(xfall, xof)     // ignore first entry
							}
						}
					}
				}

				a.Value = allcuid
				a.State = xfall
				// share state amongst all propgated datat type
				State = xfall

			default:
				panic(fmt.Errorf("Unsupported data type %q", attrDT))
			}
		}
	}

	return nil

}

func (d *NodeCache) UnmarshalValue(attr string, i interface{}) error {
	if d == nil {
		return ErrCacheEmpty
	}
	var (
		aty blk.TyAttrD
		ty  string
		ok  bool
	)
	defer d.Unlock()

	if reflect.ValueOf(i).Kind() != reflect.Ptr {
		panic(fmt.Errorf("passed in value must be a pointer"))
	}

	if ty, ok = d.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err := FetchType(ty); err != nil {
		return err
	}

	if aty, ok = TyAttrC[ty+":"+attr]; !ok {
		panic(fmt.Errorf("Attribute %q not found in type %q", attr, ty))
	}
	// build a item clause
	var pd strings.Builder
	// pd.WriteString(aty.P) // item partition
	// pd.WriteByte('#')
	pd.WriteString("A#:") // scalar data
	pd.WriteString(aty.C) // attribute compressed identifier

	for _, v := range d.m {
		// match attribute descriptor
		if v.SortK == pd.String() {
			// we now know the attribute data type, populate interface value with attribute data
			switch aty.DT {
			case "I":
				if reflect.ValueOf(i).Elem().Kind() != reflect.Int {
					return fmt.Errorf("Input type does not match data type")
				}
				reflect.ValueOf(i).Elem().SetInt(v.GetI())
				//
				// non-reflect version below - does not work as fails to set i to value
				// must return i to work. So reflect is more elegant solution as it does an inplace set.
				// if _,ok := i.(*int); !ok {
				// 	return fmt.Errorf("Input type does not match data type")
				// } // or
				// switch i.(type) {
				// case *int, *int64:
				// default:
				// 	return fmt.Errorf("Input type does not match data type")
				// }
				// ii := v.GetI()
				// fmt.Println("Age: ", ii)
				// i = &ii
				return nil
			default:
				return fmt.Errorf("Input type does not match data type")
			}

		}
	}
	return fmt.Errorf("%s not found in data", attr)

}

// UnmarshalMap is an exmaple of reflect usage. Not used in main program.
func (d *NodeCache) UnmarshalMap(i interface{}) error {
	if d == nil {
		return ErrCacheEmpty
	}
	defer d.Unlock()

	if !(reflect.ValueOf(i).Kind() == reflect.Ptr && reflect.ValueOf(i).Elem().Kind() == reflect.Struct) {
		return fmt.Errorf("passed in value must be a pointer to struct")
	}
	var (
		ty string
		ok bool
	)
	if ty, ok = d.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err := FetchType(ty); err != nil {
		return err
	}

	if ty, ok = d.GetType(); !ok {
		return NoNodeTypeDefinedErr
	}
	if _, err := FetchType(ty); err != nil {
		return err
	}

	var (
		aty blk.TyAttrD
	)

	genAttrKey := func(attr string) string {
		if aty, ok = TyAttrC[ty+":"+attr]; !ok {
			return ""
		}
		// build a item clause
		var pd strings.Builder
		//pd.WriteString(aty.P) // item partition
		pd.WriteString("A#:")
		pd.WriteString(aty.C) // attribute compressed identifier
		return pd.String()
	}

	typeOf := reflect.TypeOf(i).Elem()
	valueOf := reflect.ValueOf(i).Elem()
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		valueField := valueOf.Field(i)
		// field name should match an attribute identifer
		attrKey := genAttrKey(field.Name)
		if attrKey == "" {
			continue
		}
		for _, v := range d.m {
			// match attribute descriptor
			if v.SortK == attrKey {
				//fmt.Printf("v = %#v\n", v.SortK)
				// we now know the attribute data type, populate interface value with attribute data
				switch x := aty.DT; x {
				case "I": // int
					valueField.SetInt(v.GetI())
				case "F": // float
					valueField.SetFloat(v.GetF())
				case "S": // string
					valueField.SetString(v.GetS())
				case "Bl": // bool
					valueField.SetBool(v.GetBl())
				// case "DT": // bool
				// 	valueField.SetString(v.GetDT())
				// case "B": // binary []byte
				// 	valueField.SetBool(v.GetB())
				case "LS": // list string
					valueOf_ := reflect.ValueOf(v.GetLS())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LF": // list float
					valueOf_ := reflect.ValueOf(v.GetLF())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LI": // list int
					valueOf_ := reflect.ValueOf(v.GetLI())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LB": // List []byte
					valueOf_ := reflect.ValueOf(v.GetLB())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "LBl": // List bool
					valueOf_ := reflect.ValueOf(v.GetLB())
					newSlice := reflect.MakeSlice(field.Type, 0, 0)
					valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				// case "Nd": // List []byte
				// 	valueOf_ := reflect.ValueOf(v.GetNd())
				// 	fmt.Println("In Nd: Kind(): ", valueOf_.Kind(), valueOf_.Type().Elem(), valueOf_.Len()) //  slice string 4
				// 	newSlice := reflect.MakeSlice(field.Type, 0, 0)
				// 	valueField.Set(reflect.AppendSlice(newSlice, valueOf_))
				case "IS": // set int
				case "FS": // set float
				case "SS": // set string
				case "BS": // set binary
				default:
					panic(fmt.Errorf("Unsupported data type %q", x))
				}

			}
		}
	}
	return nil

}

func (d *NodeCache) GetType() (tyN string, ok bool) { // TODO: source type from GSI after Ty attribute is added ???
	var di *blk.DataItem

	syslog(fmt.Sprintf("d.m: %#v\n", d.m))
	if di, ok = d.m["A#T"]; !ok {
		syslog("in GetType: no A#T entry in NodeCache")
		return "", ok
	}
	ty, _ := db.GetTyLongNm(di.GetTy())
	return ty, true
}

// SetOvflBlkFull sets in cache and database the state of the overflow block UID
// in the node's particular uid-predicate (sortk) to FULL. It will nolonger be
// available accept data propagation
// the node is locked in the calling (client) pkg.
func (pn *NodeCache) SetOvflBlkFull(ovflBlkUID util.UID, sortk string) error {
	var cIdx int

	di := pn.m[sortk]

	for i, v := range di.Nd {
		if bytes.Equal(ovflBlkUID, v) {
			// set flag to full
			di.XF[i] = blk.OvflItemFull
			cIdx = i
			break
		}
	}
	//
	// preserve cache change to db
	//
	err := db.SaveOvflBlkFull(di, cIdx)
	if err != nil {
		return err
	}

	return nil
}

// ConfigureUpred returns either the parent block or an overflow block as the target for the propagation of child scalar data
// in the case of overflow block it may require more overflow blocks to be created if less than a certain number are available.
// ConfigureUpred makes changes to the node cache (i.e. adds overflows in the cache first). The last operation ofConfigureUpred
// writes the uid-pred portion of the nocde cache to the database
func (pn *NodeCache) ConfigureUpred(sortK string, pUID, cUID util.UID, rsvCnt ...int) (util.UID, int, error) {
	var (
		ok          bool
		embedded    int
		ovflBlocks  int
		id          int
		di          *blk.DataItem // existing item
		availOfUids []util.UID    // available overflow uids
		newOfUID    []util.UID
		tUID        util.UID // chosen overflow UID to use
		itemId      []int
	)
	//
	// check if its a recursive call and if greater than 1 recursive call abort
	//
	syslog(fmt.Sprintf("ConfigureUpred:  pUID,cUID,sortK : %s   %s   %s", pUID.String(), cUID.String(), sortK))
	switch {
	case len(rsvCnt) == 0:
		rsvCnt = append(rsvCnt, 1)
	default:
		rsvCnt[0] += 1
	}
	if rsvCnt[0] > param.MaxOvFlBlocks {
		return nil, 0, fmt.Errorf(fmt.Sprintf("Abort: Recursive calls to ConfigureUpred exceeeds %d", param.MaxOvFlBlocks))
	}
	//
	// exclusive parent node lock has been applied in calling routine
	//
	if di, ok = pn.m[sortK]; !ok {
		// no uid-pred exists - create an empty one
		di = new(blk.DataItem)
		pn.m[sortK] = di
		//	return nil, 0, fmt.Errorf("GetTargetBlock: Target attribute %q does not exit", sortK)
	}
	// XF is a LN (list of Number) populated in uid-predicate. Flag value 1 : c-UID, 2 : inUse 3 : c-UID is soft deleted (detached), 4 : overflow UID, 5 : overflow Blk InUse  6 : Ovflw item FUll (not applicable anymore)
	// Get the current distribution of propagation data either embedded in parent node or in overflow blocks (UIDs)
	for i, v := range di.XF {
		switch {
		case v <= blk.UIDdetached:
			// includes attached cUIDs, inUse cUIDs &  attached cUIDs
			embedded++
		case v == blk.OvflBlockUID || v == blk.OvflItemFull || v == blk.OuidInuse:
			// count overflow UIDs in any state
			ovflBlocks++
			if v == blk.OvflBlockUID { // || v == blk.OvflItemFull {
				// available overflow uids only
				availOfUids = append(availOfUids, di.Nd[i])
				itemId = append(itemId, di.Id[i])
			}
		}
	}
	// for _, v := range availOfUids {
	// 	fmt.Printf("Available OfUID: %s\n", util.UID(v).String())
	// }
	//
	if embedded < param.EmbeddedChildNodes && ovflBlocks == 0 {
		//
		// append  cUID  to Nd, XF (not using overflow yet)
		//
		di.Nd = append(di.Nd, cUID)
		di.XF = append(di.XF, blk.CuidInuse)
		di.Id = append(di.Id, 0)

		err := db.SaveCompleteUpred(di)
		if err != nil {
			return nil, 0, fmt.Errorf("SaveCompleteUpred: %s", err)
		}
		return pUID, 0, nil // attachment point is the parent UID
	}
	//
	if len(availOfUids) <= param.OvFlBlocksGrowBy && ovflBlocks < param.MaxOvFlBlocks {
		//
		// create a Overflow UID and subsequent block
		//
		ouid, err := util.MakeUID()
		if err != nil {
			return nil, 0, fmt.Errorf("Failed to make UID: %s", err.Error())
		}
		//
		// add new Overlfow block - append to exising Nd, Xf, Id. These have their equivalents in Dynamo as List types.
		//
		di.Nd = append(di.Nd, ouid)
		di.XF = append(di.XF, blk.OvflBlockUID)
		di.Id = append(di.Id, 1)
		newOfUID = append(newOfUID, ouid)
		availOfUids = append(availOfUids, ouid)
		itemId = append(itemId, 1)
		//
		// update database with new overflow UIDs and XF state
		//
		err = db.SaveCompleteUpred(di)
		if err != nil {
			return nil, 0, fmt.Errorf("SaveCompleteUpred: %s", err)
		}
		//
		// create associated overflow blocks
		//
		err = db.MakeOvflBlocks(di, newOfUID, 1)
		if err != nil {
			return nil, 0, fmt.Errorf("MakeOvflBlocks: %s", err)
		}
	}
	//
	// keep adding overflow blocks until max limit reached then go back and populate into existing overflow blocks
	// incrementing the item count. An overflow block has 1:n items. Each item contains upto param.UIDsPerOflwItem.
	// When the item limit is exceeded (ie. cUIDs in an overflow item) and MaxOvFlBlocks is exceeded we keep creating
	// new items in the existing overflow blocks
	//

	if ovflBlocks == param.MaxOvFlBlocks && len(availOfUids) == 0 {
		// only mode now is appending to existing overflow blocks creating new overflow items as item size limit exceeded.
		for i, v := range di.XF {
			if v == blk.OvflItemFull {
				di.XF[i] = blk.OvflBlockUID
				di.Id[i] += 1
				db.AddUIDPropagationTarget(di.Nd[i], sortK, di.Id[i])
			}
		}
		// try again
		return pn.ConfigureUpred(sortK, pUID, cUID, rsvCnt[0])
	}
	//
	// choose an overflow to use - must be available
	//
	tUID = availOfUids[len(availOfUids)-1]
	// get current Id for tUID
	for i, v := range di.Nd {
		if bytes.Equal(v, tUID) {
			id = di.Id[i]
			break
		}
	}
	//
	// mark cache entry as InUse
	//
	var idx int
	for i, v := range di.Nd {
		if bytes.Equal(v, tUID) {
			di.XF[i] = blk.OuidInuse
			idx = i
			break
		}
	}
	//
	// generate sortk with item id
	//
	sortk := sortK + "#" + strconv.Itoa(id)
	//
	// append child UID to Nd and XF in chosen Overflow Block. Child data will be
	// taken care of in PropagateChildData()
	//
	err := db.SaveChildUIDtoOvflBlock(cUID, tUID, sortk, id)
	if err != nil {
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			di.XF[idx] = blk.OvflItemFull
			return pn.ConfigureUpred(sortK, pUID, cUID, rsvCnt[0])
		}
		return nil, 0, err

	}
	//
	// preserve all cache changes to db
	//
	db.SaveCompleteUpred(di)

	return tUID, id, nil

}

// ************************************ Type caches ********************************************

type Ty = string      // type
type Ty_Attr = string // type:attr

//type FacetIdent string // type:attr:facet
//
// Derived Type Attributes cache
//
type TyCache map[Ty]blk.TyAttrBlock

var TyC TyCache

//
// caches for type-attribute and type-attribute-facet
//
type TyAttrCache map[Ty_Attr]blk.TyAttrD // map[Ty_Attr]blk.TyItem

var TyAttrC TyAttrCache

// type FacetAttr struct {
// 	Attr  string
// 	Facet string
// 	Ty  string
// 	Abrev string
// }
// type expression struct {
// 	arg []Arguments
// 	expr
// }

// type Attribute struct {
// 	alias  string
// 	name   string
// 	args   []Arguments
// 	facets []Facet
// 	filter []Filter
// 	attrs  []attribute
// }

// func (u *UIDs) Attr() {}

// type query struct {
// 	alias
// 	name    string
// 	var_    string
// 	f       string
// 	cascade bool
// 	filter  []Filter
// 	attr    []attribute
// 	args    []Arguments
// }

type FacetTy struct {
	Name string
	DT   string
	C    string
}

type FacetCache map[Ty_Attr][]FacetTy

var FacetC FacetCache

func init() {
	//
	// cache holding the attributes belonging to a type
	///
	TyC = make(TyCache)
	//
	// DataTy caches for type-attribute and type-attribute-facet
	//
	TyAttrC = make(TyAttrCache)
	FacetC = make(map[Ty_Attr][]FacetTy)
}

// func LoadDataDictionary(ty Ty) {
// 	// use dedicated table (DyGTy) to store types
// 	_, err := db.LoadDataDictionary()
// 	if err != nil {
// 		panic(err)
// 	}
// }

// FetchType returns a slice of attributes belonging to the type and populates two pkg map variables  TyC, TyAttrC
// TODO: make FetchType a method of TyC (?). What are the advantages if its a method? Cannot think of any.
func FetchType(ty Ty) (blk.TyAttrBlock, error) {
	// type Ty = string      // type
	// type Ty_Attr = string // type:attr
	// cache type :    type TyCache map[Ty]blk.TyAttrBlock
	// cache  vaiable: var TyC TyCache
	//
	// check Type Cache (Map) (TyC): key: Type Name
	if _, ok := db.GetTyShortNm(ty); !ok {
		if longTy, ok := db.GetTyLongNm(ty); !ok {
			return nil, fmt.Errorf("FetchType: error %q type not found", ty)
		} else {
			ty = longTy
		}
	}
	if ty, ok := TyC[ty]; ok { // ty= Person
		return ty, nil
	}
	//
	// not found in cache, load from db
	//
	tyIBlk, err := db.FetchType(ty)
	if err != nil {
		return nil, err
	}
	//
	// reorg type data into two caches (vars in this pkg): TyC, TyAttrC
	// with  respective keys: typeName, type-Attribute
	//
	var (
		tc blk.TyAttrBlock
		s  strings.Builder
		a  blk.TyAttrD
	)
	for _, v := range tyIBlk { // database item

		genT_Attr := func(attr string) Ty_Attr {
			// generte key for TyAttrC:  <typeName>:<attrName> e.g. Person:Age
			s.Reset()
			s.WriteString(ty)
			s.WriteByte(':')
			s.WriteString(attr)
			return s.String()
		}
		// checl of DT is a UID attribute and gets its base type
		//	fmt.Printf("DT:%#v \n", v)
		if len(v.Ty) == 0 {
			panic(fmt.Errorf("DT not defined for %#v", v))
		}
		//
		// scalar type or abstract type e.g [person]
		//
		if v.Ty[0] == '[' {
			a = blk.TyAttrD{Name: v.Atr, DT: "Nd", C: v.C, Ty: v.Ty[1 : len(v.Ty)-1], P: v.P, Pg: v.Pg, IncP: v.IncP}
		} else {
			a = blk.TyAttrD{Name: v.Atr, DT: v.Ty, C: v.C, P: v.P, N: v.N, Pg: v.Pg, IncP: v.IncP}
		}
		tc = append(tc, a)
		//
		TyAttrC[genT_Attr(v.Atr)] = a

		// fc, _ := FacetCache[tyAttr]
		// for _, vf := range v.F {
		// 	vfs := strings.Split(vf, "#")
		// 	if len(vfs) == 3 {
		// 		f := FacetTy{Name: vfs[0], DT: vfs[1], C: vfs[2]}
		// 		fc = append(fc, f)
		// 	} else {
		// 		panic(fmt.Errorf("%s", "Facet type information must contain 3 elements: <facetName>#<datatype>#<compressedIdentifer>"))
		// 	}
		// }
		// FacetCache[tyAttr] = fc
	}
	//
	TyC[ty] = tc

	return tc, nil
}

func AddReverseEdge(cuid, puid []byte, pTy string, sortk string) error {
	return nil
}
