package db

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/rdf/ds"
	"github.com/DynamoGraph/rdf/grmgr"
	"github.com/DynamoGraph/rdf/uuid"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"

	"github.com/DynamoGraph/rdf.m/reader"
	"github.com/DynamoGraph/rdf.m/result"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	//"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func SaveRDFNode(nv_ []ds.NV, wg *sync.WaitGroup, lmtr grmgr.Limiter) (err error) {

	defer wg.Done()
	defer func() func() {
		return func() {
			err := err
			if err != nil {
				slog.Log("SaveRDFNode: ", fmt.Sprintf("Returned.++++++++++++ [%s]", err.Error()))
			} else {
				slog.Log("SaveRDFNode: ", fmt.Sprintf("Returned.+++++++++++ No error"))
			}
		}
	}()()

	lmtr.StartR()
	defer lmtr.EndR()

	var (
		//expr expression.Expression
		av map[string]*dynamodb.AttributeValue
		//xx      map[string]*dynamodb.AttributeValue
		//values map[string]*dynamodb.AttributeValue
	)

	convertSet2list := func(av map[string]*dynamodb.AttributeValue) {
		for k, v := range av {
			switch k {
			case "Nd":
				if len(v.BS) > 0 {
					v.L = make([]*dynamodb.AttributeValue, len(v.BS), len(v.BS))
					for i, u := range v.BS {
						v.L[i] = &dynamodb.AttributeValue{B: u}
					}
					v.BS = nil
				}
			case "SS":
				// by default dynamodb uses AV List for "[]string" Go type. Below converts List to Set
				if len(v.L) > 0 {
					v.SS = make([]*string, len(v.L), len(v.L))
					for i, u := range v.L {
						v.SS[i] = u.S
					}
					v.L = nil
				}
			}
		}
	}

	for _, nv := range nv_ {
		slog.Log("SaveRDFNode: xxx ", fmt.Sprintf("+++++++++++++++++ SaveRDFNode:%#v\n ", nv))
	}
	//	slog.Log("SaveRDFNode: ", fmt.Sprintf("SaveRDFNode : %d ", len(nv_))) //, nv_[0]))

	localCh := make(chan util.UID)

	request := uuid.Request{SName: nv_[0].SName, RespCh: localCh}
	//	slog.Log("SaveRDFNode: ", fmt.Sprintf("UID request  : %#v", request))

	uuid.ReqCh <- request

	//	slog.Log("SaveRDFNode: ", "Waiting for UID ......................")

	UID := <-localCh

	//	slog.Log("SaveRDFNode: ", fmt.Sprintf("UID received  : %s", UID.String()))

	for _, nv := range nv_ {

		// append child attr value to parent uid-pred list
		switch nv.DT {

		case "I":

			type Item struct {
				PKey  []byte
				SortK string
				N     int
				P     string
				Ty    string // node type
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if i, ok := nv.Value.(int); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, N: i, P: nv.Name, Ty: nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "F":

			type Item struct {
				PKey  []byte
				SortK string
				N     string // float kept in program as string - this is a trial to see if keeping as string works.
				P     string
				Ty    string // node type
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if f, ok := nv.Value.(string); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, N: f, P: nv.Name, Ty: nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "S":

			type Item struct {
				PKey  []byte
				SortK string
				S     string
				P     string
				Ty    string // node type
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if f, ok := nv.Value.(string); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, S: f, P: nv.Name, Ty: nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "DT": // DateTime

			type Item struct {
				PKey  []byte
				SortK string
				DT    string
				P     string
				Ty    string // node type
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if dt, ok := nv.Value.(time.Time); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, DT: dt.String(), P: nv.Name, Ty: nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an String ")
			}

		case "ty": // node DyG type ??? more explanation

			type Item struct {
				PKey  []byte
				SortK string
				S     string
				P     string
				Ty    string // node type
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if s, ok := nv.Value.(string); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: "A#T", S: s, P: s}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "Bl":

			type Item struct {
				PKey  []byte
				SortK string
				Bl    bool
				P     string
				Ty    string // node type
			}
			if f, ok := nv.Value.(bool); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, Bl: f, P: nv.Name, Ty: nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "B":

			type Item struct {
				PKey  []byte
				SortK string
				B     []byte
				P     string
				Ty    string // node type
			}
			if f, ok := nv.Value.([]byte); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, B: f, P: nv.Name, Ty: nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "LI":

			type Item struct {
				PKey  []byte
				SortK string
				LN    []int64
			}
			if f, ok := nv.Value.([]int64); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, LN: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "LF":

			type Item struct {
				PKey  []byte
				SortK string
				LN    []float64
			}
			if f, ok := nv.Value.([]float64); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, LN: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "SI":

			type Item struct {
				PKey  []byte
				SortK string
				SN    []int64
			}
			if f, ok := nv.Value.([]int64); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, SN: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not a slice of int64")
			}

		case "SF":

			type Item struct {
				PKey  []byte
				SortK string
				SN    []float64
			}
			if f, ok := nv.Value.([]float64); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, SN: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an slice float64 ")
			}

		case "SBl":

			type Item struct {
				PKey  []byte
				SortK string
				SBl   []bool
			}
			if f, ok := nv.Value.([]bool); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, SBl: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an slice of bool ")
			}

		case "SS":

			type Item struct {
				PKey  []byte
				SortK string
				SS    []string // Dynamo will use AV List type - will convert to SS in convertSet2list()
			}
			if f, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, SS: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an String Set ")
			}

		case "SB":

			type Item struct {
				PKey  []byte
				SortK string
				SB    [][]byte
			}
			if f, ok := nv.Value.([][]byte); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, SB: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an Int ")
			}

		case "Nd":

			type Item struct {
				PKey  []byte
				SortK string
				Nd    [][]byte
				XF    []int
				Id    []int
			}
			// convert node blank name to UID
			xf := make([]int, 1, 1)
			xf[0] = blk.ChildUID
			id := make([]int, 1, 1)
			id[0] = 0
			if f, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				uid := make([][]byte, len(f), len(f))
				xf := make([]int, len(f), len(f))
				id := make([]int, len(f), len(f))
				for i, n := range f {
					request := uuid.Request{SName: n, RespCh: localCh}
					slog.Log("SaveRDFNode: ", fmt.Sprintf("UID Nd request  : %#v", request))

					uuid.ReqCh <- request

					UID := <-localCh

					slog.Log("SaveRDFNode: ", fmt.Sprintf("Received UID: %s\n", UID.String()))

					uid[i] = []byte(UID)
					xf[i] = blk.ChildUID
					id[i] = 0

				}
				a := Item{PKey: UID, SortK: nv.Sortk, Nd: uid, XF: xf, Id: id}
				//e:= uuid.Edges{	PKey  : UID, SortK: nv.SortK, Nd : uid}
				//execute AttachNode based on data in a
				//uuid.EdgesCh <- uuid.Edges(a)
				slog.Log("SaveRDFNode: ", fmt.Sprintf("a: = %#v", a))
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				return fmt.Errorf(" Value is not an string slice ")
			}
		}
		convertSet2list(av)
		//
		// PutItem
		//
		{
			t0 := time.Now()
			ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
				TableName:              aws.String("DyGraph"),
				Item:                   av,
				ReturnConsumedCapacity: aws.String("TOTAL"),
			})
			t1 := time.Now()
			syslog(fmt.Sprintf("SaveRDFNode: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
			if err != nil {
				return fmt.Errorf("XX Error: PutItem, %s", err.Error())
			}
		}
	}
	//
	// expand SS and LS types into individual S# entries to be indexed// TODO: what about SN, LN
	//
	for _, nv := range nv_ {

		// append child attr value to parent uid-pred list
		switch nv.DT {

		case "SS":

			type Item struct {
				PKey  []byte
				SortK string
				P     string // Dynamo will use AV List type - will convert to SS in convertSet2list()
				S     string
				Ty    string
			}
			var sk string
			if ss, ok := nv.Value.([]string); ok {
				//
				for i, s := range ss {
					sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
					a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s, Ty: nv.Ty}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
					}
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveRDFNode: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						return fmt.Errorf("XX Error: PutItem, %s", err.Error())
					}
				}
			}

		case "LS":

			type Item struct {
				PKey  []byte
				SortK string
				P     string // Dynamo will use AV List type - will convert to SS in convertSet2list()
				S     string
			}
			var sk string
			if ss, ok := nv.Value.([]string); ok {
				//
				for i, s := range ss {
					sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
					a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error())
					}
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveRDFNode: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						return fmt.Errorf("XX Error: PutItem, %s", err.Error())
					}
				}
			}
		}
	}
	//nv_ = nil

	return nil
}

func SavePersons(batch []*reader.PersonT, tyBlock blk.TyAttrBlock, tyName string, lmtr grmgr.Limiter, wg *sync.WaitGroup) {

	var (
		av   map[string]*dynamodb.AttributeValue
		err  error
		iCnt int
	)

	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	convertSet2list := func(av map[string]*dynamodb.AttributeValue) {
		for k, v := range av {
			if k == "Nd" {
				if len(v.BS) > 0 {
					v.L = make([]*dynamodb.AttributeValue, len(v.BS), len(v.BS))
					for i, u := range v.BS {
						v.L[i] = &dynamodb.AttributeValue{B: u}
					}
					v.BS = nil
				}
			}
		}
	}

	res := result.New("Person")
	resDir := result.New("Director")
	resAct := result.New("Actor")
	resBoth := result.New("Actor-Director")

	i := 0
	for _, v := range batch {
		//
		// load type item
		//
		i++
		// if i > 1000 {
		// 	return nil
		// }
		for _, ty := range tyBlock {

			// if ty.Name != v.Name {
			// 	continue
			// }
			//
			// ****** load directors only
			//
			// if v.Ty&1 != 1 {
			// 	continue
			// }
			//
			// create dummy uid-pred entries if required
			//
			if len(ty.Ty) != 0 {

				type Item struct {
					PKey  []byte
					SortK string
					Nd    [][]byte
					XF    []int
					Id    []int
				}
				// convert node blank name to UID
				xf := make([]int, 1, 1)
				xf[0] = blk.ChildUID
				id := make([]int, 1, 1)
				id[0] = 0
				uid := make([][]byte, 1, 1)
				uid[0] = []byte("__")

				a := Item{PKey: v.Uid, SortK: "A#G#:" + ty.C, Nd: uid, XF: xf, Id: id}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					syslog(fmt.Sprintf(" %s: %s", "Error: failed to marshal type definition \n", err.Error()))
					return
				}
				convertSet2list(av)
				iCnt++
				t0 := time.Now()
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String("DyGraph"),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SavePersons: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					syslog(fmt.Sprintf("Error: PutItem, %s", err.Error()))
					return
				}

			} else {

				{
					type Item struct {
						PKey  []byte
						SortK string
						Ty    string
					}
					a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyName}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("Error: failed to marshal type definition : %s", err.Error()))
						return
					}

					t0 := time.Now()

					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SavePersons: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("Error: PutItem, %s", err.Error()))
						return
					}

					switch v.Ty {
					case 1:
						resDir.Cnt++
					case 2:
						resAct.Cnt++
					case 3:
						resBoth.Cnt++
					}
				}
				//
				// load scalar items - only one, "name"
				//
				{
					type Item struct {
						PKey  []byte
						SortK string
						S     string
						P     string
						//		Id    string // original id from 1million.rdf
						Ty string // node type
						T  int
					}

					genSortK := func() string {
						// build sortk for scalars
						// partition key
						var s strings.Builder
						s.WriteString(ty.P)
						s.WriteString("#:")
						s.WriteString(ty.C)
						return s.String()
					}

					a := Item{PKey: v.Uid, SortK: genSortK(), S: v.Name, P: "Name", Ty: tyName, T: int(v.Ty)} //, Id: string(v.Id)}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("MarshalMap Error: failed to marshal type definition ", err.Error()))
						return
					}

					t0 := time.Now()
					iCnt++
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SavePersons: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("PutItem Error: ", err.Error()))
						return
					}
				}
			}
		}
	}

	res.Cnt = resDir.Cnt + resAct.Cnt - resBoth.Cnt

	result.Log <- res
	result.Log <- resDir
	result.Log <- resAct
	result.Log <- resBoth

}

func SaveGenres(tyBlock blk.TyAttrBlock, tyName string) (error, int) {

	var (
		av   map[string]*dynamodb.AttributeValue
		err  error
		iCnt int
	)

	convertSet2list := func(av map[string]*dynamodb.AttributeValue) {
		for k, v := range av {
			if k == "Nd" {
				if len(v.BS) > 0 {
					v.L = make([]*dynamodb.AttributeValue, len(v.BS), len(v.BS))
					for i, u := range v.BS {
						v.L[i] = &dynamodb.AttributeValue{B: u}
					}
					v.BS = nil
				}
			}
		}
	}
	fmt.Println("SaveGenres........")
	res := result.New("Genre")
	//	i := 0
	for _, v := range reader.Genre {
		//
		// load type item
		//
		// i++
		// if i > 1000 {
		// 	return nil
		// }
		for _, ty := range tyBlock {

			// if ty.Name != v.Name {
			// 	continue
			// }
			if len(ty.Ty) != 0 {
				continue
			}
			{
				type Item struct {
					PKey  []byte
					SortK string
					Ty    string
				}
				a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyName}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf(" %s: %s", "Error: failed to marshal type definition ", err.Error()), iCnt
				}

				t0 := time.Now()
				iCnt++
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String("DyGraph"),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SaveGenres: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					return fmt.Errorf("XX Error: PutItem, %s", err.Error()), iCnt
				}
				res.Cnt++
			}
			//
			// load scalar items
			//
			{
				type Item struct {
					PKey  []byte
					SortK string
					S     string
					P     string
					//			Id    string
					Ty string // node type
					T  int
				}

				genSortK := func() string {
					// build sortk for scalars
					// partition key
					var s strings.Builder
					s.WriteString(ty.P)
					s.WriteString("#:")
					s.WriteString(ty.C)
					return s.String()
				}

				a := Item{PKey: v.Uid, SortK: genSortK(), S: v.Name, P: "Name", Ty: tyName} //, Id: string(v.Id)}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					return fmt.Errorf("XX %s: %s", "Error: failed to marshal type definition ", err.Error()), iCnt
				}

				t0 := time.Now()
				iCnt++
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String("DyGraph"),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SaveGenres: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					return fmt.Errorf("XX Error: PutItem, %s", err.Error()), iCnt
				}
			}
			//
			// uid-preds for genre
			//
			for _, ty := range tyBlock {

				if len(ty.Ty) != 0 {

					type Item struct {
						PKey  []byte
						SortK string
						Nd    [][]byte
						XF    []int
						Id    []int
					}
					// convert node blank name to UID
					xf := make([]int, 1, 1)
					xf[0] = blk.ChildUID
					id := make([]int, 1, 1)
					id[0] = 0
					uid := make([][]byte, 1, 1)
					uid[0] = []byte("__")

					a := Item{PKey: v.Uid, SortK: "A#G#:" + ty.C, Nd: uid, XF: xf, Id: id}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						return fmt.Errorf("SavePerformance: Error failed to marshal type definition, %s ", err.Error()), iCnt
					}
					convertSet2list(av)
					iCnt++
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveGenres: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						return fmt.Errorf("SaveGenres:  Error in PutItem, %s", err.Error()), iCnt
					}
				}
			}
		}
	}
	result.Log <- res

	return nil, iCnt
}

func SaveCharacters(batch []*reader.MovieT, tyBlock blk.TyAttrBlock, tyName string, lmtr grmgr.Limiter, wg *sync.WaitGroup) {

	var (
		av  map[string]*dynamodb.AttributeValue
		err error
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	fmt.Println("SaveCharacters........")
	iCnt := 0
	res := result.New("Characters")
	for _, v := range batch {

		for _, p := range v.Performance {

			c := p.Character
			//
			// load type item
			//
			// i++
			// if i > 1000 {
			// 	return nil
			// }
			for _, ty := range tyBlock {

				// if ty.Name != v.Name {
				// 	continue
				// }
				if len(ty.Ty) != 0 {
					continue
				}
				{
					iCnt++
					type Item struct {
						PKey  []byte
						SortK string
						Ty    string
					}
					a := Item{PKey: c.Uid, SortK: "A#T", Ty: tyName}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("SaveCharacters: Error: failed to marshal type definition, %s", err.Error()))
						return
					}

					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveCharacters: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("SaveCharacters: Error in PutItem, %s", err.Error()))
						return
					}
					res.Cnt++
				}
				//
				// load scalar items
				//
				{
					iCnt++
					type Item struct {
						PKey  []byte
						SortK string
						S     string
						P     string
						//	Id    string
						Ty string // node type
					}

					genSortK := func() string {
						// build sortk for scalars
						// partition key
						var s strings.Builder
						s.WriteString(ty.P)
						s.WriteString("#:")
						s.WriteString(ty.C)
						return s.String()
					}

					a := Item{PKey: c.Uid, SortK: genSortK(), S: c.Name, P: "Name", Ty: tyName} //, Id: string(c.Id)}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("SaveCharacters: Error in MarshalMap, %s ", err.Error()))
						return
					}

					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveCharacters: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("SaveCharacters: Error in PutItem, %s", err.Error()))
						return
					}
				}
			}
		}
	}

	result.Log <- res

}

func SavePerformances(batch []*reader.MovieT, tyBlock blk.TyAttrBlock, tyName string, lmtr grmgr.Limiter, wg *sync.WaitGroup) {

	var (
		av   map[string]*dynamodb.AttributeValue
		err  error
		iCnt int
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	convertSet2list := func(av map[string]*dynamodb.AttributeValue) {
		for k, v := range av {
			if k == "Nd" {
				if len(v.BS) > 0 {
					v.L = make([]*dynamodb.AttributeValue, len(v.BS), len(v.BS))
					for i, u := range v.BS {
						v.L[i] = &dynamodb.AttributeValue{B: u}
					}
					v.BS = nil
				}
			}
		}
	}
	fmt.Println("SavePerformances........")
	res := result.New("Performance")
	for _, v := range batch {

		for _, p := range v.Performance {
			//
			// load type item
			//
			{

				type Item struct {
					PKey  []byte
					SortK string
					Ty    string
					//		Id    string
				}
				a := Item{PKey: p.Uid, SortK: "A#T", Ty: tyName} //, Id: string(p.Id)}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					syslog(fmt.Sprintf("SavePerformances: Error failed to marshal type definition. %s ", err.Error()))
					return
				}

				t0 := time.Now()
				iCnt++
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String("DyGraph"),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SavePerformances: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					syslog(fmt.Sprintln("SavePerformances: Error in PutItem, %s", err.Error()))
					return
				}
				res.Cnt++
			}
			//
			// uid-preds for film
			//
			for _, ty := range tyBlock {
				if len(ty.Ty) != 0 {

					type Item struct {
						PKey  []byte
						SortK string
						Nd    [][]byte
						XF    []int
						Id    []int
					}
					// convert node blank name to UID
					xf := make([]int, 1, 1)
					xf[0] = blk.ChildUID
					id := make([]int, 1, 1)
					id[0] = 0
					uid := make([][]byte, 1, 1)
					uid[0] = []byte("__")

					a := Item{PKey: p.Uid, SortK: "A#G#:" + ty.C, Nd: uid, XF: xf, Id: id}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("SavePerformance: Error failed to marshal type definition, %s ", err.Error()))
						return
					}
					convertSet2list(av)
					iCnt++
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SavePerformances: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("SavePerformance:  Erro in PutItem, %s", err.Error()))
						return
					}
				}
			}
		}
	}

	result.Log <- res
}

func SaveMovies(batch []*reader.MovieT, tyBlock blk.TyAttrBlock, tyName string, lmtr grmgr.Limiter, wg *sync.WaitGroup) {

	var (
		av   map[string]*dynamodb.AttributeValue
		err  error
		iCnt int
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	convertSet2list := func(av map[string]*dynamodb.AttributeValue) {
		for k, v := range av {
			if k == "Nd" {
				if len(v.BS) > 0 {
					v.L = make([]*dynamodb.AttributeValue, len(v.BS), len(v.BS))
					for i, u := range v.BS {
						v.L[i] = &dynamodb.AttributeValue{B: u}
					}
					v.BS = nil
				}
			}
		}
	}
	type Item struct {
		PKey  []byte
		SortK string
		S     string
		P     string
		//	Id    string
		Ty string // node type
		T  int
	}
	type ItemDT struct {
		PKey  []byte
		SortK string
		DT    string
		P     string
		//	Id    string
		Ty string // node type
		T  int
	}

	fmt.Println("SaveMovies........", len(batch))
	res := result.New("Film")
	for _, v := range batch {
		//
		// load type item
		//
		{
			type Item struct {
				PKey  []byte
				SortK string
				Ty    string
			}

			a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyName}
			av, err = dynamodbattribute.MarshalMap(a)
			if err != nil {
				syslog(fmt.Sprintf("SaveMovies: Error failed to marshal type definition, %s ", err.Error()))
				return
			}
			iCnt++
			t0 := time.Now()
			ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
				TableName:              aws.String("DyGraph"),
				Item:                   av,
				ReturnConsumedCapacity: aws.String("TOTAL"),
			})
			t1 := time.Now()
			syslog(fmt.Sprintf("SaveMovies: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
			if err != nil {
				syslog(fmt.Sprintf("SaveMovies:  Error in PutItem, %s", err.Error()))
				return
			}
			res.Cnt++
		}

		for _, ty := range tyBlock {

			if len(ty.Ty) != 0 {

				type Item struct {
					PKey  []byte
					SortK string
					Nd    [][]byte
					XF    []int
					Id    []int
				}
				// convert node blank name to UID
				xf := make([]int, 1, 1)
				xf[0] = blk.ChildUID
				id := make([]int, 1, 1)
				id[0] = 0
				uid := make([][]byte, 1, 1)
				uid[0] = []byte("__")

				a := Item{PKey: v.Uid, SortK: "A#G#:" + ty.C, Nd: uid, XF: xf, Id: id}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					syslog(fmt.Sprintf("SaveMovies: Error failed to marshal type definition, %s", err.Error()))
					return
				}
				iCnt++
				convertSet2list(av)
				t0 := time.Now()
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String("DyGraph"),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SaveMovies: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					syslog(fmt.Sprintf("SaveMovies:  Error in PutItem, %s", err.Error()))
					return
				}
			} else {
				//
				// load scalar items
				//
				{

					genSortK := func() string {
						// build sortk for scalars
						// partition key
						var s strings.Builder
						s.WriteString(ty.P)
						s.WriteString("#:")
						s.WriteString(ty.C)
						return s.String()
					}
					switch ty.DT {
					case "S":
						a := Item{PKey: v.Uid, SortK: genSortK(), S: v.Name[0], P: ty.Name, Ty: tyName} //, Id: string(v.Id)}
						av, err = dynamodbattribute.MarshalMap(a)
					case "DT":
						// Ordinarily this should be driven from type info, but hardwire ird as DT type (as that's is definition in type table)
						a := ItemDT{PKey: v.Uid, SortK: genSortK(), DT: v.Ird, P: ty.Name, Ty: tyName} //, Id: string(v.Id)}
						av, err = dynamodbattribute.MarshalMap(a)
					}
					if err != nil {
						syslog(fmt.Sprintf("SaveMovies: Error failed to marshal type definition %s", err.Error()))
						return
					}
					iCnt++
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveMovies scalar: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("SaveMovies: Error inPutItem, %s", err.Error()))
						return
					}
				}
			}
		}
	}
	result.Log <- res
}

// func Orson(s string) {
// 	syslog(fmt.Sprintf("db.Orson check if exists  - %s  ", s))
// 	filt := expression.Name("Id").Equal(expression.Value("13523357566899103591"))
// 	expr, err := expression.NewBuilder().WithFilter(filt).Build()
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	input := &dynamodb.ScanInput{
// 		ExpressionAttributeNames:  expr.Names(),
// 		ExpressionAttributeValues: expr.Values(),
// 		FilterExpression:          expr.Filter(),
// 		ProjectionExpression:      expr.Projection(),
// 		TableName:                 aws.String("DyGraph"),
// 	}
// 	input = input.SetTableName("DyGraph").SetReturnConsumedCapacity("TOTAL").SetConsistentRead(false)

// 	//
// 	syslog("db.Orson check if exists about to scan  ")
// 	t0 := time.Now()
// 	result, err := dynSrv.Scan(input)
// 	t1 := time.Now()
// 	if err != nil {
// 		syslog(fmt.Sprintf("db.Orson scan error - %s", err.Error()))
// 		return
// 	}
// 	//
// 	syslog(fmt.Sprintf("Orson: consumed capacity for Query: %s,  Item Count: %d Duration: %s", result.ConsumedCapacity, int(*result.Count), t1.Sub(t0)))
// 	if int(*result.Count) == 0 {
// 		syslog("Orson -  NOT FOUND....")
// 		return
// 	}
// 	syslog("db.Orson FOUND.....")

// }
