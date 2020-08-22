package db

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/rdf/ds"
	"github.com/DynamoGraph/rdf/grmgr"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"

	"github.com/DynamoGraph/rdf/uuid"

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
	slog.Log("SaveRDFNode: ", fmt.Sprintf("SaveRDFNode : %d ", len(nv_))) //, nv_[0]))

	localCh := make(chan util.UID)

	request := uuid.Request{SName: nv_[0].SName, RespCh: localCh}
	slog.Log("SaveRDFNode: ", fmt.Sprintf("UID request  : %#v", request))

	uuid.ReqCh <- request

	slog.Log("SaveRDFNode: ", "Waiting for UID ......................")

	UID := <-localCh

	slog.Log("SaveRDFNode: ", fmt.Sprintf("UID received  : %s", UID.String()))

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

		case "ty": // node DyG type

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
				return fmt.Errorf(" Value is not an Int ")
			}
		}
		convertSet2list(av)
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
