package db

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/rdf/ds"
	"github.com/DynamoGraph/rdf/grmgr"
	"github.com/DynamoGraph/rdf/uuid"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	logid = "rdfSaveDB: "
)

type tyNames struct {
	ShortNm string `json:"Atr"`
	LongNm  string
}

var (
	dynSrv    *dynamodb.DynamoDB
	err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

func GetTyShortNm(longNm string) (string, bool) {
	s, ok := tyShortNm[longNm]
	return s, ok
}

func GetTyLongNm(tyNm string) (string, bool) {
	for shortNm, longNm := range tyShortNm {
		if tyNm == longNm {
			return shortNm, true
		}
	}
	return "", false
}

func init() {
	// TODO: source session info from a central package, maybe, DynamGraph/db package
	dynamodbSrv := func() *dynamodb.DynamoDB {
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1"),
		})
		if err != nil {
			logerr(err, true)
		}
		return dynamodb.New(sess, aws.NewConfig())
	}

	dynSrv = dynamodbSrv()
	//
	tynames, err = loadTypeShortNames()
	if err != nil {
		panic(err)
	}
	//
	tyShortNm = make(map[string]string)
	for _, v := range tynames {
		tyShortNm[v.LongNm] = v.ShortNm
	}

}

func loadTypeShortNames() ([]tyNames, error) {

	syslog(fmt.Sprintf("db.loadTypeShortNames "))
	keyC := expression.KeyEqual(expression.Key("Nm"), expression.Value("#T"))
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("loadTypeShortNames", "", "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName("DyGTypes").SetReturnConsumedCapacity("TOTAL").SetConsistentRead(false)
	//
	t0 := time.Now()
	result, err := dynSrv.Query(input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("loadTypeShortNames", "Query", err)
	}
	syslog(fmt.Sprintf("loadTypeShortNames: consumed capacity for Query: %s,  Item Count: %d Duration: %s", result.ConsumedCapacity, int(*result.Count), t1.Sub(t0)))
	if int(*result.Count) == 0 {
		return nil, newDBNoItemFound("loadTypeShortNames", "", "", "Query")
	}
	//
	// Unmarshal result into
	//
	items := make([]tyNames, *result.Count, *result.Count)
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		return nil, newDBUnmarshalErr("loadTypeShortNames", "", "", "UnmarshalListOfMaps", err)
	}
	return items, nil
}

//TODO: this routine requires an error log service. Code below  writes errors to the screen in some cases but not most. Errors are returned but calling routines is a goroutine so thqt get lost.

func SaveRDFNode(nv_ []ds.NV, wg *sync.WaitGroup, lmtr grmgr.Limiter) {

	defer wg.Done()
	defer func() func() {
		return func() {
			err := err
			if err != nil {
				syslog(fmt.Sprintf("Error: [%s]", err.Error()))
			} else {
				syslog(fmt.Sprintf("Finished"))
			}
		}
	}()()

	lmtr.StartR()
	defer lmtr.EndR()

	var (
		av map[string]*dynamodb.AttributeValue
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

	// for _, nv := range nv_ {
	// 	slog.Log("SaveRDFNode: xxx ", fmt.Sprintf("NV = %#v\n ", nv))
	// }
	//	syslog( fmt.Sprintf("SaveRDFNode : %d ", len(nv_))) //, nv_[0]))

	localCh := make(chan util.UID)

	request := uuid.Request{SName: nv_[0].SName, RespCh: localCh}

	uuid.ReqCh <- request

	UID := <-localCh

	var (
		NdUid     util.UID
		tyShortNm string
		ok        bool
	)
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
			if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
				syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
				return
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if i, ok := nv.Value.(int); !ok {
				err = fmt.Errorf("Value is not an Int ")
			} else {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, N: i, P: nv.Name, Ty: tyShortNm} // nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "failed to marshal type definition ", err.Error())
				}
			}

		case "F":

			type Item struct {
				PKey  []byte
				SortK string
				N     string // float kept in program as string - this is a trial to see if keeping as string works.
				P     string
				Ty    string // node type
			}
			if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
				syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
				return
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if f, ok := nv.Value.(string); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, N: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
			}

		case "S":

			if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
				syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
				return
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if f, ok := nv.Value.(string); ok {
				//
				// use Ix attribute to determine whether the P attribute (PK of GSI) should be populated.
				//  For Ix value of FT (full text search)the S attribute will not appear in the GSI (P_S) as ElasticSearch has it covered
				//  TODO: is it worthwhile have an FTGSI attribute to have it both index in ES and GSI
				//
				switch nv.Ix {
				case "FT", "ft":
					type Item struct {
						PKey  []byte
						SortK string
						S     string
						Ty    string // node type
					}
					fmt.Println("nv.IX = FT ")
					a := Item{PKey: UID, SortK: nv.Sortk, S: f, Ty: tyShortNm} //nv.Ty}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
					}
				default:
					type Item struct {
						PKey  []byte
						SortK string
						S     string
						P     string
						Ty    string // node type
					}
					fmt.Println("default.... ")
					a := Item{PKey: UID, SortK: nv.Sortk, S: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
					}
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an string ")
			}

		case "DT": // DateTime

			type Item struct {
				PKey  []byte
				SortK string
				DT    string
				P     string
				Ty    string // node type
			}
			if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
				syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
				return
			}
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if dt, ok := nv.Value.(time.Time); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, DT: dt.String(), P: nv.Name, Ty: tyShortNm} //nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an String ")
			}

		case "ty": // node type entry

			type Item struct {
				PKey  []byte
				SortK string
				Ty    string // node type
				Ix    string
			}

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if s, ok := nv.Value.(string); ok {
				if tyShortNm, ok = GetTyShortNm(s); !ok {
					syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
					return
				}
				a := Item{PKey: UID, SortK: "A#T", Ty: tyShortNm, Ix: "X"}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
			}

		case "Bl":

			type Item struct {
				PKey  []byte
				SortK string
				Bl    bool
				P     string
				Ty    string // node type
			}
			if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
				syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
				return
			}
			if f, ok := nv.Value.(bool); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, Bl: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
			}

		case "B":

			type Item struct {
				PKey  []byte
				SortK string
				B     []byte
				P     string
				Ty    string // node type
			}
			if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
				syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
				return
			}
			if f, ok := nv.Value.([]byte); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, B: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
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
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
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
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
			}

		case "SI":

			type Item struct {
				PKey  []byte
				SortK string
				SN    []int
			}
			if f, ok := nv.Value.([]int); ok {
				// populate with dummy item to establish LIST
				a := Item{PKey: UID, SortK: nv.Sortk, SN: f}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not a slice of int")
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
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an slice float64 ")
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
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an slice of bool ")
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
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an String Set ")
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
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an Int ")
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
					syslog(fmt.Sprintf("UID Nd request  : %#v", request))

					uuid.ReqCh <- request

					UID := <-localCh

					uid[i] = []byte(UID)
					xf[i] = blk.ChildUID
					id[i] = 0

				}
				NdUid = UID // save to use to create a Type item
				syslog(fmt.Sprintf("Received UID: %T %v %s\n", UID, UID, UID.String()))
				a := Item{PKey: UID, SortK: nv.Sortk, Nd: uid, XF: xf, Id: id}
				//e:= uuid.Edges{	PKey  : UID, SortK: nv.SortK, Nd : uid}
				//execute AttachNode based on data in a
				//uuid.EdgesCh <- uuid.Edges(a)
				syslog(fmt.Sprintf("a: = %#v", a))
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
			} else {
				err = fmt.Errorf(" nv.Value is not an string slice ")
			}

		}
		if err != nil {
			return
		}
		convertSet2list(av)
		//
		// PutItem
		//
		t0 := time.Now()
		ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
			TableName:              aws.String("DyGraph"),
			Item:                   av,
			ReturnConsumedCapacity: aws.String("TOTAL"),
		})
		t1 := time.Now()
		syslog(fmt.Sprintf("SaveRDFNode: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
		if err != nil {
			err = fmt.Errorf("Error: PutItem, %s", err.Error())
			return
		}
		//
		// add a Type item for each uid-pred
		//
		if nv.DT == "Nd" {
			{
				type Item struct {
					PKey  []byte
					SortK string
					Ty    string // node type
				}
				syslog(fmt.Sprintf("Adding Type item for uid-pred %s %s to %s UID: %s", nv.Ty, nv.Sortk, nv.Sortk+"#T", NdUid.String()))
				// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
				a := Item{PKey: NdUid, SortK: nv.Sortk + "#T", Ty: tyShortNm}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
				}
				{
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String("DyGraph"),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveRDFNode for Ty: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						err = fmt.Errorf("Error: PutItem for uid-pred type  %s", err.Error())
					}
				}
			}
		}
		if err != nil {
			return
		}

	}
	//
	// expand Set and List types into individual S# entries to be indexed// TODO: what about SN, LN
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

					if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
						syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
						return
					}

					sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
					a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s, Ty: tyShortNm} //nv.Ty}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
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
						err = fmt.Errorf("Error: PutItem, %s", err.Error())
					}
				}
			}
			if err != nil {
				return
			}

		case "SI":

			type Item struct {
				PKey  []byte
				SortK string
				P     string // Dynamo will use AV List type - will convert to SS in convertSet2list()
				N     float64
				Ty    string
			}
			var sk string
			if si, ok := nv.Value.([]int); ok {
				//
				for i, s := range si {

					if tyShortNm, ok = GetTyShortNm(nv.Ty); !ok {
						syslog(fmt.Sprintf("Error: type name %q not found in GetTyShortNm \n", nv.Ty))
						return
					}

					sk = "S#:" + nv.C + "#" + strconv.Itoa(i)

					a := Item{PKey: UID, SortK: sk, P: nv.Name, N: float64(s), Ty: tyShortNm} //nv.Ty}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
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
						err = fmt.Errorf("Error: PutItem, %s", err.Error())
					}
				}
			}
			if err != nil {
				return
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
						err = fmt.Errorf("%s: %s", "Error: failed to marshal type definition ", err.Error())
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
						err = fmt.Errorf("Error: PutItem, %s", err.Error())
					}
				}
			}
			if err != nil {
				return
			}

		}
	}
}
