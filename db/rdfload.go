package db

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/rdf/grmgr"
	"github.com/DynamoGraph/types"

	"github.com/DynamoGraph/rdf.m/reader"
	"github.com/DynamoGraph/rdf.m/result"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func SavePersons(batch []*reader.PersonT, tyBlock blk.TyAttrBlock, tyName string, lmtr grmgr.Limiter, wg *sync.WaitGroup) {

	var (
		av        map[string]*dynamodb.AttributeValue
		err       error
		iCnt      int
		tyShortNm string
		ok        bool
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

	if tyShortNm, ok = types.GetTyShortNm(tyName); !ok {
		syslog(fmt.Sprintf("Error: type name %q not found in  types.GetTyShortNm \n", tyName))
		return
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
					TableName:              aws.String(graphTbl),
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
						Ix    string
					}
					a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyShortNm, Ix: "X"}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("Error: failed to marshal type definition : %s", err.Error()))
						return
					}

					t0 := time.Now()

					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String(graphTbl),
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

					a := Item{PKey: v.Uid, SortK: genSortK(), S: v.Name, P: "Name", Ty: tyShortNm}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("MarshalMap Error: failed to marshal type definition ", err.Error()))
						return
					}

					t0 := time.Now()
					iCnt++
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String(graphTbl),
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

func SaveGenres(tyBlock blk.TyAttrBlock, tyName string) {

	var (
		av        map[string]*dynamodb.AttributeValue
		err       error
		iCnt      int
		tyShortNm string
		ok        bool
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
	if tyShortNm, ok = types.GetTyShortNm(tyName); !ok {
		syslog(fmt.Sprintf("Error: type name %q not found in  types.GetTyShortNm \n", tyName))
		return
	}
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
					Ix    string
				}
				a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyShortNm, Ix: "X"}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					syslog(fmt.Sprintf("SaveGenre: Error: failed to marshal type definition %s", err.Error()))
					return
				}

				t0 := time.Now()
				iCnt++
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String(graphTbl),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SaveGenres: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					syslog(fmt.Sprintf("SaveGenre: Error: PutItem, %s", err.Error()))
					return
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

				a := Item{PKey: v.Uid, SortK: genSortK(), S: v.Name, P: "Name", Ty: tyShortNm} //, Id: string(v.Id)}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					syslog(fmt.Sprintf("SaveGenre:  Error: failed to marshal type definition %s", err.Error()))
					return
				}

				t0 := time.Now()
				iCnt++
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String(graphTbl),
					Item:                   av,
					ReturnConsumedCapacity: aws.String("TOTAL"),
				})
				t1 := time.Now()
				syslog(fmt.Sprintf("SaveGenres: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
				if err != nil {
					syslog(fmt.Sprintf("SaveGenre: Error: PutItem, %s", err.Error()))
					return
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
						syslog(fmt.Sprintf("SaveGenres: Error failed to marshal type definition, %s ", err.Error()))
						return
					}
					convertSet2list(av)
					iCnt++
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String(graphTbl),
						Item:                   av,
						ReturnConsumedCapacity: aws.String("TOTAL"),
					})
					t1 := time.Now()
					syslog(fmt.Sprintf("SaveGenres: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
					if err != nil {
						syslog(fmt.Sprintf("SaveGenres:  Error in PutItem, %s", err.Error()))
						return
					}
				}
			}
		}
	}
	result.Log <- res

	return
}

func SaveCharacters(batch []*reader.MovieT, tyBlock blk.TyAttrBlock, tyName string, lmtr grmgr.Limiter, wg *sync.WaitGroup) {

	var (
		av        map[string]*dynamodb.AttributeValue
		err       error
		tyShortNm string
		ok        bool
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	fmt.Println("SaveCharacters........")
	if tyShortNm, ok = types.GetTyShortNm(tyName); !ok {
		syslog(fmt.Sprintf("Error: type name %q not found in  types.GetTyShortNm \n", tyName))
		return
	}
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
						Ix    string
					}
					a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyShortNm, Ix: "X"}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("SaveCharacters: Error: failed to marshal type definition, %s", err.Error()))
						return
					}

					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String(graphTbl),
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

					a := Item{PKey: c.Uid, SortK: genSortK(), S: c.Name, P: "Name", Ty: tyShortNm} //, Id: string(c.Id)}
					av, err = dynamodbattribute.MarshalMap(a)
					if err != nil {
						syslog(fmt.Sprintf("SaveCharacters: Error in MarshalMap, %s ", err.Error()))
						return
					}

					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String(graphTbl),
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
		av        map[string]*dynamodb.AttributeValue
		err       error
		iCnt      int
		tyShortNm string
		ok        bool
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
	if tyShortNm, ok = types.GetTyShortNm(tyName); !ok {
		syslog(fmt.Sprintf("Error: type name %q not found in  types.GetTyShortNm \n", tyName))
		return
	}
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
					Ix    string
				}
				a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyShortNm, Ix: "X"}
				av, err = dynamodbattribute.MarshalMap(a)
				if err != nil {
					syslog(fmt.Sprintf("SavePerformances: Error failed to marshal type definition. %s ", err.Error()))
					return
				}

				t0 := time.Now()
				iCnt++
				ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
					TableName:              aws.String(graphTbl),
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
						TableName:              aws.String(graphTbl),
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
		av        map[string]*dynamodb.AttributeValue
		err       error
		iCnt      int
		tyShortNm string
		ok        bool
		rnd       int
		id        string
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
		Ty    string // node type
	}
	type ItemRv struct {
		PKey  []byte
		SortK string
		N     float64
		P     string
		Ty    string // node type
	}
	type ItemDT struct {
		PKey  []byte
		SortK string
		DT    string
		P     string
		Ty    string // node type
	}

	fmt.Println("SaveMovies........", len(batch))
	if tyShortNm, ok = types.GetTyShortNm(tyName); !ok {
		syslog(fmt.Sprintf("Error: type name %q not found in  types.GetTyShortNm \n", tyName))
		return
	}
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
				Ix    string
			}
			a := Item{PKey: v.Uid, SortK: "A#T", Ty: tyShortNm, Ix: "X"}
			av, err = dynamodbattribute.MarshalMap(a)
			if err != nil {
				syslog(fmt.Sprintf("SaveMovies: Error failed to marshal type definition, %s ", err.Error()))
				return
			}
			iCnt++
			t0 := time.Now()
			ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
				TableName:              aws.String(graphTbl),
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
					TableName:              aws.String(graphTbl),
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
					rnd = rand.Intn(20)
					id = strconv.Itoa(rnd) + "b-1234-B-2020"
					switch ty.Name {
					case "NetflixId":
						a := Item{PKey: v.Uid, SortK: genSortK(), S: id, P: ty.Name, Ty: tyShortNm}
						av, err = dynamodbattribute.MarshalMap(a)
					case "Revenue":
						a := ItemRv{PKey: v.Uid, SortK: genSortK(), N: float64(rnd), P: ty.Name, Ty: tyShortNm}
						av, err = dynamodbattribute.MarshalMap(a)
					case "Name":
						a := Item{PKey: v.Uid, SortK: genSortK(), S: v.Name[0], P: ty.Name, Ty: tyShortNm}
						av, err = dynamodbattribute.MarshalMap(a)
					case "Initial_Release_Date":
						// Ordinarily this should be driven from type info, but hardwire ird as DT type (as that's is definition in type table)
						a := ItemDT{PKey: v.Uid, SortK: genSortK(), DT: v.Ird, P: ty.Name, Ty: tyShortNm}
						av, err = dynamodbattribute.MarshalMap(a)
					}
					if err != nil {
						syslog(fmt.Sprintf("SaveMovies: Error failed to marshal type definition %s", err.Error()))
						return
					}
					iCnt++
					t0 := time.Now()
					ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
						TableName:              aws.String(graphTbl),
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
// 		TableName:                 aws.String(graphTbl),
// 	}
// 	input = input.SetTableName(graphTbl).SetReturnConsumedCapacity("TOTAL").SetConsistentRead(false)

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
