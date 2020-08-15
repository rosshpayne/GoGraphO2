package db

import (
	"bytes"
	"fmt"
	"strings"

	blk "github.com/DynamoGraph/block"
	//slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

var dynSrv *dynamodb.DynamoDB

func init() {
	// establish dynamodb service
	dynamodbSrv := func() *dynamodb.DynamoDB {
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1"),
		})
		if err != nil {
			panic(err)
		}
		return dynamodb.New(sess, aws.NewConfig())
	}

	dynSrv = dynamodbSrv()
}

type pKey struct {
	PKey  []byte
	SortK string
}

// ty.   - type of child? node
// puid - parent node uid
// sortK - uidpred of parent to append value G#:S (sibling) or G#:F (friend)
// value - child value
func PropagateChildData(ty blk.TyAttrD, puid util.UID, sortK string, targetUID util.UID, value interface{}) error { //, wg ...*sync.WaitGroup) error {

	// defer func() {
	// 	if len(wg) > 0 {
	// 		wg[0].Done()
	// 	}
	// }()

	var (
		lty     string
		sortk   string
		err     error
		expr    expression.Expression
		updateC expression.UpdateBuilder
		//xx      map[string]*dynamodb.AttributeValue
		values map[string]*dynamodb.AttributeValue
	)

	convertSet2List := func() {
		// fix to possible sdk error/issue for Binary ListAppend operations. SDK builds
		//  a BS rather than a LIST for LISTAPPEND operation invovling binary data.
		// This is the default for binary for some reason - very odd.
		// We therefore need to convert from BS created by the SDK to LB (List Binary)
		var s strings.Builder
		for k, v := range expr.Names() {
			switch *v {
			//case "XB", "Nd":
			case "Nd", "LBl", "LS", "LN", "XB", "XF", "LB":
				s.WriteByte(':')
				s.WriteByte(k[1])
				// check if BS is used and then convert if it is
				if len(values[s.String()].BS) > 0 {
					nl := make([]*dynamodb.AttributeValue, 1, 1)
					nl[0] = &dynamodb.AttributeValue{B: values[s.String()].BS[0]}
					values[s.String()] = &dynamodb.AttributeValue{L: nl}
				}
				s.Reset()
			}
		}
	}

	if ty.DT != "Nd" {
		// simple scalar e.g. Age
		lty = "L" + ty.DT
		sortk = sortK + "#:" + ty.C // TODO: currently ignoring concept of partitioning data within node block. Is that right?
	} else {
		// uid-predicate e.g. Sibling
		lty = "Nd"
		sortk = "G#:" + sortK[len(sortK)-1:] // TODO: currently ignoring concept of partitioning data within node block. Is that right?
	}
	//
	// shadow XB null identiier
	//
	null := make([]bool, 1, 1)
	if value == nil {
		null[0] = true
	}
	// append child attr value to parent uid-pred list
	switch lty {

	case "LI", "LF":
		// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
		if value == nil {
			switch ty.DT {
			case "I":
				value = int64(0)
			case "F":
				value = float64(0)
			}
		}
		fmt.Println("LN : ")
		switch x := value.(type) {
		case int:
			fmt.Println("LN int: ", x)
			v := make([]int, 1, 1)
			v[0] = x
			updateC = expression.Set(expression.Name("LN"), expression.ListAppend(expression.Name("LN"), expression.Value(v)))
		case int32:
			fmt.Println("LN int32: ", x)
			v := make([]int32, 1, 1)
			v[0] = x
			updateC = expression.Set(expression.Name("LN"), expression.ListAppend(expression.Name("LN"), expression.Value(v)))
		case int64:
			fmt.Println("LN int64: ", x)
			v := make([]int64, 1, 1)
			v[0] = x
			updateC = expression.Set(expression.Name("LN"), expression.ListAppend(expression.Name("LN"), expression.Value(v)))
		case float64:
			fmt.Println("LN  float64: ", x)
			v := make([]float64, 1, 1)
			v[0] = x
			updateC = updateC.Set(expression.Name("LN"), expression.ListAppend(expression.Name("LN"), expression.Value(v)))
		// case string:
		// 	v := make([]string, 1)
		// 	v[0] = x
		// 	updateC = expression.Set(expression.Name(lty), expression.ListAppend(expression.Name(lty), expression.Value(v)))
		default:
			// TODO: check if string - ok
			panic(fmt.Errorf("data type must be a number, int64, float64"))
		}
		// handle NULL values
		updateC = updateC.Set(expression.Name("XB"), expression.ListAppend(expression.Name("XB"), expression.Value(null)))
		expr, err = expression.NewBuilder().WithUpdate(updateC).Build()
		if err != nil {
			return newDBExprErr("PropagateChildData", "", "", err)
		}

	case "LBl":
		if value == nil {
			value = false
		}
		if x, ok := value.(bool); !ok {
			logerr(fmt.Errorf("data type must be a bool"), true)
		} else {
			v := make([]bool, 1, 1)
			v[0] = x
			updateC = expression.Set(expression.Name(lty), expression.ListAppend(expression.Name(lty), expression.Value(v)))
		}
		updateC = updateC.Set(expression.Name("XB"), expression.ListAppend(expression.Name("XB"), expression.Value(null)))
		expr, err = expression.NewBuilder().WithUpdate(updateC).Build()
		if err != nil {
			return newDBExprErr("PropagateChildData", "", "", err)
		}

	case "LS":
		if value == nil {
			value = "__NULL__"
		}
		if x, ok := value.(string); !ok {
			logerr(fmt.Errorf("data type must be a string"), true)
		} else {
			v := make([]string, 1, 1)
			v[0] = x
			fmt.Println("value = ", v)
			updateC = expression.Set(expression.Name(lty), expression.ListAppend(expression.Name(lty), expression.Value(v)))
		}
		updateC = updateC.Set(expression.Name("XB"), expression.ListAppend(expression.Name("XB"), expression.Value(null)))
		expr, err = expression.NewBuilder().WithUpdate(updateC).Build()
		if err != nil {
			return newDBExprErr("PropagateChildData", "", "", err)
		}

	case "LB":
		if value == nil {
			value = []byte("__NULL__")
		}
		if x, ok := value.([]byte); !ok {
			logerr(fmt.Errorf("data type must be a byte slice"), true)
		} else {
			v := make([][]byte, 1, 1)
			v[0] = x
			updateC = expression.Set(expression.Name(lty), expression.ListAppend(expression.Name(lty), expression.Value(value)))
		}
		updateC = updateC.Set(expression.Name("XB"), expression.ListAppend(expression.Name("XB"), expression.Value(null)))
		expr, err = expression.NewBuilder().WithUpdate(updateC).Build()
		if err != nil {
			return newDBExprErr("PropagateChildData", "", "", err)
		}

	case "Nd":
		xf := make([]int, 1)
		if bytes.Equal(puid, targetUID) {
			xf[0] = blk.ChildUID
		} else {
			xf[0] = blk.OverflowBlockUID
		}
		if x, ok := value.([]byte); !ok {
			logerr(fmt.Errorf("data type must be a byte slice"), true)
		} else {
			v := make([][]byte, 1)
			v[0] = x
			updateC = expression.Set(expression.Name("Nd"), expression.ListAppend(expression.Name("Nd"), expression.Value(v)))
		}
		updateC = updateC.Set(expression.Name("XF"), expression.ListAppend(expression.Name("XF"), expression.Value(xf)))
		// increment count of nodes
		updateC = updateC.Add(expression.Name("cnt"), expression.Value(1))
		//
		expr, err = expression.NewBuilder().WithUpdate(updateC).Build()
		if err != nil {
			return newDBExprErr("PropagateChildData", "", "", err)
		}

	}
	values = expr.Values()
	// convert expression values result from binary Set to binary List
	convertSet2List()
	//
	// Marshal primary key of parent node
	//
	var pkey pKey

	if bytes.Equal(puid, targetUID) {
		puidb64 := puid.Encodeb64()
		pkey = pKey{PKey: puidb64, SortK: sortk}
	} else {
		targetb64 := targetUID.Encodeb64()
		pkey = pKey{PKey: targetb64, SortK: sortk}
	}
	av, err := dynamodbattribute.MarshalMap(&pkey)
	//
	if err != nil {
		return newDBMarshalingErr("PropagateChildData", "X", "", "MarshalMap", err)
	}
	//
	input := &dynamodb.UpdateItemInput{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: values,
		UpdateExpression:          expr.Update(),
	}
	input = input.SetTableName("DyGraph").SetReturnConsumedCapacity("TOTAL")
	//
	{
		//	t0 := time.Now()
		_, err := dynSrv.UpdateItem(input)
		//	t1 := time.Now()
		if err != nil {
			return newDBSysErr("PropagateChildData", "UpdateItem", err)
		}
		//syslog(fmt.Sprintf("PropagateChildData:consumed capacity for Query  %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))

	}
	return nil
}

func logerr(err error, b ...bool) {
	if len(b) > 0 && b[0] {
		panic(err)
	}

	panic(err)
}
