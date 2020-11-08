package db

import (
	"fmt"
	"time"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/dbConn"
	slog "github.com/DynamoGraph/syslog"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	logid = "TypesDB: "
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

func init() {

	dynSrv = dbConn.New()

	tynames, err = loadTypeShortNames()
	if err != nil {
		panic(err)
	}
	//
	// populate type short name cache. This cache is conccurent safe as it is readonly from now on.
	//
	tyShortNm = make(map[string]string)
	for _, v := range tynames {
		tyShortNm[v.LongNm] = v.ShortNm
	}

}

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

func LoadDataDictionary() (blk.TyIBlock, error) {

	filt := expression.BeginsWith(expression.Name("Nm"), "#").Not()
	expr, err := expression.NewBuilder().WithFilter(filt).Build()
	if err != nil {
		return nil, newDBExprErr("LoadDataDictionary", "", "", err)
	}

	input := &dynamodb.ScanInput{
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName("DyGTypes").SetReturnConsumedCapacity("TOTAL").SetConsistentRead(false)
	//
	t0 := time.Now()
	result, err := dynSrv.Scan(input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("LoadDataDictionary", "Scan", err)
	}
	syslog(fmt.Sprintf("LoadDataDictionary: consumed capacity for Scan: %s,  Item Count: %d Duration: %s", result.ConsumedCapacity, int(*result.Count), t1.Sub(t0)))
	//
	if int(*result.Count) == 0 {
		//newDBNoItemFound(rt string, pk string, sk string, api string, err error)
		return nil, newDBNoItemFound("LoadDataDictionary", "", "", "Scan")
	}
	//
	dd := make(blk.TyIBlock, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &dd)
	if err != nil {
		//func newDBUnmarshalErr(rt string, pk string, sk string, api string, err error) error {
		return nil, newDBUnmarshalErr("UnmarshalListOfMaps", "", "", "UnmarshalListOfMaps", err)
	}

	return dd, nil

}

func loadTypeShortNames() ([]tyNames, error) {

	syslog("db.loadTypeShortNames ")
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
