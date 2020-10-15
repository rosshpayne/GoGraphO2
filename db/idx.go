package db

import (
	"fmt"

	"github.com/DynamoGraph/util"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Equality int

const (
	EQ Equality = iota + 1
	LT
	GT
	GE
	LE
)

// api for GQL query functions

type NodeResult struct {
	PKey  util.UID
	SortK string
	Ty    string
}

type QResult []NodeResult

func GSIQueryN(attr AttrName, lv float64, op Equality) (QResult, error) {

	var keyC expression.KeyConditionBuilder
	//
	// DD determines what index to search based on Key value. Here Key is Name and DD knows its a string hence index P_S
	//
	switch op {
	case EQ:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").Equal(expression.Value(lv)))
	case LT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").LessThan(expression.Value(lv)))
	case GT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").GreaterThan(expression.Value(lv)))
	case GE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").GreaterThanEqual(expression.Value(lv)))
	case LE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").LessThanEqual(expression.Value(lv)))
	}
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("GSIS", attr, "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName("DyGraph").SetIndexName("P_S").SetReturnConsumedCapacity("TOTAL")
	//
	result, err := dynSrv.Query(input)
	if err != nil {
		return nil, newDBSysErr("GSIS", "Query", err)
	}
	syslog(fmt.Sprintf("GSIS:consumed capacity for Query index P_S, %s.  ItemCount %d  %d ", result.ConsumedCapacity, len(result.Items), *result.Count))
	//
	if int(*result.Count) == 0 {
		return nil, newDBNoItemFound("GSIS", attr, "", "Query") //TODO add lv
	}
	//
	ptR := make(QResult, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &ptR)
	if err != nil {
		return nil, newDBUnmarshalErr("GSIS", attr, "", "UnmarshalListOfMaps", err)
	}
	//
	return ptR, nil
}

func GSIQueryS(attr AttrName, lv string, op Equality) (QResult, error) {

	var keyC expression.KeyConditionBuilder
	//
	// DD determines what index to search based on Key value. Here Key is Name and DD knows its a string hence index P_S
	//
	switch op {
	case EQ:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").Equal(expression.Value(lv)))
	case LT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").LessThan(expression.Value(lv)))
	case GT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").GreaterThan(expression.Value(lv)))
	case GE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").GreaterThanEqual(expression.Value(lv)))
	case LE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").LessThanEqual(expression.Value(lv)))
	}
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("GSIS", attr, "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName("DyGraph").SetIndexName("P_S").SetReturnConsumedCapacity("TOTAL")
	//
	result, err := dynSrv.Query(input)
	if err != nil {
		return nil, newDBSysErr("GSIS", "Query", err)
	}
	syslog(fmt.Sprintf("GSIS:consumed capacity for Query index P_S, %s.  ItemCount %d  %d ", result.ConsumedCapacity, len(result.Items), *result.Count))
	//
	if int(*result.Count) == 0 {
		return nil, newDBNoItemFound("GSIS", attr, lv, "Query")
	}
	//
	ptR := make(QResult, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &ptR)
	if err != nil {
		return nil, newDBUnmarshalErr("GSIS", attr, lv, "UnmarshalListOfMaps", err)
	}
	//
	return ptR, nil
}
