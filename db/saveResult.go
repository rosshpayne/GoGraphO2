package db

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func SaveTestResult(test string, status string, nodes int, levels []int, parseET, execET string, msg string, json string, fetches int, abort bool) {

	type Item struct {
		When    string
		Test    string
		Status  string
		Nodes   int
		Levels  []int
		ParseET string
		ExecET  string
		DBread  int
		Msg     string
		Json    string
	}

	if abort {
		return
	}

	when := time.Now().String()
	a := Item{When: when[:21], Test: test, Status: status, Nodes: nodes, Levels: levels, ParseET: parseET, ExecET: execET, Json: json, DBread: fetches, Msg: msg}
	av, err := dynamodbattribute.MarshalMap(a)
	if err != nil {
		syslog(fmt.Sprintf(" %s: %s", "Error: failed to marshal type definition \n", err.Error()))
		return
	}

	t0 := time.Now()
	ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
		TableName:              aws.String("TestLog"),
		Item:                   av,
		ReturnConsumedCapacity: aws.String("TOTAL"),
	})
	t1 := time.Now()
	syslog(fmt.Sprintf("TestLog: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
	if err != nil {
		syslog(fmt.Sprintf("Error: PutItem, %s", err.Error()))
		return
	}

}
