package db

import (
	"fmt"
	"time"

	"github.com/DynamoGraph/event"
	"github.com/DynamoGraph/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func LogEvent(eventData event.Event) error {

	var (
		av      map[string]*dynamodb.AttributeValue
		err     error
		eventTy string
	)
	//
	return nil

	switch x := eventData.(type) {

	case event.AttachNode:

		eventTy = "AttachNode"
		av, err = dynamodbattribute.MarshalMap(x)
		if err != nil {
			return fmt.Errorf("%s: %s", "Error: failed to marshal event.AttachNode ", eventTy, err.Error())
		}

	case event.DetachNode:
		eventTy = "DetachNode"
		av, err = dynamodbattribute.MarshalMap(x)
		if err != nil {
			return fmt.Errorf("LogEvent for %s: %s", "Error: failed to marshal event.DetachNode ", eventTy, err.Error())
		}
	}
	{
		t0 := time.Now()
		ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
			TableName:              aws.String("DyGEvent"),
			Item:                   av,
			ConditionExpression:    aws.String("attribute_not_exists(EID)"),
			ReturnConsumedCapacity: aws.String("TOTAL"),
		})
		t1 := time.Now()
		syslog(fmt.Sprintf("LogEvent for %s: consumed capacity for PutItem  %s. Duration: %s", eventTy, ret.ConsumedCapacity, t1.Sub(t0)))
		if err != nil {
			return fmt.Errorf("LogEvent Error: PutItem for %s Error: %s", eventTy, err.Error())
		}
	}
	return nil
}

func LogEventSuccess(eID util.UID, duration string) error {
	return nil
	//	return UpdateEvent(eID, "C", duration)
}

func LogEventFail(eID util.UID, duration string, err error) error {
	return nil
	//	return UpdateEvent(eID, "F", duration, err)
}

func UpdateEvent(eID util.UID, status string, duration string, errEv ...error) error {

	type pKey struct {
		EID []byte
		SEQ int
	}
	return nil

	upd := expression.Set(expression.Name("Status"), expression.Value(status))
	upd = upd.Set(expression.Name("Dur"), expression.Value(duration))
	if len(errEv) > 0 {
		upd = upd.Set(expression.Name("Err"), expression.Value(errEv[0].Error()))
	}
	updC := expression.Equal(expression.Name("Status"), expression.Value("C")).Not()
	//
	expr, err := expression.NewBuilder().WithCondition(updC).WithUpdate(upd).Build()
	if err != nil {
		return newDBExprErr("UpdateEvent", "", "", err)
	}
	//
	// Marshal primary key, sortK
	//
	pkey := pKey{EID: eID, SEQ: 1}
	av, err := dynamodbattribute.MarshalMap(&pkey)
	if err != nil {
		return newDBMarshalingErr("UpdateEvent", eID.String(), "", "MarshalMap", err)
	}
	input := &dynamodb.UpdateItemInput{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
	}
	input = input.SetTableName("DyGEvent").SetReturnConsumedCapacity("TOTAL")
	//
	{
		t0 := time.Now()
		uio, err := dynSrv.UpdateItem(input)
		t1 := time.Now()
		syslog(fmt.Sprintf("UpdateEvent: consumed updateitem capacity: %s, Duration: %s\n", uio.ConsumedCapacity, t1.Sub(t0)))
		if err != nil {
			return err
		}
	}
	return nil
}
