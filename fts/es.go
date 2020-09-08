package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/DynamoGraph/util"
	esv7 "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	//	esv8 "github.com/elastic/go-elasticsearch/v8"

	//"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	//	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

var dynSrv *dynamodb.DynamoDB

func init() {
	// establish dynamodb service
	dynamodbSrv := func() *dynamodb.DynamoDB {
		sess, _ := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1"),
		})

		return dynamodb.New(sess, aws.NewConfig())
	}

	dynSrv = dynamodbSrv()

}

func main() {
	cfg := esv7.Config{
		Addresses: []string{
			"http://ip-172-31-18-75.ec2.internal:9200",
		},
		// ...
	}

	es, err := esv7.NewClient(cfg)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(esv7.Version)

	log.Println(es.Info())

	err = LoadFTSIndex(es)
	if err != nil {
		fmt.Println(err)
	}
}

func LoadFTSIndex(es *esv7.Client) error {

	var (
		first  bool
		err    error
		result *dynamodb.ScanOutput
		input  *dynamodb.ScanInput
		t0, t1 time.Time
	)
	type itemT struct {
		P    string
		S    string
		PKey []byte
	}
	type esDocT struct {
		P string
		S string
	}
	var esDoc esDocT
	for {

		if !first {
			input = &dynamodb.ScanInput{
				Limit: aws.Int64(150),
			}
		} else {
			input = &dynamodb.ScanInput{
				ExclusiveStartKey: result.LastEvaluatedKey,
				Limit:             aws.Int64(150),
			}
		}
		input = input.SetTableName("DyGraph").SetIndexName("P_S").SetReturnConsumedCapacity("TOTAL")
		//
		result, err = dynSrv.Scan(input)
		if err != nil {
			return err
		}
		fmt.Println(result.ConsumedCapacity)
		//
		if int(*result.Count) == 0 {
			//newDBNoItemFound(rt string, pk string, sk string, api string, err error)
			return err
		}
		var item itemT
		fmt.Println("LastEvaluatedKey: ", result.LastEvaluatedKey)
		for i, v := range result.Items {

			err = dynamodbattribute.UnmarshalMap(v, &item)
			if err != nil {
				fmt.Println("Got error unmarshalling:")
				fmt.Println(err.Error())
				return err
			}
			fmt.Printf("%d  %s %s %q\n", i, item.P, item.S, util.UID(item.PKey).String())
		}
		if len(result.LastEvaluatedKey) == 0 {
			fmt.Println("empty lastEvaluatedKEy.....")
			break
		}
		//
		esDoc.P = item.P
		esDoc.S = item.S

		esdoc, err := json.Marshal(&esDoc)
		if err != nil {
			return err
		}
		t0 = time.Now()
		req := esapi.IndexRequest{
			Index:      "DyG",                        // Index name
			Body:       bytes.NewReader(esdoc),       // Document body
			DocumentID: util.UID(item.PKey).String(), // Document ID
			Refresh:    "true",                       // Refresh
		}
		t1 = time.Now()
		{

			res, err := req.Do(context.Background(), es)
			t3 := time.Now()
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			//defer res.Body.Close()

			log.Println(res, "duration IndexRequest: ", t1.Sub(t0), "  req.Do() ", t3.Sub(t1))

			res.Body.Close()
		}

	}
	return nil

}
