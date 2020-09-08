package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
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
		t1, t3 time.Time
		c      int
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
			fmt.Println("EOF")
			break
		}
		var item itemT
		//fmt.Println("LastEvaluatedKey: ", result.LastEvaluatedKey)
		for _, v := range result.Items {

			err = dynamodbattribute.UnmarshalMap(v, &item)
			if err != nil {
				fmt.Println("Got error unmarshalling:")
				fmt.Println(err.Error())
				return err
			}
			//fmt.Printf("%d  %s %s %q\n", i, item.P, item.S, util.UID(item.PKey).ToString())

			if len(result.LastEvaluatedKey) == 0 {
				fmt.Println("empty lastEvaluatedKEy.....------------------------------------------------------")
				break
			}
			//
			esDoc.P = item.P
			esDoc.S = item.S

			esdoc, err := json.Marshal(&esDoc)
			if err != nil {
				return err
			}
			//fmt.Println("save to ES: docId: ", util.UID(item.PKey).ToString())
			req := esapi.IndexRequest{
				Index:      "dyg",                          // Index name
				Body:       bytes.NewReader(esdoc),         // Document body
				DocumentID: util.UID(item.PKey).ToString(), // Document ID
				Refresh:    "true",                         // Refresh
			}
			t1 = time.Now()
			{
				res, err := req.Do(context.Background(), es)
				t3 = time.Now()
				if err != nil {
					log.Fatalf("Error getting response: %s", err)
				}
				//defer res.Body.Close()
				if res.StatusCode != 200 {
					log.Fatal("Bad response: %v", res)
				}
				c++
				if math.Mod(float64(c), 50.0) == 0 {
					log.Printf("%d   Duration: %s\n", c, t3.Sub(t1))
				}
				res.Body.Close()

			}
		}

	}
	return nil

}
