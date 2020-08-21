package es

import (
	"context"
	"log"
	"strings"
	"time"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	//	esv8 "github.com/elastic/go-elasticsearch/v8"
)

func ESTest() {
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
	log.Print("About to run es.Index ....")
	t0 := time.Now()
	res, err := es.Index(
		"test", // Index name
		strings.NewReader(`{"title" : "Test33"}`), // Document body
		es.Index.WithDocumentID("1"),              // Document ID
		es.Index.WithRefresh("true"),              // Refresh
	)
	t1 := time.Now()
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	log.Print("It worked.....")
	defer res.Body.Close()

	log.Println(res, "duration: ", t1.Sub(t0))

	// => [201 Created] {"_index":"test","_type":"_doc","_id":"1" ...

	t0 = time.Now()
	req := esapi.IndexRequest{
		Index:      "test",                                   // Index name
		Body:       strings.NewReader(`{"title" : "Test2"}`), // Document body
		DocumentID: "2",                                      // Document ID
		Refresh:    "true",                                   // Refresh
	}
	t1 = time.Now()
	{

		res, err := req.Do(context.Background(), es)
		t3 := time.Now()
		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}
		defer res.Body.Close()

		log.Println(res, "duration IndexRequest: ", t1.Sub(t0), "  req.Do() ", t3.Sub(t1))
	}
	// => [200 OK] {"_index":"test","_type":"_doc","_id":"1","_version":2 ...

}
