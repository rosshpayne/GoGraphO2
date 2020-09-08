package main

import (
	"fmt"
	"os"
	"time"

	rdfm "github.com/DynamoGraph/rdf.m"
	slog "github.com/DynamoGraph/syslog"
)

func main() {

	f, err := os.Open("/home/ec2-user/environment/project/DynamoGraph/data/1million.rdf") // TODO: grab directory from environment variable DyGHOME
	if err != nil {
		panic(err)
	}
	t0 := time.Now()

	err = rdfm.Load(f)

	t1 := time.Now()
	if err != nil {

		fmt.Printf("Exited due to Error: %s\n", err.Error())
		slog.Log("Exited due to error: ", fmt.Sprintf("Duration:  %s ", t1.Sub(t0)))
		return
	}
	slog.Log("Main", fmt.Sprintf("Finished successfully. Duration: %s.", t1.Sub(t0)))
	fmt.Println("Main", "Finished successfully. Duration: %s.", t1.Sub(t0))

}
