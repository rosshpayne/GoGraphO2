package rdf

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DynamoGraph/types"
)

func TestLoadFile(t *testing.T) {

	//	f, err := os.Open("1million.rdf")
	types.SetGraph("Movies")
	f, err := os.Open("test.rdf")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	err = Load(f)
	if err != nil {
		t.Fatal(err)
	}
	//AttachMovie2Director()
	t1 := time.Now()
	fmt.Println("Duration: ", t1.Sub(t0))
	if err != nil {
		t.Fatal()
	}
	t.Log("Finished...")

	//	time.Sleep(4 * time.Second)
}

// func TestAttach(t *testing.T) {

// 	types.SetGraph("Movies")
// 	t0 := time.Now()
// 	Attach()
// 	t1 := time.Now()
// 	fmt.Println("Duration: ", t1.Sub(t0))

// 	t.Log("Finished...")

// 	//	time.Sleep(4 * time.Second)
// }
