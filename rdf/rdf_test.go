package rdf

import (
	"os"
	"testing"
)

func TestLoadFile(t *testing.T) {

	f, err := os.Open("person.rdf")
	if err != nil {
		t.Fatal(err)
	}

	err = Load(f)
	if err != nil {
		t.Fatal()
	}
	t.Log("Finished...")

	//	time.Sleep(4 * time.Second)
}
