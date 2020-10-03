package gql

import (
	"fmt"
	"testing"
)

func TestSimpleRootQuery(t *testing.T) {

	input := `{
  directors(func: eq(count(siblings), 2)) {
    name
  }
}`

	Execute(input)

}

func TestRootQuery(t *testing.T) {

	input := `{
  directors(func: gt(count(director.film), 5)) {
    totalDirectors : count(uid)
  }
}`

	Execute(input)

}
