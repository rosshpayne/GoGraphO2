package gql

import (
	"github.com/DynamoGraph/qql/lexer"
	"github.com/DynamoGraph/qql/parser"
)

func Execute(query string) {

	l := lexer.New(input)
	p := New(l)

	// *ast.RootStmt, []error)
	r, errs := p.ParseInput()

	if len(errs) > 0 {
		return errs
	}
	r.RetrievePredicates()

	result := r.Execute()

}
