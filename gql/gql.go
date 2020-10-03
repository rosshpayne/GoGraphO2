package gql

import (
	//	"github.com/DynamoGraph/gql/lexer"
	"github.com/DynamoGraph/gql/parser"
)

func Execute(query string) error {

	// l := lexer.New(input)
	// p := New(l)
	p := parser.New(query)
	// *ast.RootStmt, []error)
	stmts, errs := p.ParseInput()

	if len(errs) > 0 {
		return errs
	}

	for _, s := range stmts {

		result := r.Execute()

		for _, x := range result {
			fmt.Printf("%#v\n", x)
		}
	}

}
