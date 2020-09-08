package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"text/scanner"
	"time"
	"unicode"

	"github.com/DynamoGraph/db"
)

type ObjT struct {
	ty    string      // type def from Type
	value interface{} // rdf object value
}

// RDF SPO
type rdfT struct {
	Subj []byte // subject
	Pred string // predicate
	Obj  ObjT   // object
}
