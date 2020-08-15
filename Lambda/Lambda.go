package main

import (
	"fmt"
	_ "log"

	//"github.com/aws/aws-lambda-go/lambda/db"
	"github.com/DynamoGraph/Lambda/db"
	blk "github.com/DynamoGraph/block"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/aws/aws-lambda-go/lambdacontext"
)

// type TyAttrD struct {
// 	Name string // Attribute Identfier
// 	DT   string // Attribute Data - derived. ??
// 	C    string // Attribute short identifier
// 	Ty   string // For abstract attribute types the type it respresents e.g "Person"
// 	P    string // data partition (aka shard) containing attribute
// 	N    bool   // true: nullable (attribute may not exist) false: not nullable
// }

// type TyAttrBlock []TyAttrD

// Event item that is put into Event table for PCD event which is a superset of other events.
// EID  []byte
// SEQ  int
// CDI  string
// TyTy string
// TyDT string
// TyC  string
// TyN  bool
// PUID []byte
// TUID []byte
// SK   string // sortk
// N    float64
// S    string
// B    []byte

// cdi struct used to transfer data from event table to function that actions event
//  actions take what they need from this struct to perform their work
type cdi struct {
	//UIDs [][]byte // alternative input but more problematic as need to treat as Set or List
	eUID []byte // event id - for debug puposes only
	cUID []byte // child node
	pUID []byte // parent node
	tUID []byte // target UID for attaching child scalar values
	// flattened blk.TyAttrD type
	TyTy string
	TyDT string
	TyC  string
	TyP  string
	//
	SortK string // identifies parent uPred which is the source of the parent-child edge
	// pass data to operation function
	N   float64
	S   string
	B   []byte
	Bl  []bool
	SS  []string
	SN  []float64
	SBl []bool
	//
}

// type operation struct {
// 	data cdi
// 	f    func(*cdi) error
// }

//  func that implements action
type opfunc func(args *cdi) error

// Insert
// If attribute type is SS, IS, FS, BlS and its type attribute ix is set to x, expand set values to their own items
//    set SortK value to "Z%<attr>%<value>. Z attributes are never queried. A->S are shard values. A is scalar
//    set. P attribute to attribute name
func handleRequest(e events.DynamoDBEvent) {

	// var (
	// 	wg sync.WaitGroup
	// )

	// Note: AttachNode is now part of client package, that initiates scalar propagation.
	// actionAttachNode := func(cUID []byte, pUID []byte, sortK string) {
	// 	// TODO handle sync issues?
	// 	err := db.AttachNode(cUID, pUID, sortK)
	// 	if err != nil {
	// 		syslog("Error in event processing. Action: AttachNode, EID: %s, Error: %s", eUID, err.Error())
	// 	}
	// }
	//
	// *** add propagateChildData
	//actionPropagateChildData := func(i int, pUID util.UID, sortK string, tUID util.UID, tyDT string, tyC string, tyTy string, tyP string, Value interface{}) {
	actionPropagateChildData := func(a *cdi) error {
		//
		// from event data create a TyAttrD
		t := blk.TyAttrD{DT: a.TyDT, C: a.TyC, Ty: a.TyTy, P: a.TyP}
		var value interface{}
		switch a.TyDT {
		case "I", "F":
			value = a.N
		case "S":
			value = a.S
		case "B":
			value = a.B
		case "DT":
			value = a.S
		}

		err := db.PropagateChildData(t, a.pUID, a.SortK, a.tUID, value)
		if err != nil {
			return err
		}
		return nil
	}
	// actionDetachNode := func(a cdi) error { //func(cUID []byte, pUID []byte, pTy string, sortK string) {
	// 	// TODO handle locking
	// 	err := db.DetachNode(cUID, pUID, pTyc, ty)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// actionIndex := func(a cdi) error { //func(cUID []byte, pUID []byte, pTy string, sortK string) {
	// 	err := db.IndexMultiValueAttr(cUID, pTyc)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// actionFullText := func(a cdi) error { //func(cUID []byte, pUID []byte, pTy string, sortK string) {
	// 	_, err := db.GetStringValue(cUID, pTyc)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	// es.AddEntry(s, cUID)
	// 	// if err != nil {
	// 	// 	panic(err)
	// 	// }
	// }

	// actionUpdateValue := func(a cdi) error { //func(cUID []byte, pUID []byte, pTy string, sortK string) {
	// 	// propagate child attribute defined by pTyc to all parents
	// 	err := db.UpdateValue(cUID, pTyc)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// actionUpdateType := func(a cdi) error { //func(cUID []byte, pUID []byte, pTy string, sortK string) {
	// 	// pass msg via channel to all goroutines that cache type data - this will force them to requery the type data.
	// 	return nil
	// }

	for _, record := range e.Records {

		if record.EventName == "INSERT" {
			fmt.Printf("Processing request data for event ID %s, type %s.\n", record.EventID, record.EventName)
			var f opfunc
			a := &cdi{}
			//wg.Add(1)
			// Print new values for attributes of type String
			for name, value := range record.Change.NewImage { // map[string(<attrName>)]events.DynamoDBAttributeValue
				switch name {
				case "CDI": // DyG change type

					switch value.String() {
					case "PCD":
						f = actionPropagateChildData
						// case "RE":
						// 	f = actionPropagateChildData
						// case "ND":
						// 	f = actionDettachNode
						// case "UV":
						// 	f = actionUpdateValue
						// case "UT":
						// 	f = actionUpdateType
						// case "I":
						// 	f = actionIndex
						// case "FT":
						// 	f = actionFullText
						// default:
					}

				case "CUID":
					a.cUID = value.Binary() // child UID
				case "PUID":
					a.pUID = value.Binary() // child UID
				case "TUID":
					a.tUID = value.Binary() // target UID
				case "SK": // SortK of parent uid-predicate e.g A#G#:S. Defines the uid-pred.
					a.SortK = value.String()
				case "TyTy": // type of child - scalars to propagate
					a.TyTy = value.String()
				case "TyC": // type of child - scalars to propagate
					a.TyC = value.String()
				case "TyP": // type of child - scalars to propagate
					a.TyP = value.String()
				case "TyDT": // type of child - scalars to propagate
					a.TyDT = value.String()
				case "N": // type of child - scalars to propagate
					a.N, _ = value.Float()
					// if err != nil { // TODO: what about errors??
					// 	return err
					// }
				case "S": // type of child - scalars to propagate
					a.S = value.String()
				case "B": // type of child - scalars to propagate
					a.B = value.Binary()

				}
			}
			// Do the action - upto some configurable concurrent number //TODO -  throttle goroutines
			f(a)
		}
	}
	//wg.Wait()

}

func main() {
	lambda.Start(handleRequest)
}
