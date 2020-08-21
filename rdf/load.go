package rdf

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/cache"
	"github.com/DynamoGraph/client"
	"github.com/DynamoGraph/db"
	"github.com/DynamoGraph/rdf/anmgr"
	"github.com/DynamoGraph/rdf/ds"
	elog "github.com/DynamoGraph/rdf/errlog"
	"github.com/DynamoGraph/rdf/grmgr"
	"github.com/DynamoGraph/rdf/reader"
	"github.com/DynamoGraph/rdf/uuid"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

const (
	// number of nodes in rdf to load in single read
	readBatchSize = 2 // prod: 20
//	processBatchSize = 2 // prod 3 (total 60 concurrent nodes )
)
const (
	I   = "I"
	F   = "F"
	S   = "S"
	Nd  = "Nd"
	SS  = "SS"
	SI  = "SI"
	SF  = "SF"
	LS  = "LS"
	LI  = "LI"
	LF  = "LF"
	LBl = "LbL"
	SBl = "SBl"
)

//
// channels
//
var verifyCh chan verifyNd
var saveCh chan []ds.NV

//
//
var errNodes ds.ErrNodes

type verifyNd struct {
	n     int
	nodes []*ds.Node
}

func syslog(s string) {
	slog.Log("rdfLoader: ", s)
}

func init() {
	errNodes = make(ds.ErrNodes)
	verifyCh = make(chan verifyNd, 2)
	saveCh = make(chan []ds.NV, 2*readBatchSize)
}

// uid PKey of the sname-UID pairs - consumed and populated by the SaveRDFNode()

func Load(f io.Reader) error { // S P O
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel := context.WithCancel(context.Background())
	//
	var (
		wpStart, wpEnd sync.WaitGroup
		ctxEnd         sync.WaitGroup
		err            error
		n              int // for loop counter
		eof            bool
	)
	//
	// sync.WorkGroups
	//
	wpStart.Add(6)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	wpEnd.Add(2)
	ctxEnd.Add(4)
	//
	// start pipeline goroutines
	//
	go verify(&wpStart, &wpEnd)
	go saveNode(&wpStart, &wpEnd)
	//
	// start autonomous goroutines
	//
	go uuid.PowerOn(ctx, &wpStart, &ctxEnd)
	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	go elog.PowerOn(ctx, &wpStart, &ctxEnd)
	go anmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	//
	// wait for processes to start
	//
	wpStart.Wait()
	syslog(fmt.Sprintf(" principle goroutines started "))
	//
	// create rdf reader
	//
	rdr, _ := reader.New(f)
	//

	var errLimitCh chan bool
	errLimitCh = make(chan bool)

	errLimitReached := func() bool {
		elog.CheckLimit(errLimitCh)
		return <-errLimitCh
	}

	for {
		//
		// make nodes
		//
		nodes := make([]*ds.Node, readBatchSize, readBatchSize)
		// assign pointers
		for i := range nodes {
			nodes[i] = new(ds.Node)
		}
		//
		// read rdf file []nodes at a time
		//
		n, eof, err = rdr.Read(nodes)
		if err != nil {
			// log error and continue to read until eof reached
			elog.Add <- fmt.Errorf("Read error: %s", err.Error())
		}
		//
		// send []nodes on pipeline to be unmarshalled and saved to db
		//
		v := verifyNd{n: n, nodes: nodes}
		syslog("Send node batch on channel verifyCh")
		verifyCh <- v

		// check if too many errors
		if errLimitReached() {
			break
		}
		//
		// exit when
		//
		if n < len(nodes) || eof {
			break
		}
	}

	syslog("close verify channel")
	close(verifyCh)
	//go processErrors()
	wpEnd.Wait()

	// cancel context (close Done channel) on all autonomous goroutines
	lcherr := make(chan elog.ErrorS)
	elog.ListReqCh <- lcherr
	syslog("11.....")
	ex := <-lcherr
	syslog(fmt.Sprintf("22.....error cnt: %d", len(ex)))

	for _, e := range ex {
		fmt.Println("Errors: ", e.Error())
	}

	cancel()

	ctxEnd.Wait()
	syslog("loader exists.....")

	return err
}

func verify(wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup) { //, wg *sync.WaitGroup) {

	defer wpEnd.Done()
	defer close(saveCh)
	// sync verify's internal goroutines

	wpStart.Done()

	// waitgroups
	var wg sync.WaitGroup
	//
	// concurrent settings for goroutines
	//
	//	unmarshalTrg := grmgr.Trigger{R: routine, C: 5, Ch: make(chan struct{})}
	limiter := grmgr.New("unmarshall", 5)

	syslog("verify started....")
	// the loop will terminate on close of channel
	// each goroutine will finish when it completes - none left hanging
	//	var c int
	for nodes_ := range verifyCh {
		//		c++
		syslog(fmt.Sprintf("read from verifyCH : nodes_.n = %d", nodes_.n))

		nodes := nodes_.nodes

		// unmarshal (& validate) each node in its own goroutine
		for i := 0; i < nodes_.n; i++ {

			if len(nodes[i].Lines) == 0 {
				break
			}
			ii := i
			ty, err := getType(nodes[ii])
			if err != nil {
				elog.Add <- err
			}
			// first pipeline func. Passes NV data to saveCh and then to database.
			//	slog.Log("verify: ", fmt.Sprintf("Pass to unmarshal... %d %#v", i, nodes[ii]))
			limiter.Ask()
			<-limiter.RespCh()

			wg.Add(1)
			go unmarshalRDF(nodes[ii], ty, &wg, limiter)

		}
	}
	wg.Wait()

}

// type TyAttrD struct {
// 	Name string // Attribute Identfier
// 	DT   string // Attribute Data - derived. ??
// 	C    string // Attribute short identifier
// 	Ty   string // For abstract attribute types the type it respresents e.g "Person"
// 	P    string // data partition (aka shard) containing attribute
// 	N    bool   // true: nullable (attribute may not exist) false: not nullable
// 	Pg   bool   // true: propagate scalar data to parent
// }
func unmarshalRDF(node *ds.Node, ty blk.TyAttrBlock, wg *sync.WaitGroup, lmtr grmgr.Limiter) {
	defer wg.Done()

	genSortK := func(ty blk.TyAttrD) string {
		var s strings.Builder

		if ty.DT == "Nd" {
			if len(ty.P) == 0 {
				s.WriteString("G#:")
			} else {
				s.WriteString(ty.P)
				s.WriteString("#G#:")
			}
			s.WriteString(ty.C)
		} else {
			s.WriteString(ty.P)
			s.WriteString("#:")
			s.WriteString(ty.C)
		}
		return s.String()
	}

	slog.Log("unmarshalRDF: ", "Entered unmarshalRDF. ")

	lmtr.StartR()
	defer lmtr.EndR()

	// accumulate predicate (spo) n.Obj ect values in the following map
	type mergedRDF struct {
		value interface{}
		name  string
		dt    string
		sortk string
		c     string // type attribute short name
	}
	var attr map[string]*mergedRDF
	attr = make(map[string]*mergedRDF)
	//
	var nv []ds.NV // AttributName-Dynamo-Value

	// find predicate in Lines matching type attribute name in ty'

	for _, v := range ty {
		var found bool
		//	fmt.Println("node.Lines: ", len(node.Lines), node.Lines)

		for _, n := range node.Lines {

			if !strings.EqualFold(v.Name, n.Pred) {
				continue
			}
			found = true

			switch v.DT {
			case I:
				// check n.Obj ect can be coverted to int

				i, err := strconv.Atoi(n.Obj)
				if err != nil {
					err := fmt.Errorf("expected Integer %s ", n.Obj)
					node.Err = append(node.Err, err)
					continue
				}
				attr[v.Name] = &mergedRDF{value: i, dt: v.DT}

			case F:
				// check n.Obj ect can be converted to float
				attr[v.Name] = &mergedRDF{value: n.Obj, dt: v.DT}
				//attr[v.Name] = n.Obj // keep float as string as Dynamodb transport it as string

			case S:
				// check n.Obj ect can be converted to float

				//attr[v.Name] = n.Obj
				attr[v.Name] = &mergedRDF{value: n.Obj, dt: v.DT}

			case SS:

				if a, ok := attr[v.Name]; !ok {
					ss := make([]string, 1)
					ss[0] = n.Obj
					attr[v.Name] = &mergedRDF{value: ss, dt: v.DT, c: v.C}
				} else {
					if ss, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with SS type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						syslog(fmt.Sprintf("Add to SS . [%s]", n.Obj))
						ss = append(ss, n.Obj)
						attr[v.Name].value = ss
					}
				}

			// case SBl:
			// case SB:
			// case LBl:
			// case LB:

			case LS:
				if a, ok := attr[v.Name]; !ok {
					ss := make([]string, 1)
					ss[0] = n.Obj
					attr[v.Name] = &mergedRDF{value: ss, dt: v.DT, c: v.C}
					//	attr[v.Name] = ss
				} else {
					if ls, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with SS type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						ls = append(ls, n.Obj)
						attr[v.Name].value = ls
					}
				}

			case LI:
				if a, ok := attr[v.Name]; !ok {
					ss := make([]int, 1)
					i, err := strconv.Atoi(n.Obj)
					if err != nil {
						err := fmt.Errorf("expected Integer %s", n.Obj)
						node.Err = append(node.Err, err)
						continue
					}
					ss[0] = i // n.Obj  int
					//attr[v.Name] = ss
					attr[v.Name] = &mergedRDF{value: ss, dt: v.DT}
				} else {
					if li, ok := a.value.([]int); !ok {
						err := fmt.Errorf("Conflict with SS type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						i, err := strconv.Atoi(n.Obj)
						if err != nil {
							err := fmt.Errorf("expected Integer %s", n.Obj)
							node.Err = append(node.Err, err)
							continue
						}
						li = append(li, i)
						attr[v.Name].value = li
					}
				}

			case Nd:

				// need to convert n.Obj  value of SName to UID
				if a, ok := attr[v.Name]; !ok {
					ss := make([]string, 1)
					ss[0] = n.Obj
					//attr[v.Name] = ss
					attr[v.Name] = &mergedRDF{value: ss, dt: v.DT}
					//addEdgesCh<-
				} else {
					if nd, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with SS type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						nd = append(nd, n.Obj)
						attr[v.Name].value = nd
					}
				}

				//	addEdgesCh<-
			}
			//
			// generate sortk key
			//
			at := attr[v.Name]
			at.sortk = genSortK(v)
		}
		//
		//
		//
		if !found {
			if !v.N && v.DT != "Nd" {
				err := fmt.Errorf("Not null type attribute %q must be specified in node %s", v.Name, node.ID)
				node.Err = append(node.Err, err)
			}
		}
		if len(node.Err) > 0 {
			slog.Log("unmarshalRDF: ", fmt.Sprintf("return with %d errors. First error:  %s", len(node.Err), node.Err[0].Error()))
			elog.AddBatch <- node.Err
			return
		}

	}
	//
	// unmarshal attr into NV -except Nd types, handle in next for
	//
	var addTy = true
	for k, v := range attr {
		//
		if v.dt == Nd {
			continue
		}
		if addTy {
			// add type of node to NV
			e := ds.NV{Sortk: "A#T", SName: node.ID, Value: node.TyName, DT: "ty"}
			nv = append(nv, e)
			addTy = false
		}
		e := ds.NV{Sortk: v.sortk, Name: k, SName: node.ID, Value: v.value, DT: v.dt, C: v.c}
		nv = append(nv, e)
	}
	//
	// check all uid-predicate types (DT="Nd") have an NV entry - as this simplies later processing if one is guaranteed to exist even if not originally defined in RDF file
	//
	for _, v := range ty {
		if v.DT == Nd {
			// if _, ok := attr[v.Name]; ok {
			// 	continue
			// }
			// create empty item
			value := []string{"__"}
			e := ds.NV{Sortk: genSortK(v), Name: v.Name, SName: "__", Value: value, DT: Nd}
			nv = append(nv, e)
		}
	}
	//
	//  build list of attachNodes (in uuid pkg) to be processed after all other tuples have been added to db
	//
	for _, v := range attr {
		if v.dt == "Nd" {
			x := v.value.([]string)
			for _, s := range x {
				anmgr.EdgeSnCh <- anmgr.EdgeSn{CSn: node.ID, PSn: s, Sortk: v.sortk}
			}
		}
	}
	//
	// pass NV onto database goroutine if no errors detected
	//
	if len(node.Err) == 0 {
		slog.Log("unmarshalRDF: ", fmt.Sprintf("send on saveCh: nv: %#v", nv))
		saveCh <- nv
	} else {
		node.Lines = nil
		errNodes[node.ID] = node
	}
	//
	slog.Log("unmarshalRDF: ", "Exit  unmarshalRDF. ")
}

func saveNode(wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup) {

	defer wpEnd.Done()

	var wg sync.WaitGroup

	syslog("saveNode started......")
	wpStart.Done()
	syslog("define saveNode limiter......")
	//
	// define goroutine limiters
	//
	limiterSave := grmgr.New("saveNode", 2)

	// upto 5 concurrent save routines
	var c int
	for nv := range saveCh {
		c++

		//	slog.Log("saveNode: ", fmt.Sprintf("read from saveCH channel %d ", c))
		limiterSave.Ask()
		<-limiterSave.RespCh()

		//	slog.Log("saveNode: ", "limiter has ACK and will start goroutine...")

		wg.Add(1)
		go db.SaveRDFNode(nv, &wg, limiterSave)

	}
	//	syslog("saveNode  waiting on saveRDFNode routines to finish")
	wg.Wait()
	syslog("saveNode finished waiting...exiting")

	limiterAttach := grmgr.New("nodeAttach", 6)
	// retrieve attach node pairs from uuid.edges via channel uuid.AttachNodeCh
	anmgr.AttachCh <- struct{}{}
	c = 0
	for {
		c++
		e := <-anmgr.AttachNodeCh
		if string(e.Cuid) == "eol" {
			break
		}
		slog.Log("attachNode: ", fmt.Sprintf("read from AttachNodeCh channel %d now ASK limiter", c))

		limiterAttach.Ask()
		<-limiterAttach.RespCh()

		//slog.Log("attachNode: ", "limiter has ACK and will start goroutine...")

		wg.Add(1)
		slog.Log("AttachNode: ", fmt.Sprintf("goroutine about to start %d cUID,pUID   %s  %s  ", c, util.UID(e.Cuid).String(), util.UID(e.Puid).String()))
		go client.AttachNode(util.UID(e.Cuid), util.UID(e.Puid), e.Sortk, e.E, &wg, limiterAttach)
	}
	syslog("saveNode  waiting on AttachNode to finish")
	wg.Wait()
	syslog("saveNode finished waiting...exiting")
}

func getType(node *ds.Node) (blk.TyAttrBlock, error) {

	// TODO - replace with goroutine + channel req/resp

	type loc struct {
		sync.Mutex
	}
	var ll loc
	syslog(".  getType..")

	// is there a type defined
	if len(node.TyName) == 0 {
		node.Err = append(node.Err, fmt.Errorf("No type defined for %s", node.ID))
	}
	syslog(fmt.Sprintf("node.TyName : [%s]", node.TyName))
	ll.Lock()
	ty, err := cache.FetchType(node.TyName)
	ll.Unlock()
	if err != nil {
		return nil, err
	}
	return ty, nil
}
