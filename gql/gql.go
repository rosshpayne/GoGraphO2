package gql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DynamoGraph/gql/monitor"
	"github.com/DynamoGraph/gql/parser"
	"github.com/DynamoGraph/rdf/grmgr"
	slog "github.com/DynamoGraph/syslog"
)

func syslog(s string) {
	slog.Log("gql: ", s)
}

func Execute(query string) {

	var (
		wpStart sync.WaitGroup
		ctxEnd  sync.WaitGroup
	)
	wpStart.Add(2)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(2)
	// l := lexer.New(input)
	// p := New(l)
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel := context.WithCancel(context.Background())

	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	go monitor.PowerOn(ctx, &wpStart, &ctxEnd)

	wpStart.Wait()
	syslog(fmt.Sprintf(" background routines started "))

	// 	cores=
	// 	rtpercore=
	golimiter := grmgr.New("execute", 66) // cores*rtpercore)

	t0 := time.Now()
	p := parser.New(query)
	// *ast.RootStmt, []error)
	stmt, errs := p.ParseInput()
	if len(errs) > 0 {
		panic(errs[0])
	}
	fmt.Printf("doc: %s\n", stmt.String())
	stmt.Execute(golimiter) // []pkey,sortk,ty
	t1 := time.Now()
	stmt.MarshalJSON()
	t2 := time.Now()
	fmt.Printf("\nDuration: Execute: %s      Output: %s\n\n", t1.Sub(t0), t2.Sub(t1))
	syslog(fmt.Sprintf("Duration: Execute: %s      Output: %s", t1.Sub(t0), t2.Sub(t1)))
	time.Sleep(2 * time.Second) // give time for monitor to empty its channel queues
	cancel()

	ctxEnd.Wait()
	fmt.Println("Exit.....")

}

// func fetchNodeData(r db.QResult) {
// 	//
// 	// fetch data
// 	//
// 	var fr *cache.NodeCache
// 	nc, err := cache.FetchNode(r.Pkey, "A#")
// 	if err != nil {
// 		panic(err)
// 	}
// 	//
// 	// build NV - based on predicates from Stmt
// 	//
// 	var (
// 		nv     ds.ClientNV
// 		cUpred string
// 		nvName string
// 	)
// 	tyAC := cache.TypeCache.TyAttrCache
// 	//
// 	for _, t := range result {
// 		//
// 		for _, v := range stmt.Predicates {

// 			longTy, _ = cache.GetTyLongNm(t)
// 			a := tyAC[longTy+":"+v]

// 			if len(a.Ty) == 0 {
// 				// scalar
// 				if len(cUpred) > 0 {
// 					nvName = cUpred + ":" + a.Name
// 				} else {
// 					nvName = a.Name
// 				}
// 				nv = append(nv, ds.NV{Name: nvName})

// 			} else {
// 				// uid-pred : uid-pred:
// 				if len(cUpred) > 0 {
// 					cUpred += ":" + a.Name
// 				} else {
// 					cUpred = a.Name
// 				}
// 				nv = append(nv, ds.NV{Name: cUpred})
// 			}
// 		}
// 	}
// 	//
// 	// unmarshal cache into NV
// 	//
// 	err = nc.UnmarshalCache(nv)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// }
