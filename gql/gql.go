package gql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DynamoGraph/gql/ast"
	"github.com/DynamoGraph/gql/monitor"
	"github.com/DynamoGraph/gql/parser"
	"github.com/DynamoGraph/rdf/grmgr"
	slog "github.com/DynamoGraph/syslog"
)

var ctx context.Context
var cancel context.CancelFunc
var ctxEnd sync.WaitGroup

func syslog(s string) {
	slog.Log("gql: ", s)
}

func init() {
	fmt.Println("====================== STARTUP =====================")
	Startup()
}

func Execute(query string) {

	var (
		wpStart sync.WaitGroup
		ctxEnd  sync.WaitGroup
	)
	tstart := time.Now()
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
	syslog(fmt.Sprintf(" services started "))

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
	//
	t1 := time.Now()
	stmt.Execute(golimiter) // []pkey,sortk,ty
	t2 := time.Now()
	//
	stmt.MarshalJSON()
	//
	t3 := time.Now()
	fmt.Printf("Duration: Setup  %s  Parse  %s  Execute: %s      Output: %s\n", t0.Sub(tstart), t1.Sub(t0), t2.Sub(t1), t3.Sub(t2))
	syslog(fmt.Sprintf("Duration: Parse  %s  Execute: %s      Output: %s", t1.Sub(t0), t2.Sub(t1), t3.Sub(t2)))
	time.Sleep(2 * time.Second) // give time for monitor to empty its channel queues
	cancel()

	ctxEnd.Wait()
	fmt.Println("Exit.....")

}

func Execute_(query string) *ast.RootStmt {

	//defer Shutdown()

	golimiter := grmgr.New("execute", 66)

	t0 := time.Now()
	p := parser.New(query)
	stmt, errs := p.ParseInput()
	if len(errs) > 0 {
		panic(errs[0])
	}
	//
	t1 := time.Now()
	stmt.Execute(golimiter)
	t2 := time.Now()

	fmt.Printf("Duration:  Parse  %s  Execute: %s    \n", t1.Sub(t0), t2.Sub(t1))
	syslog(fmt.Sprintf("Duration: Parse  %s  Execute: %s ", t1.Sub(t0), t2.Sub(t1)))
	time.Sleep(2 * time.Second) // give time for monitor to empty its channel queues

	//Shutdown()

	return stmt

}

func Startup() {

	var (
		wpStart sync.WaitGroup
	)
	syslog("Startup...")
	wpStart.Add(2)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(2)
	// l := lexer.New(input)
	// p := New(l)
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel = context.WithCancel(context.Background())

	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	go monitor.PowerOn(ctx, &wpStart, &ctxEnd)

	wpStart.Wait()
	syslog(fmt.Sprintf("services started "))
}

func Shutdown() {

	syslog("Shutdown commenced...")
	cancel()

	ctxEnd.Wait()
	syslog("Shutdown...")
}
