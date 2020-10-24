package anmgr

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DynamoGraph/rdf/uuid"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

type EdgeSn struct {
	CSn   string
	PSn   string
	Sortk string
}

var edges []EdgeSn

type Edge struct {
	Cuid  []byte
	Puid  []byte
	Sortk string
	E     EdgeSn
}

type attachRunningMap map[EdgeSn]bool // set of running attachNodes
var attachRunning attachRunningMap

type attachDoneMap map[EdgeSn]bool // set of completed attachNodes
var attachDone attachDoneMap

var (
	EdgeSnCh     chan EdgeSn
	AttachCh     chan struct{}
	AttachNodeCh chan Edge
	attachDoneCh chan EdgeSn
)

func init() {
	EdgeSnCh = make(chan EdgeSn)
	AttachCh = make(chan struct{})
	AttachNodeCh = make(chan Edge)
	attachDoneCh = make(chan EdgeSn)
}

func AttachDone(e EdgeSn) {
	attachDoneCh <- e
}
func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {
	defer wgEnd.Done()

	slog.Log("anmgr: ", "Powering on...")
	wp.Done()

	lch := make(chan util.UID)

	for {

		select {

		case e := <-EdgeSnCh:

			edges = append(edges, e)
			slog.Log("anmgr ", fmt.Sprintf("received on EdgeSnCn. %#v", e))

		case <-AttachCh:

			attachDone = make(attachDoneMap)
			attachRunning = make(attachRunningMap)
			var dontrun bool
			if len(edges) > 0 {
				//
				for len(attachDone) != len(edges)-1 {
					//
					for _, e := range edges {
						dontrun = false
						slog.Log("anmgr ", fmt.Sprintf("for loop: e = %#v", e))
						if attachRunning[e] || attachDone[e] {
							slog.Log("anmgr ", fmt.Sprintf("running or done: e = %#v", e))
							continue
						}
						//
						// check if any running attachers have completed
						//
						for i := 0; i < 3; i++ {
							select {

							case e := <-attachDoneCh:

								slog.Log("anmgr ", fmt.Sprintf("** received on attachDoneCh.... %d", i))
								attachDone[e] = true
								delete(attachRunning, e)

							default:
								time.Sleep(5 * time.Millisecond)
							}

						}
						for r, _ := range attachRunning {
							// if new edge shares any edges with currently running attach jobs move onot next edge
							if e.CSn == r.CSn || e.PSn == r.CSn || e.CSn == r.PSn || e.PSn == r.PSn {
								dontrun = true
								break
							}
						}
						if dontrun {

							if len(attachDone) == len(edges)-1 {
								slog.Log("anmgr ", fmt.Sprintf("sleep "))
								time.Sleep(50 * time.Millisecond)
								// only one left to run go to top of loop
								break
							}
							continue
						}

						// get UUIDs for rdf blank node names (SName) from uuid goroutine
						uuid.ReqCh <- uuid.Request{SName: e.CSn, RespCh: lch}
						csn := <-lch
						uuid.ReqCh <- uuid.Request{SName: e.PSn, RespCh: lch}
						psn := <-lch

						slog.Log("anmgr ", fmt.Sprintf("About to run AttachNodeCh: %s  %s  %s %s", e.CSn, e.PSn, csn.String(), psn.String()))

						AttachNodeCh <- Edge{Cuid: csn, Puid: psn, Sortk: e.Sortk, E: e}

						attachRunning[e] = true
					}
					slog.Log("anmgr ", fmt.Sprintf("for loop finished %d  %d ", len(attachDone), len(edges)))
					// if len(attachDone) == len(edges)-1 {
					// 	break OuterLoop
					// }
				}
			}
			// all edges joined to respect nodesn - send end-of-data on channel
			edges = nil
			AttachNodeCh <- Edge{Cuid: []byte("eod")}

		case <-attachDoneCh:

		case <-ctx.Done():
			slog.Log("anmgr: ", "Powering down...")
			return

		}
	}
}
