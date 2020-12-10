package anmgr

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/DynamoGraph/rdf/uuid"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

const (
	LogLabel = "anmgr: "
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
	attachDoneCh = make(chan EdgeSn, 1)
}

func AttachDone(e EdgeSn) {
	attachDoneCh <- e
}
func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {
	defer wgEnd.Done()

	var ec float64

	slog.Log("anmgr: ", "Powering on...")
	wp.Done()

	lch := make(chan util.UID)

	for {

		select {

		case e := <-EdgeSnCh:

			// optionally: save entries to a file
			edges = append(edges, e)
			ec++
			if math.Mod(ec, 100) == 0 {
				slog.Log(LogLabel, fmt.Sprintf("Edge Count. %g", ec))
			}

		case <-AttachCh:

			attachDone = make(attachDoneMap)
			attachRunning = make(attachRunningMap)
			var (
				dontrun bool
			)
			if len(edges) > 0 {
				//
				for len(attachDone) < len(edges) { // 1 accounts for last currently  running attacher which has just been started
					//
					for _, e := range edges {
						// interested in unattached nodes only - checking for attach complete message for running attach routines.
						switch {
						case attachDone[e]:
							continue
						case attachRunning[e]:
							// poll for attach complete message
							select {
							case e := <-attachDoneCh:
								slog.Log(LogLabel, fmt.Sprintf("** Received on attachDoneCh.... %#v", e))
								attachDone[e] = true
								delete(attachRunning, e)
							default:
							}
							continue
						}

						dontrun = false
						if attachRunning[e] || attachDone[e] {
							continue
						}
						//
						// detect for possible concurrency issues with running attachers - for this to work we need to be aware of when attachers have finished (ie. done)
						//
						for r, _ := range attachRunning {
							// if new edge shares any edges with currently running attach jobs move onto next edge
							if e.CSn == r.CSn || e.PSn == r.CSn || e.CSn == r.PSn || e.PSn == r.PSn {
								dontrun = true
								break
							}
						}
						if dontrun {

							if len(attachDone) == len(edges)-1 {
								slog.Log(LogLabel, fmt.Sprintf("sleep "))
								time.Sleep(50 * time.Millisecond)
								break
							}
							continue
						}
						//
						// get UUIDs for rdf blank node names (SName) from uuid service
						//
						uuid.ReqCh <- uuid.Request{SName: e.CSn, RespCh: lch}
						csn := <-lch
						uuid.ReqCh <- uuid.Request{SName: e.PSn, RespCh: lch}
						psn := <-lch

						slog.Log(LogLabel, fmt.Sprintf("Run AttachNodeCh: %s  %s  %s %s", e.CSn, e.PSn, csn.String(), psn.String()))

						// send AttachNode msg. This will be throttled by rdf.Loader's limiterAttach, allowed concurrent client.AttachNode routines

						AttachNodeCh <- Edge{Cuid: csn, Puid: psn, Sortk: e.Sortk, E: e}

						attachRunning[e] = true

					}
					//
					// wait for running attachers to complete before trying to attach nodes that "donotrun"
					//
					for i := len(attachRunning); i > 0; i-- {
						e := <-attachDoneCh
						slog.Log(LogLabel, fmt.Sprintf("**** received on attachDoneCh.... %#v", e))
						attachDone[e] = true
						delete(attachRunning, e)
					}
				}
				edges = nil
				attachDone = nil
				attachRunning = nil
			}
			// all edges joined - send end-of-data on channel

			AttachNodeCh <- Edge{Cuid: []byte("eod")}

		case <-ctx.Done():
			slog.Log("anmgr: ", "Powering down...")
			return

		}
	}
}
