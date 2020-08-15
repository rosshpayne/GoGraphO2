package uuid

import (
	"context"
	"sync"

	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

type ndAlias = string
type nodeMap map[ndAlias]util.UID

var (
	nodeUID nodeMap
	ReqCh   chan Request
	SaveCh  chan Key
	RespCh  chan util.UID
)

func init() {
	// maps
	nodeUID = make(nodeMap)
	// channels
	ReqCh = make(chan Request)
	SaveCh = make(chan Key)
	RespCh = make(chan util.UID)

}

type Request struct {
	SName  ndAlias
	RespCh chan util.UID
}

type Key struct {
	SName ndAlias
	UID   util.UID
}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {
	defer wgEnd.Done()
	var (
		err error
		ok  bool
		req Request
		uid util.UID
	)

	slog.Log("rdfuuid: ", "Powering on...")
	wp.Done()

	for {

		select {

		case req = <-ReqCh:

			if uid, ok = nodeUID[req.SName]; !ok {
				if req.SName == "__" {
					uid = []byte(req.SName)
				} else {
					uid, err = util.MakeUID()
					if err != nil {
						panic(err) //TODO - handle errors somehow
					}
				}
				nodeUID[req.SName] = uid
			}

			req.RespCh <- uid

		case <-ctx.Done():

			slog.Log("rdfuuid: ", "Powering down...")
			return

		}

		// check for context close channel in separate select - to keep concurrent requests from UUID services and context close separate

		// select {
		// case <-ctx.Done():
		// 	slog.Log("rdfuuid: ", "Powering down...")
		// 	return
		// default:
		// }
	}
}
