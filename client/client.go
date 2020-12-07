package client

import (
	//"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	blk "github.com/DynamoGraph/block"
	gerr "github.com/DynamoGraph/dygerror"

	"github.com/DynamoGraph/cache"
	"github.com/DynamoGraph/db"
	"github.com/DynamoGraph/ds"
	"github.com/DynamoGraph/event"
	mon "github.com/DynamoGraph/gql/monitor"
	"github.com/DynamoGraph/rdf/anmgr"
	"github.com/DynamoGraph/rdf/errlog"
	"github.com/DynamoGraph/rdf/grmgr"
	"github.com/DynamoGraph/types"
	//	"github.com/DynamoGraph/rdf/uuid"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

const (
	logid = "AttachNode"
)

func UpdateValue(cUID util.UID, sortK string) error {
	// for update node predicate (sortk)
	// 1. perform cache update first.
	// 2. synchronous update to dynamo plus add stream CDI
	// 4. streams process: to each parent of cUID propagate value.(Streams api: propagateValue(pUID, sortk, v interface{}).

	// for AttachNode
	// for each child scalar create a CDI triggering api propagateValue(pUID, sortk, v interface{}).
	return nil
}

func GetStringValue(cUID util.UID, sortK string) (string, error) { return "", nil }

func IndexMultiValueAttr(cUID util.UID, sortK string) error { return nil }

// sortK is parent's uid-pred to attach child node too. E.g. G#:S (sibling) or G#:F (friend) or A#G#:F It is the parent's attribute to attach the child node.
// pTy is child type i.e. "Person". This could be derived from child's node cache data.

func AttachNode(cUID, pUID util.UID, sortK string, e_ anmgr.EdgeSn, wg_ *sync.WaitGroup, lmtr *grmgr.Limiter) { // pTy string) error { // TODO: do I need pTy (parent Ty). They can be derived from node data. Child not must attach to parent attribute of same type
	//
	// update db only (cached copies of node are not updated) to reflect child node attached to parent. This involves
	// 1. append chid UID to the associated parent uid-predicate, parent e.g. sortk A#G#:S
	// 2. propagate child scalar data to associated uid-predicate (parent's 'G' type) G#:S#:A etc..
	//
	defer anmgr.AttachDone(e_)
	defer wg_.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	type chPayload struct {
		tUID   util.UID
		itemId int
		nd     *cache.NodeCache // pass locked nodecache
		pTy    blk.TyAttrBlock
	}

	var (
		eID              util.UID
		pnd              *cache.NodeCache
		cTyName, pTyName string
		ok               bool
		err              error
		wg               sync.WaitGroup
	)

	syslog := func(s string) {
		slog.Log("AttachNode: ", s)
	}
	gc := cache.NewCache()
	//
	// log Event via defer
	//
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				event.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event
			} else {
				event.LogEventSuccess(eID, t1.Sub(t0).String())
			}
		}
	}()()

	syslog(fmt.Sprintf(" About to join cUID --> pUID       %s -->  %s  %s", util.UID(cUID).String(), util.UID(pUID).String(), sortK))

	//
	// this API deals only in UID that are known to exist - hence NodeExists() not necessary
	//
	// if ok, err := db.NodeExists(cUID); !ok {
	// 	if err == nil {
	// 		return addErr(fmt.Errorf("Child node UUID %q does not exist:", cUID))
	// 	} else {
	// 		return addErr(fmt.Errorf("Error in validating child node %w", err))
	// 	}
	// }
	// if ok, err := db.NodeExists(pUID, sortK); !ok {
	// 	if err == nil {
	// 		return addErr(fmt.Errorf("Parent node and/or attachment predicate for UUID %q does not exist", pUID))
	// 	} else {
	// 		return addErr(fmt.Errorf("Error in validating parent node %w", err))
	// 	}
	// }
	// create channels used to pass target UID for propagation and errors
	xch := make(chan chPayload, 1)
	defer close(xch)
	//
	// NOOP condition aka CEG - Concurrent event gatekeeper. Add edge only if it doesn't already exist (in one atomic unit) that can be used to protect against identical concurrent (or otherwise) attachnode events.
	//
	// TODO: fix bugs in edgeExists algorithm - see bug list
	if ok, err := db.EdgeExists(cUID, pUID, sortK, db.ADD); ok {
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			errlog.Add(logid, err)
		} else {
			errlog.Add(logid, fmt.Errorf("AttachNode  db.EdgeExists errored: %w ", err))
		}
		return
	}
	//
	// log Event
	//
	// going straight to db is safe provided its part of a FetchNode lock and all updates to the "R" predicate are performed within the FetchNode lock.
	ev := event.AttachNode{CID: cUID, PID: pUID, SK: sortK}
	//eID, err = eventNew(ev)
	eID, err = event.New(ev)
	if err != nil {
		return
	}
	//
	wg.Add(1)
	var childErr error
	//
	go func() {
		defer wg.Done()
		//
		// Grab child scalar data (sortk: A#A#) and lock child node. Unlocked in UnmarshalCache and defer.(?? no need for cUID lock after Unmarshal - I think?)  ALL SCALARS SHOUD BEGIN WITH sortk "A#"
		// A node may not have any scalar values (its a connecting node in that case), but there should always be a A#A#T item defined which defines the type of the node
		//
		cnd, err := gc.FetchForUpdate(cUID, "A#A#")
		defer cnd.Unlock("ON cUID for AttachNode second goroutine..") // note unmarshalCache nolonger release the lock
		// testing: see what happens with an error
		// if cUID.String() == "66PNdV1TSKOpDRlO71+Aow==" {
		// 	err = db.NewDBNoItemFound("FetchNode", cUID.String(), "", "Query")
		// }

		if err != nil {
			errlog.Add(logid, fmt.Errorf("Error fetching child scalar data: %w", err))
			childErr = err
			return
		}
		//
		// get type of child node from A#T sortk e.g "Person"
		//
		if cTyName, ok = cnd.GetType(); !ok {
			errlog.Add(logid, cache.NoNodeTypeDefinedErr)
			return
		}
		//
		// get type details from type table for child node
		//
		var cty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
		if cty, err = types.FetchType(cTyName); err != nil {
			errlog.Add(logid, err)
			return
		}
		//
		//***************  wait for payload from cocurrent routine ****************
		//
		var payload chPayload
		// prevent panic on closed channel by using bool test on channel.
		if payload, ok = <-xch; !ok {
			return
		}
		syslog(fmt.Sprintf("gr1 Payload received %#v", payload))
		tUID := payload.tUID
		pnd = payload.nd
		//defer pnd.Unlock()
		id := payload.itemId
		pty := payload.pTy // parent type
		if tUID == nil {
			//panic(fmt.Errorf("errored: target UID is nil for  cuid: %s   pUid: %s", cUID, pUID))
			errlog.Add(logid, fmt.Errorf("Received on channel: target UID of nil, cuid: %s   pUid: %s  sortK: %s", cUID, pUID, sortK))
			return
		}
		//
		// build NVclient based on Type info - either all scalar types or only those  declared in IncP attruibte for the attachment type define in sortk
		//
		var cnv ds.ClientNV
		//
		// find attachment data type from sortK eg. A#G#:S
		// here S is simply the abreviation for the Ty field which defines the child type  e.g 	"Person"
		//
		s := strings.LastIndex(sortK, "#")
		attachPoint := sortK[s+2:]
		var found bool
		for _, v := range pty {
			if v.C == attachPoint {
				found = true
				//
				//  attachment point attribute (parent) found. the attribute's type must match the child node type. // TODO: implement check
				//
				// is a IncP defined in the type definition. This will define the child attributes to propagate (short names used).
				// Note: to support has() all nullable (type attribute N = true) must be propagated
				//
				if v.Ty != cTyName {
					panic(fmt.Errorf("Parent node attachpoint does not match child type")) //TODO: replace panic with error message
				}

				//if len(v.IncP) > 0 {

				// 	for _, ps := range v.IncP {
				// 		var found bool
				// 		for _, cs := range cty {
				// 			if cs.C == ps {
				// 				switch cs.DT {
				// 				case "I", "F", "Bl", "S", "DT":
				// 				}
				// 				// found assoicated child scalar attribute
				// 				found = true
				// 				cnv = append(cnv, &ds.NV{Name: cs.Name})
				// 			}
				// 		}
				// 		if !found {
				// 			errch <- fmt.Errorf(fmt.Sprintf("AttachNode: Child scalar attribute not found based on parent IncP value, %q", ps))
				// 			return
				// 		}
				// 	}
				// 	//
				// 	// propagate all nullable attributes if not already included in IncP specification. Will use XBl data to determine if attribute exists in child node for has().
				// 	//
				// 	included := func(name string) bool {
				// 		for _, v := range cnv {
				// 			if v.Name == name {
				// 				return true
				// 			}
				// 		}
				// 		return false
				// 	}
				// 	//
				// 	for _, cs := range cty {
				// 		fmt.Println("XXXXX1: include this nullable attribute: ", cs.Name, cs.N)
				// 		if cs.N {
				// 			// include in cnv if not already present
				// 			if !included(cs.Name) {
				// 				switch v.DT {
				// 				// scalar types to be propagated
				// 				case "I", "F", "Bl", "S", "DT": //TODO: these attributes should belong to pUpred type only. Can a node be made up of more than one type? Pesuming at this stage only 1, so all scalars are relevant.
				// 					fmt.Println("XXXXX2: include this nullable attribute: ", cs.Name)
				// 					cnv = append(cnv, &ds.NV{Name: cs.Name})
				// 				}

				// 			}
				// 		}
				// 	}

				// } else {

				// grab all scalars from child type if the attribute has propagaton enabled or the attribute is nullable (meaning it may or may not be defined)
				// we need to propagate not nulls to support the has() as its the only to know if its defined for the child as the XF(?) attribute will be true if its defined or false if not.
				for _, v := range cty {
					switch v.DT {

					case "I", "F", "Bl", "S", "DT": //TODO: these attributes should belong to pUpred type only. Can a node be made up of more than one type? Pesuming at this stage only 1, so all scalars are relevant.
						if v.Pg || v.N {
							nv := &ds.NV{Name: v.Name}
							cnv = append(cnv, nv)
						}
					}
				}
				//	}
			}
		}
		if !found {
			panic(fmt.Errorf("Attachmment predicate %q not round in parent", attachPoint)) //TODO - handle as error
		}

		if len(cnv) > 0 {
			//
			// copy cache data into cnv and unlock child node.
			//
			err = cnd.UnmarshalCache(cnv)
			if err != nil {
				errlog.Add(logid, fmt.Errorf("AttachNode (child node): Unmarshal error : %s", err))
				return
			}

			//
			// ConfigureUpred() has primed the target propagation block with cUID and XF Inuse flag. Ready for propagation of Scalar data.
			// lock pUID if it is the target of the data propagation.
			// for overflow blocks the entry in the Nd of the uid-pred is set to InUse which syncs access.

			for _, t := range cty {

				for _, v := range cnv {

					if t.Name == v.Name { //&& v.Value != nil {

						id, err = db.PropagateChildData(t, pUID, sortK, tUID, id, v.Value)

						if err != nil {

							if errors.Is(err, db.ErrAttributeDoesNotExist) {

								id, err = db.InitialisePropagationItem(t, pUID, sortK, tUID, id)

								if err != nil {
									errlog.Add(logid, fmt.Errorf("AttachNode: error in PropagateChildData %w", err))
									return
								}

								// retry failed PropagateChildData
								id, err = db.PropagateChildData(t, pUID, sortK, tUID, id, v.Value)

								if err != nil {
									errlog.Add(logid, fmt.Errorf("AttachNode: error in PropagateChildData %w", err))
									return
								}
							} else {
								errlog.Add(logid, fmt.Errorf("AttachNode: error in PropagateChildData %w", err))
								return
							}
						}
						break
					}
				}
			}
		}
		// reverse edge is not cached so deal directly with database
		// no cache or db locking as the update is a atomic set-add
		err = db.UpdateReverseEdge(cUID, pUID, tUID, sortK, id)
		if err != nil {
			errlog.Add(logid, err)
			return
		}

	}()

	// setAvailable := func(tUID util.UID, id int, cnt int, ty string) {
	// 	err = pnd.SetUpredAvailable(sortK, pUID, cUID, tUID, id, cnt, ty)
	// 	if err != nil {
	// 		errlog.Add(logid, fmt.Errorf("AttachNode main errored in SetUpredAvailable. Ty %s. Error: %s", ty, err.Error()))
	// 	}
	// 	syslog(fmt.Sprintf("SetUpredAvailable succesful %d %d %s", id, cnt, ty))
	// }

	handleErr := func(err error) {
		pnd.Unlock()
		errlog.Add(logid, err)
		// send empty payload so concurrent routine will abort -
		// not necessary to capture nil payload error from routine as it has a buffer size of 1
		xch <- chPayload{}
		wg.Wait()
	}

	//pnd, err = gc.FetchForUpdate(pUID, sortK)
	pnd, err = gc.FetchUIDpredForUpdate(pUID, sortK)
	defer pnd.Unlock()
	// to fix need to add Ty item to each uid-pred so type is returned from {uid,sortk} query
	//	pnd, err = gc.FetchForUpdate(pUID, sortK)
	if err != nil {
		handleErr(fmt.Errorf("main errored in FetchForUpdate: for %s errored..%w", pUID, err))
		return
	}
	//
	// get type of child node from A#T sortk e.g "Person"
	//
	if pTyName, ok = pnd.GetType(); !ok {
		handleErr(fmt.Errorf(fmt.Sprintf("AttachNode: Error in GetType of parent node")))
		return
	}
	syslog(fmt.Sprintf("in main, pTyName %s sortk %q  pUID  %s", pTyName, sortK, pUID))
	//
	// get type details from type table for child node
	//
	var pty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
	if pty, err = types.FetchType(pTyName); err != nil {
		handleErr(fmt.Errorf("AttachNode main: Error in types.FetchType : %w", err))
		return
	}
	//
	targetUID, id, err := pnd.ConfigureUpred(sortK, pUID, cUID) // TODO - don't saveConfigUpred until child node successfully joined. Also clear cache entry for uid-pred on parent - so it must be read from storage.
	if err != nil {
		// undo inUse state set by ConfigureUpred
		if targetUID != nil {
			pnd.ClearCache(sortK) //setAvailable(targetUID, id, 0, pTyName)
		}
		handleErr(fmt.Errorf("AttachNode main error in configuring upd-pred: %w", err))
		return
	}
	//
	// get concurrent goroutine to write event items
	//
	pass := chPayload{tUID: targetUID, itemId: id, nd: pnd, pTy: pty}
	xch <- pass

	syslog(fmt.Sprintf("AttachNode: Waitng for child routine to finish"))
	wg.Wait()

	if childErr != nil {

		err = childErr
		syslog(fmt.Sprintf("AttachNode (cUID->pUID: %s->%s %s) failed Error: %s", cUID, pUID, sortK, childErr))
		pnd.ClearCache(sortK, true)

	} else {

		syslog(fmt.Sprintf("AttachNode (cUID->pUID: %s->%s %s) Suceeded", cUID, pUID, sortK))
		err = pnd.CommitUPred(sortK, pUID, cUID, targetUID, id, 1, pTyName)
		if err != nil {
			errlog.Add(logid, fmt.Errorf("AttachNode main errored in SetUpredAvailable. Ty %s. Error: %s", pTyName, err.Error()))
		}
		syslog(fmt.Sprintf("SetUpredAvailable succesful %d %d %s", id, 1, pTyName))

	}
	//
	// monitor: increment attachnode counter
	//
	stat := mon.Stat{Id: mon.AttachNode}
	mon.StatCh <- stat

}

// recoverItemSizeErr is now redundant. It was necessary when the design used the 400K Dynamodb item size limit
// as a triggering point to create a new target item for progatation of child data.
func recoverItemSizeErr(gc *cache.GraphCache, pUID, cUID, tUID util.UID, sortk string) []error {
	var (
		err    []error
		wg     sync.WaitGroup
		xcherr chan error
	)

	fmt.Println("recoverItemSizeErr: ")
	xcherr = make(chan error, 2)
	defer close(xcherr)

	wg.Add(2)

	// these two routines operate on separate parts of the cache and can be safely run concurrently

	// in Parent Node: set overflow block (UID) flag to full
	go func() {
		defer wg.Done()

		pnd, err := gc.FetchForUpdate(pUID, sortk)
		defer pnd.Unlock()
		if err != nil {
			xcherr <- err
			return
		}
		// update cache and persist to db
		err = pnd.SetOvflBlkFull(tUID, sortk)
		if err != nil {
			xcherr <- err
		}
	}()

	go func() {
		defer wg.Done()

		// clear cache as I don't want to update cache.
		// we can now update db and then unlock.
		fmt.Println("LockAndClearNodeCache : ", tUID.String())
		en := gc.LockAndClearNodeCache(tUID)
		defer en.UnlockNode()
		fmt.Printf("en is: %#v\n", en)
		// not cached so update db
		err := db.SetCUIDpgFlag(tUID, cUID, sortk)
		if err != nil {
			xcherr <- err
			return
		}
		if err != nil {
			xcherr <- fmt.Errorf("Failure in recoverItemSizeErr of AttachNode. Error: %s ", err.Error())
		}

	}()

	wg.Wait()

	for i := 0; i < 2; i++ {
		select {
		case e := <-xcherr: // what if both error - simply ignore for time being
			err = append(err, e)
		default:
		}
	}

	if len(err) > 0 {
		return err
	}
	return nil
}

func DetachNode(cUID, pUID util.UID, sortK string) error {
	//

	var (
		err error
		ok  bool
		eID util.UID
	)

	ev := event.DetachNode{CID: cUID, PID: pUID, SK: sortK}
	eID, err = event.New(ev)
	if err != nil {
		return fmt.Errorf("Error in DetachNode creating an event: %s", err)
	}
	// log Event via defer
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				event.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event. NO THIS IS PERFORMED BY STREAMS Lambda function.
			} else {
				event.LogEventSuccess(eID, t1.Sub(t0).String())
			}
		}
	}()()
	//
	// CEG - Concurrent event gatekeeper.
	//
	if ok, err = db.EdgeExists(cUID, pUID, sortK, db.DELETE); !ok {
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			return gerr.NodesNotAttached
		}
	}
	if err != nil {
		return err
	}
	err = db.DetachNode(cUID, pUID, sortK)
	if err != nil {
		var nif db.DBNoItemFound
		if errors.As(err, &nif) {
			err = nil
			fmt.Println(" returning with error NodesNotAttached..............")
			return gerr.NodesNotAttached
		}
		return err
	}

	return nil
}

// func eventNew(eventData interface{}) ([]byte, error) {

// 	eID, err := event.New()
// 	if err != nil {
// 		return nil, err
// 	}

// 	m := event.EventMeta{EID: eID, SEQ: 1, Status: "I", Start: time.Now().String(), Dur: "_"}
// 	switch x := eventData.(type) {

// 	case event.AttachNode:
// 		m.OP = "AN"
// 		x.EventMeta = m
// 		db.LogEvent(x)

// 	case event.DetachNode:
// 		m.OP = "DN"
// 		x.EventMeta = m
// 		db.LogEvent(x)
// 	}

// 	return eID, nil

// }
