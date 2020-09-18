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
	"github.com/DynamoGraph/rdf/anmgr"
	"github.com/DynamoGraph/rdf/grmgr"
	//	"github.com/DynamoGraph/rdf/uuid"
	"github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
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

func AttachNode(cUID, pUID util.UID, sortK string, e_ anmgr.EdgeSn, wg_ *sync.WaitGroup, lmtr grmgr.Limiter) []error { // pTy string) error { // TODO: do I need pTy (parent Ty). They can be derived from node data. Child not must attach to parent attribute of same type
	//
	// update data cache to reflect child node attached to parent. This involves
	// 1. append chid UID to the associated parent uid-predicate, parent e.g. sortk A#G#:S
	// 2. propagate child scalar data to associated uid-predicate (parent's 'G' type) G#:S#:A etc..
	//
	//func AttachNode(cUID, pUID util.UID, sortK string, wg_ *sync.WaitGroup) []error { // pTy string) error { // TODO: do I need pTy (parent Ty). They can be derived from node data. Child not must attach to parent attribute of same type
	//
	// update data cache to reflect child node attached to parent. This involves
	// 1. append chid UID to the associated parent uid-predicate, parent e.g. sortk A#G#:S
	// 2. propagate child scalar data to associated uid-predicate (parent's 'G' type) G#:S#:A etc..
	//
	defer anmgr.AttachDone(e_)
	defer wg_.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	var errS []error

	var addErr = func(e ...error) []error {
		errS = append(errS, e...)
		return errS
	}

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

	gc := cache.NewCache()
	//
	// log Event via defer
	//
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				db.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event
			} else {
				db.LogEventSuccess(eID, t1.Sub(t0).String())
			}
		}
	}()()
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
	errch := make(chan error, 1) // buffered so allowed to return
	defer close(errch)
	//
	// NOOP condition aka CEG - Concurrent event gatekeeper. Add edge only if it doesn't already exist (in one atomic unit) that can be used to protect against identical concurrent (or otherwise) attachnode events.
	//
	// TODO: fix bugs in edgeExists algorithm - see bug list
	if ok, err := db.EdgeExists(cUID, pUID, sortK, db.ADD); ok {
		fmt.Println("Edge does exit: ", err.Error())
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			return addErr(gerr.NodesAttached)
		}
		return addErr(err)

	}
	fmt.Println("Edge does NOT exit....")
	//
	// log Event
	//
	// going straight to db is safe provided its part of a FetchNode lock and all updates to the "R" predicate are performed within the FetchNode lock.
	ev := event.AttachNode{CID: cUID, PID: pUID, SK: sortK}
	eID, err = eventNew(ev)
	if err != nil {
		return addErr(err)
	}
	//
	wg.Add(1)
	//
	go func() {
		defer wg.Done()
		//
		// Grab child scalar data and lock child  node. Unlocked in UnmarshalCache and defer.(?? no need for cUID lock after Unmarshal - I think?)  ALL SCALARS SHOUD BEGIN WITH sortk "A#"
		//
		syslog.Log("AttachNode: gr1 ", fmt.Sprintf("FetchForUpdate: for child    %s", cUID.String()))
		cnd, err := gc.FetchForUpdate(cUID, "A#")
		defer cnd.Unlock("ON cUID for AttachNode second goroutine..") // note unmarshalCache nolonger release the lock
		if err != nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("AttachNode: error fetching child scalar data: %s", err.Error()))
			errch <- fmt.Errorf("AttachNode: error fetching child scalar data: %w", err)
			return
		}
		//
		// get type of child node from A#T sortk e.g "Person"
		//
		if cTyName, ok = cnd.GetType(); !ok {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("Error in GetType"))
			errch <- cache.NoNodeTypeDefinedErr
			return
		}
		//
		// get type details from type table for child node
		//
		var cty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
		if cty, err = cache.FetchType(cTyName); err != nil {
			errch <- err
			return
		}
		fmt.Printf("cty: %#v\n", cty)
		//
		//***************  wait for payload from cocurrent routine ****************
		//
		var payload chPayload
		// prevent panic on closed channel by using bool test on channel.
		if payload, ok = <-xch; !ok {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("Errored: Channel xch prematurely closed and drained "))
			errch <- fmt.Errorf("AttachNode: Channel xch prematurely closed and drained")
			return
		}
		tUID := payload.tUID
		pnd = payload.nd
		defer pnd.Unlock()
		id := payload.itemId
		pty := payload.pTy // parent type
		if tUID == nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("errored: target UID is nil.. "))
			errch <- fmt.Errorf("AttachNode: Got a target UID of nil")
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
		s := strings.Split(sortK, "#")

		attachPoint := s[len(s)-1][1:]
		fmt.Printf("\nggf attachTy: %s %s\n", s[len(s)-1], attachPoint)
		//
		// find attachment point in parent type based on sortk input
		//
		for _, v := range pty {

			if v.C == attachPoint {
				//
				// parent attachment point attribute found
				//
				// is a IncP defined in the type definition. This will define the child attributes to propagate (short names used)
				//
				if len(v.IncP) > 0 {

					for _, ps := range v.IncP {
						var found bool
						for _, cs := range cty {
							if cs.C == ps {
								switch cs.DT {
								case "I", "F", "Bl", "S", "DT":
								}
								// found assoicated child scalar attribute
								found = true
								cnv = append(cnv, &ds.NV{Name: cs.Name})
							}
						}
						if !found {
							errch <- fmt.Errorf(fmt.Sprintf("AttachNode: Child scalar attribute not found based on parent IncP value, %q", ps))
							return
						}
					}

				} else {

					// grab all scalars from child type
					for _, v := range cty {
						switch v.DT {
						// scalar types to be propagated
						case "I", "F", "Bl", "S", "DT": //TODO: these attributes should belong to pUpred type only. Can a node be made up of more than one type? Pesuming at this stage only 1, so all scalars are relevant.
							if v.Pg {
								// scalar type has propagation enabled
								nv := &ds.NV{Name: v.Name}
								cnv = append(cnv, nv)
							}
						}
					}
				}
			}
		}
		fmt.Printf("\nfff  nv : %#v\n", cnv)
		//
		// copy cache data into cnv and unlock child node.
		//
		err = cnd.UnmarshalCache(cnv)
		if err != nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("Errored: Unmarshall errored... %s", err.Error()))
			errch <- fmt.Errorf("AttachNode: Unmarshal error : %s", err)
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
						errch <- fmt.Errorf("AttachNode: error in PropagateChildData %w", err) //TODO: understand goroutine errch <- ??
						//gc.UnlockNode(tUID)
						return // triggers wg.Done()
					}
					break
				}
			}
		}
		// reverse edge is not cached so deal directly with database
		// no cache or db locking as the update is a atomic set-add
		err = db.UpdateReverseEdge(cUID, pUID, tUID, sortK, id)
		if err != nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf(" puidLocked UNLOCK a %s", err.Error()))
			errch <- err
			return
		}
		//
		err = pnd.SetUpredAvailable(sortK, pUID, cUID, tUID, id, 1)
		if err != nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("Errored: SetUpredAvailable %s", err.Error()))
			errch <- err
		}

		// select {
		// 	case <-ctx.Done():

		// }
	}()
	//
	// fetch parent node to find its type. This will lock parent node for update (no shared locks). Explicit unlocked in defer
	//
	syslog.Log("AttachNode: main ", fmt.Sprintf("FetchForUpdate: for parent    %s  sortk: %s", pUID.String(), sortK))
	idx := strings.IndexByte(sortK, '#')

	pnd, err = gc.FetchForUpdate(pUID, sortK[:idx+1])
	//	pnd, err = gc.FetchForUpdate(pUID, sortK)
	if err != nil {
		pnd.Unlock()
		syslog.Log("AttachNode: main ", fmt.Sprintf("FetchForUpdate:  errored..%s", err.Error()))
		return addErr(err)
	}
	//
	// get type of child node from A#T sortk e.g "Person"
	//
	if pTyName, ok = pnd.GetType(); !ok {
		syslog.Log("AttachNode: main ", fmt.Sprintf("#Error in GetType"))
		errch <- cache.NoNodeTypeDefinedErr
		return addErr(err)
	}
	//
	// get type details from type table for child node
	//
	var pty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
	if pty, err = cache.FetchType(pTyName); err != nil {
		errch <- err
		return addErr(err)
	}
	//
	targetUID, id, err := pnd.ConfigureUpred(sortK, pUID, cUID)
	if err != nil {
		pnd.Unlock()
		err := fmt.Errorf("AttachNode: Error in configuring upd-pred block for propagation of child data: %w", err)
		// TODO: consider using a Cancel Context
		xch <- chPayload{}
		wg.Wait()
		<-errch
		return addErr(err)
	}
	//
	// get concurrent goroutine to write event items
	//
	pass := chPayload{tUID: targetUID, itemId: id, nd: pnd, pTy: pty}
	xch <- pass

	wg.Wait()
	//
	// two goroutines can result in upto two errors
	//
	for i := 0; i < 2; i++ {
		select {
		case e := <-errch:

			if errors.Is(e, db.ErrItemSizeExceeded) {
				// Note: this error should note occur. I have changed from using the 400K dynamodb inbuilt item size limit to trigger a new
				// UID item for propagation to using the SIZE attribute limit as a conditional update.
				// recover from error and rerun operation
				e := recoverItemSizeErr(gc, pUID, cUID, targetUID, sortK)

				if len(e) > 0 {
					addErr(e...)
				} else {
					return AttachNode(cUID, pUID, sortK, e_, wg_, lmtr)
				}

			} else {
				addErr(e)
			}

		default:
		}
	}
	if len(errS) > 0 {
		return errS
	}
	return nil
}

func AttachNode2(cUID, pUID util.UID, sortK string) []error { // pTy string) error { // TODO: do I need pTy (parent Ty). They can be derived from node data. Child not must attach to parent attribute of same type
	//
	// update data cache to reflect child node attached to parent. This involves
	// 1. append chid UID to the associated parent uid-predicate, parent e.g. sortk A#G#:S
	// 2. propagate child scalar data to associated uid-predicate (parent's 'G' type) G#:S#:A etc..
	//
	//func AttachNode(cUID, pUID util.UID, sortK string, wg_ *sync.WaitGroup) []error { // pTy string) error { // TODO: do I need pTy (parent Ty). They can be derived from node data. Child not must attach to parent attribute of same type
	//
	// update data cache to reflect child node attached to parent. This involves
	// 1. append chid UID to the associated parent uid-predicate, parent e.g. sortk A#G#:S
	// 2. propagate child scalar data to associated uid-predicate (parent's 'G' type) G#:S#:A etc..
	//

	var errS []error

	var addErr = func(e ...error) []error {
		errS = append(errS, e...)
		return errS
	}

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

	gc := cache.NewCache()
	//
	// log Event via defer
	//
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				db.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event
			} else {
				db.LogEventSuccess(eID, t1.Sub(t0).String())
			}
		}
	}()()
	//
	// this API deals only in UID that are known to exist - hence NodeExists() not necessary
	//
	syslog.Log("AttachNode2:", fmt.Sprintf("Start: attach  %q to %q ", cUID.String(), pUID.String()))
	if ok, err := db.NodeExists(cUID); !ok {
		if err == nil {
			return addErr(fmt.Errorf("Child node UUID %q does not exist:", cUID))
		} else {
			return addErr(fmt.Errorf("Error in validating child node %w", err))
		}
	}
	if ok, err := db.NodeExists(pUID, sortK); !ok {
		if err == nil {
			return addErr(fmt.Errorf("Parent node and/or attachment predicate for UUID %q does not exist", pUID))
		} else {
			return addErr(fmt.Errorf("Error in validating parent node %w", err))
		}
	}
	// create channels used to pass target UID for propagation and errors
	xch := make(chan chPayload, 1)
	defer close(xch)
	errch := make(chan error, 1) // buffered so allowed to return
	defer close(errch)
	//
	// NOOP condition aka CEG - Concurrent event gatekeeper. Add edge only if it doesn't already exist (in one atomic unit) that can be used to protect against identical concurrent (or otherwise) attachnode events.
	//
	// TODO: fix bugs in edgeExists algorithm - see bug list
	if ok, err := db.EdgeExists(cUID, pUID, sortK, db.ADD); ok {
		syslog.Log("AttachNode2:", fmt.Sprintf("Error: Edge exists : %s", err.Error()))
		if errors.Is(err, db.ErrConditionalCheckFailed) {
			return addErr(gerr.NodesAttached)
		}
		return addErr(err)
	}
	//
	// log Event
	//
	// going straight to db is safe provided its part of a FetchNode lock and all updates to the "R" predicate are performed within the FetchNode lock.
	ev := event.AttachNode{CID: cUID, PID: pUID, SK: sortK}
	eID, err = eventNew(ev)
	if err != nil {
		return addErr(err)
	}
	//
	wg.Add(1)
	//
	go func() {
		defer wg.Done()
		//
		// Grab child scalar data and lock child  node. Unlocked in UnmarshalCache and defer.(?? no need for cUID lock after Unmarshal - I think?)  ALL SCALARS SHOUD BEGIN WITH sortk "A#"
		//
		syslog.Log("AttachNode: gr1 ", fmt.Sprintf("FetchForUpdate: for child    %s", cUID.String()))

		cnd, err := gc.FetchForUpdate(cUID, "A#")
		defer cnd.Unlock("ON cUID for AttachNode second goroutine..") // note unmarshalCache nolonger release the lock
		if err != nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("AttachNode: error fetching child scalar data: %s", err.Error()))
			errch <- fmt.Errorf("AttachNode: error fetching child scalar data: %w", err)
			return
		}
		//
		// get type of child node from sortk A#T  e.g "Person"
		//
		if cTyName, ok = cnd.GetType(); !ok {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("Error in GetType"))
			errch <- cache.NoNodeTypeDefinedErr
			return
		}
		//
		// get type details from type table for child node
		//
		var cty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
		if cty, err = cache.FetchType(cTyName); err != nil {
			errch <- err
			return
		}
		//
		// ***************  wait for payload from main routine ****************
		//
		var payload chPayload
		// prevent panic on closed channel by using bool test on channel.
		if payload, ok = <-xch; !ok {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("Errored: Channel xch prematurely closed and drained "))
			errch <- fmt.Errorf("AttachNode: Channel xch prematurely closed and drained")
			return
		}
		// assign payload contents to vars
		tUID := payload.tUID
		id := payload.itemId
		pnd = payload.nd
		pty := payload.pTy // parent type
		defer pnd.Unlock()

		if tUID == nil {
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf("errored: target UID is nil.. "))
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
		//
		// find attachment point in parent type based on sortk input
		//
		s := strings.Split(sortK, "#")
		attachPoint := s[len(s)-1][1:]

		for _, v := range pty {

			if v.C == attachPoint {
				//
				// parent attachment point attribute found
				//
				// is a IncP defined in the type definition. This will define the child attributes to propagate (short names used)
				//
				if len(v.IncP) > 0 {

					for _, ps := range v.IncP {
						var found bool
						for _, cs := range cty {
							if cs.C == ps {
								switch cs.DT {
								case "I", "F", "Bl", "S", "DT":
								}
								// found assoicated child scalar attribute
								found = true
								cnv = append(cnv, &ds.NV{Name: cs.Name})
							}
						}
						if !found {
							errch <- fmt.Errorf(fmt.Sprintf("AttachNode: Child scalar attribute not found based on parent IncP value, %q", ps))
							return
						}
					}

				} else {

					// grab all scalars from child type
					for _, v := range cty {
						switch v.DT {
						// scalar types to be propagated
						case "I", "F", "Bl", "S", "DT": //TODO: these attributes should belong to pUpred type only. Can a node be made up of more than one type? Pesuming at this stage only 1, so all scalars are relevant.
							if v.Pg {
								// scalar type has propagation enabled
								nv := &ds.NV{Name: v.Name}
								cnv = append(cnv, nv)
							}
						}
					}
				}
			}
		}
		//
		// if there are scalars to propagate
		//
		if len(cnv) > 0 {
			//
			// unmarshal cache data into cnv and unlock child node.
			//
			err = cnd.UnmarshalCache(cnv)
			if err != nil {
				syslog.Log("AttachNode: main ", fmt.Sprintf("Errored: Unmarshall errored... %s", err.Error()))
				errch <- fmt.Errorf("AttachNode: Unmarshal error : %s", err)
				return
			}
			//
			// ConfigureUpred() has primed the target propagation block with cUID and XF Inuse flag. Ready for propagation of Scalar data.
			// lock pUID if it is the target of the data propagation.
			// for overflow blocks the entry in the Nd of the uid-pred is set to InUse which syncs access.
			// propagation data is not cached - so call db api directly

			for _, t := range cty {

				for _, v := range cnv {

					if t.Name == v.Name { //&& v.Value != nil {

						id, err = db.PropagateChildData(t, pUID, sortK, tUID, id, v.Value)
						if err != nil {
							// TODO: rollback propagation data.
							errch <- fmt.Errorf("AttachNode: error in PropagateChildData %w", err) //TODO: understand goroutine errch <- ??
							//gc.UnlockNode(tUID)
							return // triggers wg.Done()
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
			syslog.Log("AttachNode: gr1 ", fmt.Sprintf(" puidLocked UNLOCK a %s", err.Error()))
			errch <- err
			return
		}

	}()

	setAvailable := func(tUID util.UID, id int, cnt int) {
		err = pnd.SetUpredAvailable(sortK, pUID, cUID, tUID, id, cnt)
		if err != nil {
			syslog.Log("AttachNode: main ", fmt.Sprintf("Errored: SetUpredAvailable %s", err.Error()))
		}
		syslog.Log("AttachNode: main ", fmt.Sprintf("SetUpredAvailable succesful %d %d ", id, cnt))
	}

	//
	// fetch parent node to find its type. This will lock parent node for update (no shared locks). Explicit unlocked in defer
	//
	syslog.Log("AttachNode: main ", fmt.Sprintf("FetchForUpdate: for parent    %s  sortk: %s", pUID.String(), sortK))

	idx := strings.IndexByte(sortK, '#')

	pnd, err = gc.FetchForUpdate(pUID, sortK[:idx+1])
	if err != nil {
		pnd.Unlock()
		syslog.Log("AttachNode: main ", fmt.Sprintf("FetchForUpdate:  errored..%s", err.Error()))
		xch <- chPayload{}
		wg.Wait()
		return addErr(err)
	}
	//
	// get type of child node from A#T sortk e.g "Person"
	//
	if pTyName, ok = pnd.GetType(); !ok {
		syslog.Log("AttachNode: main ", fmt.Sprintf("Error in GetType"))
		xch <- chPayload{}
		wg.Wait()
		return addErr(cache.NoNodeTypeDefinedErr)
	}
	//
	// get type details from type table for child node
	//
	var pty blk.TyAttrBlock // note: this will load cache.TyAttrC -> map[Ty_Attr]blk.TyAttrD
	if pty, err = cache.FetchType(pTyName); err != nil {
		xch <- chPayload{}
		wg.Wait()
		return addErr(err)
	}
	//
	// ConfigureUpred selects target for propagation of scalar data and marks it inUse. Adds cUID to Nd/Overflowblock.
	//
	targetUID, id, err := pnd.ConfigureUpred(sortK, pUID, cUID)
	if err != nil {
		// undo inUse state set by ConfigureUpred
		setAvailable(targetUID, id, 0)
		pnd.Unlock()
		err := fmt.Errorf("AttachNode: Error in configuring upd-pred block for propagation of child data: %w", err)
		// TODO: consider using a Cancel Context
		xch <- chPayload{}
		wg.Wait()
		return addErr(err)
	}
	//
	// get concurrent goroutine to write event items
	//
	pass := chPayload{tUID: targetUID, itemId: id, nd: pnd, pTy: pty}
	xch <- pass

	wg.Wait()
	//
	setAvailable(targetUID, id, 1)
	//
	// two goroutines can result in upto two errors
	//
	for i := 0; i < 2; i++ {
		select {
		case e := <-errch:

			if errors.Is(e, db.ErrItemSizeExceeded) {
				// Note: this error should note occur. I have changed from using the 400K dynamodb inbuilt item size limit to trigger a new
				// UID item for propagation to using the SIZE attribute limit as a conditional update.
				// recover from error and rerun operation
				e := recoverItemSizeErr(gc, pUID, cUID, targetUID, sortK)

				if len(e) > 0 {
					addErr(e...)
				} else {
					return AttachNode2(cUID, pUID, sortK)
				}

			} else {
				addErr(e)
			}

		default:
		}
	}
	if len(errS) > 0 {
		return errS
	}
	return nil
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
	eID, err = eventNew(ev)
	if err != nil {
		return err
	}
	// log Event via defer
	defer func() func() {
		t0 := time.Now()
		return func() {
			t1 := time.Now()
			if err != nil {
				db.LogEventFail(eID, t1.Sub(t0).String(), err) // TODO : this should also create a CW log event. NO THIS IS PERFORMED BY STREAMS Lambda function.
			} else {
				db.LogEventSuccess(eID, t1.Sub(t0).String())
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
	fmt.Println("here..")
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

func eventNew(eventData interface{}) ([]byte, error) {

	eID, err := event.New()
	if err != nil {
		return nil, err
	}

	m := event.EventMeta{EID: eID, SEQ: 1, Status: "I", Start: time.Now().String(), Dur: "_"}
	switch x := eventData.(type) {

	case event.AttachNode:
		m.OP = "AN"
		x.EventMeta = m
		db.LogEvent(x)

	case event.DetachNode:
		m.OP = "DN"
		x.EventMeta = m
		db.LogEvent(x)
	}

	return eID, nil

}
