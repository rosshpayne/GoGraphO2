package cache

import (
	"fmt"
	"strings"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/db"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

func (g *GraphCache) LockNode(uid util.UID) {

	fmt.Printf("** Cache LockNode  Key Value: [%s]\n", uid.String())

	g.Lock()
	uids := uid.String()
	e := g.cache[uids]

	if e == nil {
		e = &entry{ready: make(chan struct{})}
		e.NodeCache = &NodeCache{}
		g.cache[uids] = e
		g.Unlock()
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock e . Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()

}

func (g *GraphCache) FetchForUpdate(uid util.UID, sortk ...string) (*NodeCache, error) {
	var sortk_ string
	//	return g.FetchNode(uid, true)
	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = "A#"
	}
	//slog.Log("FetchForUpdate: ", fmt.Sprintf("** Cache FetchForUpdate Cache Key Value: [%s]   sortk: %s", uid.String(), sortk_))
	e := g.cache[uid.String()]
	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uid.String()] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataItem
		nb, err := db.FetchNode(uid, sortk_)
		if err != nil {
			slog.Log("FetchForUpdate: ", fmt.Sprintf("db fetchnode error: %s", err.Error()))
			return nil, err
		}
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.SortK] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock e to prevent updates from other routines. Must explicitly Unlock() from client.
	//  Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()
	//if e != nil && e.NodeCache == nil || e == nil {
	// node cache has been cleared. Start again.
	if e.NodeCache == nil {
		slog.Log("FetchForUpdate: ", "e.NodeCache == nil TRUE bad a bout to e=nil")
		// cache has been cleared. Start again.
		e = nil
		g.FetchForUpdate(uid, sortk_)
	}
	e.ffuEnabled = true
	e.locked = false
	var found bool
	// check sortk is cached
	// TODO: better algorithm required to detect if sortk is cached.
	if sortk_ == "A#" && len(e.m) < 4 {
		e.fetchItems(sortk_)
	}

	if !found {
		e.fetchItems(sortk_) // e.NodeCache.fetchItems(sortk)
	}

	return e.NodeCache, nil
}

func (g *GraphCache) LockPredR(uid util.UID, sortk ...string) error {

	g.rsync.Lock()

	uids := uid.String()
	r := g.cacheR[uids]

	if r == nil {
		r = &Rentry{ready: make(chan struct{})}
		g.cacheR[uids] = r
		g.rsync.Unlock()
		close(r.ready)
	} else {
		g.rsync.Unlock()
		<-r.ready
	}

	r.Lock()

	return nil
}

func (g *GraphCache) FetchNodeExec(uid util.UID, ty string) (*NodeCache, error) {
	return nil, nil
}
func (g *GraphCache) FetchNodeExec_(uid util.UID, sortk string, ty string) (*NodeCache, error) {
	return nil, nil
}

func (g *GraphCache) FetchNodeNonCache(uid util.UID, sortk ...string) (*NodeCache, error) {
	var sortk_ string
	fmt.Printf("** Cache FetchNode uid- %d %s sortK: %s \n\n", len(uid), uid.String(), sortk)

	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = "A#"
	}
	uids := uid.String()
	e := g.cache[uids]
	fmt.Printf("**2 Cache FetchNode uid- %d %s sortK: %s \n\n", len(uid), uid.String(), sortk_)
	e = nil
	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uids] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataIte
		fmt.Printf("**3 Cache FetchNode uid- %d %s sortK: %s \n\n", len(uid), uid.String(), sortk_)
		nb, err := db.FetchNode(uid, sortk_)
		if err != nil {
			return nil, err
		}

		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.SortK] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock node cache. TODO: when is it unlocked?????
	//
	e.RLock()
	if e.NodeCache == nil {
		// cache has been cleared. Start again.
		e = nil
		g.FetchNode(uid, sortk_)
	}
	e.locked = true
	e.ffuEnabled = false
	var found bool
	// check sortk is cached
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			found = true
			break
		}
	}
	if !found {
		e.RUnlock()
		e.fetchItems(sortk_)
	}

	e.RUnlock()

	return e.NodeCache, nil
}

func (g *GraphCache) FetchNode(uid util.UID, sortk ...string) (*NodeCache, error) {
	var sortk_ string
	fmt.Printf("** Cache FetchNode uid- %d %s sortK: %s \n\n", len(uid), uid.String(), sortk)

	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = "A#"
	}
	uids := uid.String()
	e := g.cache[uids]
	fmt.Printf("**2 Cache FetchNode uid- %d %s sortK: %s \n\n", len(uid), uid.String(), sortk_)

	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uids] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataIte
		fmt.Printf("**3 Cache FetchNode uid- %d %s sortK: %s \n\n", len(uid), uid.String(), sortk_)
		nb, err := db.FetchNode(uid, sortk_)
		if err != nil {
			return nil, err
		}

		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.SortK] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock node cache. TODO: when is it unlocked?????
	//
	e.RLock()
	if e.NodeCache == nil {
		// cache has been cleared. Start again.
		e = nil
		g.FetchNode(uid, sortk_)
	}
	e.locked = true
	e.ffuEnabled = false
	var found bool
	// check sortk is cached
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			found = true
			break
		}
	}
	if !found {
		e.RUnlock()
		e.fetchItems(sortk_)
	}

	e.RUnlock()

	return e.NodeCache, nil
}

func (g *NodeCache) fetchItems(sortk string) error {

	slog.Log("fetchItems: ", fmt.Sprintf("+++  Cache FetchItems for sortk %s UID: [%s] \n", sortk, g.Uid.String()))

	nb, err := db.FetchNode(g.Uid, sortk)
	if err != nil {
		return err
	}
	// add data items to cache map
	for _, v := range nb {
		g.m[v.SortK] = v
	}

	return nil
}

func (g *GraphCache) LockAndClearNodeCache(uid util.UID) *entry {

	fmt.Println()
	fmt.Println("================================ LOCK and CLEAR NODE CACHE =======================================")
	fmt.Printf(" Clear node cache for: %s", uid.String())
	fmt.Println()
	//
	// check if node is cached
	//
	var (
		//		ok bool
		e *entry
	)
	uids := uid.String()
	fmt.Println("Acquire gLock")
	g.Lock()
	fmt.Println("Acquired gLock")
	//	if e, ok = g.cache[uids]; !ok {
	e = &entry{ready: make(chan struct{})}
	e.NodeCache = &NodeCache{gc: g}
	g.cache[uids] = e
	g.Unlock()
	fmt.Println("REleased gLock")
	// 	close(e.ready)
	// } else {
	// 	fmt.Println("RElease gLock")
	// 	g.Unlock()
	// 	fmt.Printf("REleased gLock e is %#v\n ", e)
	// 	<-e.ready
	// }
	//
	// lock node and then clear cache
	//
	fmt.Println("Acquire eLock")
	e.Lock()
	fmt.Println("Acquired eLock")
	fmt.Printf("== EXIT LOCK and CLEAR NODE CACHE == %#v\n", e)
	return e
}

// UnlockNode used for blocks that are not cached.
func (e *entry) UnlockNode() error {

	// var (
	// 	e  *entry
	// 	ok bool
	// )
	fmt.Printf("** Cache UnlockNode  K\n")
	//g := e.gc
	// g.Lock()
	// uids := uid.String()
	// g.Unlock()
	// if e, ok = g.cache[uids]; !ok {
	// 	return fmt.Errorf("No lock on %q", uids)
	// }
	if e == nil {
		return fmt.Errorf("e is nil for ")
	}
	fmt.Println("Unlock e")
	if e.NodeCache == nil {
		fmt.Println("UnlockNode: for e.NodeCache == nul.......")
	}
	e.Unlock("from e UnlockNode")
	fmt.Println("Unlocked e")

	return nil
}

func (g *GraphCache) ClearNodeCache(uid util.UID) error {

	fmt.Println()
	fmt.Println("================================ CLEAR NODE CACHE =======================================")
	fmt.Printf(" Clear node cache for: %s", uid.String())
	fmt.Println()
	//
	// check if node is cached
	//
	var (
		// ty  string
		// tab blk.TyAttrBlock
		ok bool
		e  *entry
	)
	g.Lock()
	if e, ok = g.cache[uid.String()]; !ok {
		fmt.Println("Nothing to clear")
		g.Unlock()
		return nil
	}
	g.Unlock()
	//
	// lock node
	//
	fmt.Println("ClearNodeCache: FetchForUpdate")
	nc, err := g.FetchForUpdate(uid)
	defer nc.Unlock("set by FetchforUPdate in Clear node cache ")
	if err != nil {
		return err
	}
	//
	// remove any overflow blocks
	//
	// get type definition and list its uid-predicates (e.g. siblings, friends)
	// if ty, ok = nc.GetType(); !ok {
	// 	return NoNodeTypeDefinedErr
	// }
	// if tab, err = FetchType(ty); err != nil {
	// 	return err
	// }

	// for _, c := range tab.GetUIDpredC() {
	// 	sortk := "A#G#:" + c
	// 	// get sortk's overflow Block UIDs if any
	// 	for _, uid_ := range nc.GetOvflUIDs(sortk) {
	// 		if _, ok = g.cache[uid_.String()]; ok {
	// 			// delete map entry will mean e is unassigned and allow GC to purge e and associated node cache.
	// 			g.Lock()
	// 			delete(g.cache, uid_.String())
	// 			g.Unlock()
	// 		}
	// 	}
	// }
	//
	// clear NodeCache forcing any waiting readers on uid node to refresh from db
	//
	e.NodeCache = nil
	//
	// remove node from g cache and
	//
	g.Lock()
	delete(g.cache, uid.String())
	g.Unlock()
	//
	fmt.Println("==Clear cache finished ==")
	return nil
}

func (nd *NodeCache) Unlock(s ...string) {
	if len(s) > 0 {
		slog.Log("Unlock: ", fmt.Sprintf("******* IN UNLOCK NC ********************  %s", s[0]))
	} else {
		slog.Log("Unlock: ", "******* IN UNLOCK NC ********************")
	}
	if nd == nil {
		return
	}
	if nd.m != nil && len(nd.m) == 0 {
		// locked by LockNode() - without caching daa
		slog.Log("Unlock: ", "Success RWMutex.Unlock() len(nd.m)=0")
		nd.RWMutex.Unlock()
		return
	}
	if nd.ffuEnabled {

		nd.RWMutex.Unlock()
		nd.ffuEnabled = false

	} else if nd.locked {

		nd.RUnlock()
		slog.Log("Unlock: ", "Success RUnlock()")
		nd.locked = false
	} else {
		slog.Log("Unlock: ", "Error: Nothing unlocked ")
	}
	// locked by LockAndClearNodeCache or LockNode
	//nd.RWMutex.Unlock()
	//	fmt.Println("Exit UNLOCK NC..")
}

// // ClearOverflowCache: remove overflow blocks from the cache
// func (g *GraphCache) ClearOverflowCache(uid util.UID) error {

// 	fmt.Println()
// 	fmt.Println("================================ CLEAR NODE CACHE =======================================")
// 	fmt.Printf(" Clear any Overflow caches for: %s", uid.String())
// 	fmt.Println()
// 	//
// 	// check if node is cached
// 	//
// 	var (
// 		ty  string
// 		tab blk.TyAttrBlock
// 		ok  bool
// 	)
// 	g.Lock()
// 	if _, ok := g.cache[uid.String()]; !ok {
// 		fmt.Println("Nothing to clear")
// 		g.Unlock()
// 		return nil
// 	}
// 	g.Unlock()
// 	//
// 	// lock node
// 	//
// 	nc, err := g.FetchForUpdate(uid)
// 	defer nc.Unlock()
// 	if err != nil {
// 		return err
// 	}
// 	//
// 	// remove any overflow blocks
// 	//
// 	// get type definition and list its uid-predicates (e.g. siblings, friends)
// 	if ty, ok = nc.GetType(); !ok {
// 		return NoNodeTypeDefinedErr
// 	}
// 	if tab, err = FetchType(ty); err != nil {
// 		return err
// 	}

// 	for _, c := range tab.GetUIDpredC() {
// 		sortk := "A#G#:" + c
// 		// get sortk's overflow Block UIDs if any
// 		for _, uid_ := range nc.GetOvflUIDs(sortk) {
// 			g.Lock()
// 			if _, ok = g.cache[uid_.String()]; ok {
// 				delete(g.cache, uid_.String())
// 				g.Unlock()
// 			} else {
// 				g.Unlock()
// 			}
// 		}
// 	}

// 	return nil
// }
