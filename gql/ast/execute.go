package ast

import (
	"fmt"

	blk "github.com/DynamoGraph/block"
	"github.com/DynamoGraph/cache"
	"github.com/DynamoGraph/ds"
	"github.com/DynamoGraph/rdf/grmgr"
	"github.com/DynamoGraph/util"
)

type rootResult struct {
	uid   util.UID    //returned from root query
	tyS   string      //returned from root query
	sortk string      // returned from root query
	path  string      // #root (R) #root#Siblings#Friends - not currently used
	nv    ds.ClientNV // the data from the uid item. Populated during execution phase with results of filter operation.
}

func (r *RootStmt) Execute(grl grmgr.Limiter) {
	//
	// execute root func - get back slice of unfiltered results
	//
	result := r.RootFunc.F(r.RootFunc.Farg, r.RootFunc.Value)

	if len(result) == 0 {
		return
	}

	for _, v := range result {

		grl.Ask()
		<-grl.RespCh()

		result := &rootResult{uid: v.PKey, tyS: v.Ty, sortk: v.SortK, path: "root"}

		go r.filterRootResult(grl, result)

	}
}

func (r *RootStmt) filterRootResult(grl grmgr.Limiter, result *rootResult) {
	var (
		err error
		nc  *cache.NodeCache
	)
	//
	// generate NV from GQL stmt - will also hold data from query response once UmarshalNodeCache is run.
	// query->cache->unmarshal(nv)
	//
	nvm, nvc := r.genNV()
	//
	// generate sortk - determines extent of node data to be loaded into cache. Tries to keep it as norrow (specific) as possible.
	//
	sortkS := cache.GenSortK(nvc, result.tyS)
	//
	// fetch data - with optimised fetch - perform queries sequentially becuase of mutex lock on node map
	//
	gc := cache.GetCache()
	for _, sortk := range sortkS {
		nc, _ = gc.FetchNodeExec_(result.uid, sortk, result.tyS)
	}
	//
	// assign cached data to NV
	//
	err = nc.UnmarshalNodeCache(nvc, result.tyS)
	if err != nil {
		panic(err)
	}
	if r.Filter != nil {
		if !r.Filter.RootApply(nvm) {
			// clear node from cache.
			// nc.ClearCache() // TODO: implement
			return
		}
	}
	//
	for _, p := range r.Select {
		//
		switch x := p.Edge.(type) {

		case *UidPred: // child of child, R.N.N

			for _, p := range x.Select {

				switch x := p.Edge.(type) {

				case ScalarPred, *Variable:
					// do nothing as UnmarshalNodeCode has already saved scalar results in nv

				case *UidPred:
					var (
						ok  bool
						aty blk.TyAttrD
						nds [][][]byte
					)
					// get type of the uid-pred
					if aty, ok = cache.TypeC.TyAttrC[result.tyS+":"+x.Name()]; !ok {
						continue // ignore this attribute as it is not in current type
					}
					// results not in nv for this depth in graph. Must query uids stored in nv[i].Value -> [][][]byte
					data := nvm[x.Name()]
					if nds, ok = data.Value.([][][]byte); !ok {
						panic(fmt.Errorf("filterRootResult: data.Value is of wrong type"))
					}
					for i, u := range nds {
						for j, uid := range u {

							if data.State[i][j] == 1 {
								continue // soft delete set or failed filter condition
							}

							grl.Ask()
							<-grl.RespCh()

							go x.execNode(grl, util.UID(uid), aty.Ty)
						}
					}
				}
			}
		}
	}

	r.nv = nvc
}

func (u *UidPred) execNode(grl grmgr.Limiter, uid util.UID, ty string) {

	var (
		err error
		nc  *cache.NodeCache
	)
	//
	// generate NV from GQL stmt - will also hold data from query response once UmarshalNodeCache is run.
	// query->cache->unmarshal(nv)
	//
	nvm, nvc := u.genNV()
	//
	// generate sortk - determines extent of node data to be loaded into cache. Tries to keep it as norrow (specific) as possible.
	//
	sortkS := cache.GenSortK(nvc, ty)
	//
	// fetch data - with optimised fetch - perform queries sequentially because of mutex lock on node map
	//
	gc := cache.GetCache()
	for _, sortk := range sortkS {
		nc, _ = gc.FetchNodeExec_(uid, sortk, ty)
	}
	//
	// assign cached data to NV
	//
	err = nc.UnmarshalNodeCache(nvc)
	if err != nil {
		panic(err)
	}
	// filter will logically remove child nodes (via nv.State setting) that fa
	u.Filter.Apply(nvm, ty, u.Name())

	//
	for _, p := range u.Select {
		//
		switch x := p.Edge.(type) {

		case ScalarPred, *Variable: // R.p ignore, already processed

		case *UidPred: // []R.N - child's uid-pred, contained in nv

			for _, p := range x.Select {

				switch x := p.Edge.(type) {

				case ScalarPred:
					// do nothing as UnmarshalNodeCode has already saved scalar results in nv

				case *UidPred:
					// NV entry contains child UIDs i.e nv[upred].Value -> [][][]byte
					var (
						ok  bool
						aty blk.TyAttrD
						nds [][][]byte
					)
					// get type of the uid-pred
					if aty, ok = cache.TypeC.TyAttrC[ty+":"+x.Name()]; !ok {
						continue // ignore this attribute as it is not in current type
					}
					// results not in nv for this depth in graph. Must query uids stored in nv[i].Value -> [][][]byte
					data := nvm[x.Name()]
					if nds, ok = data.Value.([][][]byte); !ok {
						panic(fmt.Errorf("filterRootResult: data.Value is of wrong type"))
					}

					for i, u := range nds {
						for j, uid := range u {

							if data.State[i][j] == 1 {
								continue // soft delete set or failed filter condition
							}

							grl.Ask()
							<-grl.RespCh()

							go x.execNode(grl, util.UID(uid), aty.Ty)
						}
					}

				}
			}
		}
	}
	u.nv = nvc

}
