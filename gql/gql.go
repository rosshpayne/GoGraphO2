package gql

import (
	//	"github.com/DynamoGraph/gql/lexer"
	"github.com/DynamoGraph/cache"
	"github.com/DynamoGraph/gql/parser"
	"github.com/DynamoGraph/rdf/grmgr"
)

func Execute(query string) {

	wpStart.Add(1)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	wpEnd.Add(1)
	ctxEnd.Add(1)
	// l := lexer.New(input)
	// p := New(l)
	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)

	wpStart.Wait()
	syslog(fmt.Sprintf(" goroutine started "))

	// 	cores=
	// 	rtpercore=

	golimiter := grmgr.New("execute", 4) // cores*rtpercore)

	p := parser.New(query)
	// *ast.RootStmt, []error)
	stmt, errs := p.ParseInput()
	if len(errs) > 0 {
		panic(errs[0])
	}

	result = stmt.Execute(golimiter) // []pkey,sortk,ty

	if len(result) == 0 {
		return
	}
	stmt.Output(golimiter)
	//

	fmt.Println(d)
	syslog("execute exits.....")

}

func fetchNodeData(r db.QResult) {
	//
	// fetch data
	//
	var fr *cache.NodeCache
	nc, err := cache.FetchNode(r.Pkey, "A#")
	if err != nil {
		panic(err)
	}
	//
	// build NV - based on predicates from Stmt
	//
	var (
		nv     ds.ClientNV
		cUpred string
		nvName string
	)
	tyAC := cache.TypeCache.TyAttrCache
	//
	for _, t := range result {
		//
		for _, v := range stmt.Predicates {

			longTy, _ = cache.GetTyLongNm(t)
			a := tyAC[longTy+":"+v]

			if len(a.Ty) == 0 {
				// scalar
				if len(cUpred) > 0 {
					nvName = cUpred + ":" + a.Name
				} else {
					nvName = a.Name
				}
				nv = append(nv, ds.NV{Name: nvName})

			} else {
				// uid-pred : uid-pred:
				if len(cUpred) > 0 {
					cUpred += ":" + a.Name
				} else {
					cUpred = a.Name
				}
				nv = append(nv, ds.NV{Name: cUpred})
			}
		}
	}
	//
	// unmarshal cache into NV
	//
	err = nc.UnmarshalCache(nv)
	if err != nil {
		t.Fatal(err)
	}

}
