package ast

import (
	"fmt"
	"strings"
	//	"strings"
	"github.com/DynamoGraph/ds"
	mon "github.com/DynamoGraph/gql/monitor"
	"github.com/DynamoGraph/util"
)

func (r *RootStmt) Output() {
	//
	// execute root func - get back slice of unfiltered results
	//
	fmt.Printf("in output....nodes: %d \n", len(r.nodesc), len(r.nodes))
	//
	// foreach uid in root node map
	//
	for uid, nvc := range r.nodesc {

		stat := mon.Stat{Id: mon.TouchNode, Lvl: 0}
		mon.StatCh <- stat

		nvm := r.nodes[uid]

		fmt.Printf("{ \n uid: %q ,\n", uid)
		fmt.Println(" data: {")

		for _, s := range r.Select {

			switch x := s.Edge.(type) {

			case *ScalarPred:

				nv := nvm[x.Name()]
				switch x := nv.Value.(type) {
				case int64:
					fmt.Printf("%s%s : %v,\n", strings.Repeat("\t", 1), nv.Name, x)
				case string:
					fmt.Printf("%s%s : %q,\n", strings.Repeat("\t", 1), nv.Name, x)
				case float64:
					fmt.Printf("%s%s : %f,\n", strings.Repeat("\t", 1), nv.Name, x)
				default:
					fmt.Printf("%s %s : %v,\n", strings.Repeat("\t", 1), nv.Name, x)
				}

			case *UidPred: // child of child, R.N.N
				// grab the predicates (scalars) belonging to uid-pred x: e.g. Friends:Name, Friednds:Age
				var spred []*ds.NV

				for _, v := range nvc {
					if !(strings.Index(v.Name, ":") > -1 && v.Name[:strings.Index(v.Name, ":")] == x.Name() && len(v.Name) > len(x.Name())+1) {
						continue
					}
					spred = append(spred, v)
				}
				//
				// get child uids that belong to edge x and print out the scalar attributes for x
				//
				var s strings.Builder
				fmt.Printf("%s%s : [ \n", strings.Repeat("\t", 1), x.Name())

				v := nvm[x.Name()+":"]
				uids := v.Value.([][][]byte)

				for i, uidS := range uids {
					for j, v := range uidS {
						s.Reset()
						//
						// increment touch counter
						//
						stat := mon.Stat{Id: mon.TouchNode, Lvl: x.lvl}
						mon.StatCh <- stat
						s.WriteString(fmt.Sprintf("%s{ \n", strings.Repeat("\t", 2)))
						s.WriteString(fmt.Sprintf("%sidx: { i: %d, j: %d }\n", strings.Repeat("\t", 2), i, j))
						s.WriteString(fmt.Sprintf("%suid: %s\n", strings.Repeat("\t", 2), util.UID(v).String()))
						for _, scalar := range spred {

							pred := scalar.Name[strings.Index(scalar.Name, ":")+1:] // Friends:Age

							switch z := scalar.Value.(type) {
							case [][]string:
								s.WriteString(fmt.Sprintf("%s%s: %s,\n", strings.Repeat("\t", 2), pred, z[i][j]))
							case [][]int64:
								s.WriteString(fmt.Sprintf("%s%s: %d,\n", strings.Repeat("\t", 2), pred, z[i][j]))
							case [][]float64:
								s.WriteString(fmt.Sprintf("%s%s: %g,\n", strings.Repeat("\t", 2), pred, z[i][j]))
							case [][]bool:
								s.WriteString(fmt.Sprintf("%s%s: %v,\n", strings.Repeat("\t", 2), pred, z[i][j]))
								// TODO: what about other data types, sets in particular SS,SN..
							}
						}
						fmt.Print(s.String())
						//
						// walk the graph using uid-pred attributes belonging to edge x.
						// Output will print the scalar values associated with each child node of x.
						//
						for _, p := range x.Select {
							if y, ok := p.Edge.(*UidPred); ok {
								// only need to run output once for all uid-pred's in x
								y.output(v)
								break
							}
						}
						fmt.Printf("%s}, \n", strings.Repeat("\t", 2))
					}
				}
			}
		}
	}
}

// 	fmt.Println("Output root:   ")

// }

func (u *UidPred) output(uid_ []uint8) {

	uid := util.UID(uid_).String()
	//
	nvc, ok := u.nodesc[uid]
	if !ok {
		panic(fmt.Errorf("Error in u.output. uid %q not in nodesc for %s", uid, u.Name()))
	}
	nvm, ok := u.nodes[uid]
	if !ok {
		panic(fmt.Errorf("Error in u.output. uid %q not in nodes for %s", uid, u.Name()))
	}

	upred := u.Parent.(*UidPred)
	for _, s := range upred.Select {

		switch x := s.Edge.(type) {

		case *UidPred: // child of child, R.N.N

			var spred []*ds.NV

			for _, v := range nvc {
				if !(strings.Index(v.Name, ":") > -1 && v.Name[:strings.Index(v.Name, ":")] == x.Name() && len(v.Name) > len(x.Name())+1) {
					continue
				}
				spred = append(spred, v)
			}
			//
			// get child uids that belong to edge x and print out the scalar attributes for x
			//
			var s strings.Builder
			fmt.Printf("%s%s : [ \n", strings.Repeat("\t", u.lvl), x.Name())

			v := nvm[x.Name()+":"]
			uids := v.Value.([][][]byte)

			for i, uidS := range uids {
				for j, v := range uidS {
					//fmt.Printf("i, j, UID: %d %d, %s", i, j, util.UID(v).String())
					s.Reset()
					stat := mon.Stat{Id: mon.TouchNode, Lvl: x.lvl}
					mon.StatCh <- stat
					s.WriteString(fmt.Sprintf("%s{ \n", strings.Repeat("\t", u.lvl+1)))
					s.WriteString(fmt.Sprintf("%sidx: { i: %d, j: %d }\n", strings.Repeat("\t", u.lvl+1), i, j))
					s.WriteString(fmt.Sprintf("%suid: %s\n", strings.Repeat("\t", u.lvl+1), util.UID(v).String()))

					for _, scalar := range spred {

						pred := scalar.Name[strings.Index(scalar.Name, ":")+1:]
						switch z := scalar.Value.(type) {
						case [][]string:
							s.WriteString(fmt.Sprintf("%s%s: %s,\n", strings.Repeat("\t", u.lvl+1), pred, z[i][j]))
						case [][]int64:
							s.WriteString(fmt.Sprintf("%s%s: %d,\n", strings.Repeat("\t", u.lvl+1), pred, z[i][j]))
						case [][]float64:
							s.WriteString(fmt.Sprintf("%s%s: %g,\n", strings.Repeat("\t", u.lvl+1), pred, z[i][j]))
						case [][]bool:
							s.WriteString(fmt.Sprintf("%s%s: %v,\n", strings.Repeat("\t", u.lvl+1), pred, z[i][j]))
							// TODO: what about other data types, sets in particular SS,SN..
						}
					}
					s.WriteString(fmt.Sprintf("%s}, \n", strings.Repeat("\t", u.lvl+1)))
					fmt.Print(s.String())
					//
					// walk the graph using uid-pred attributes belonging to edge x.
					// Output will print the scalar values associated with each child node of x.
					//
					for _, p := range x.Select {

						if y, ok := p.Edge.(*UidPred); ok {
							// only need to run output once for all uid-pred's in x. Once filter is incorporated this will change.
							y.output(v)
							break
						}
					}
				}
			}
		}
	}
}
