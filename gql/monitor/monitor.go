package monitor

import (
	"context"
	"fmt"
	"sync"

	slog "github.com/DynamoGraph/syslog"
)

const (
	Candidate = iota
	PassRootFilter
	TouchNode
	TouchLvl
	NodeFetch
	DBFetch
	AttachNode
	DetachNode
	LIMIT
)

type Stat struct {
	Id    int
	Lvl   int
	Value interface{}
}

var (
	StatCh  chan Stat
	ClearCh chan struct{}
	stats   []interface{}
)

func PowerOn(ctx context.Context, wps *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	wps.Done()

	slog.Log("monitor: ", "Powering on...")

	//
	// initialisation
	//
	stats := make([]interface{}, LIMIT, LIMIT)
	a := make([]int, 1, 1)
	stats[TouchLvl] = a

	StatCh = make(chan Stat)
	ClearCh = make(chan struct{})

	var (
		n int
		s Stat
	)
	//
	// Note: Select on channel can be a performance killer if not implemented correctly
	//       Better to keep ctx.Done() in main select.
	//       Probing both ctx.Done() and StatCH channel eats CPU and increases Dynamodb API response times by x10.
	//       Test cases go from 50ms to 250ms
	//
	for {

		select {

		case s = <-StatCh:

			//	Default behaviour for a int stat is simply increment it.
			//
			switch x := s.Id; x {

			case TouchNode:
				if stats[x] == nil {
					stats[x] = 1
					a := stats[TouchLvl].([]int)
					if len(a) <= s.Lvl+1 {
						a = append(a, 0)
						stats[TouchLvl] = a
					}
					a[s.Lvl] += 1
				} else {
					n, _ = stats[x].(int)
					stats[x] = n + 1
					a := stats[TouchLvl].([]int)
					if len(a) <= s.Lvl+1 {
						a = append(a, 0)
						stats[TouchLvl] = a
					}
					a[s.Lvl] += 1
				}

			default:
				if stats[x] == nil {
					stats[x] = 1
				} else {
					switch n := stats[s.Id].(type) {
					case int64:
						stats[s.Id] = n + 1
					case int:
						stats[s.Id] = n + 1
					}
				}
			}

		case <-ClearCh:

		case <-ctx.Done():

			slog.Log("monitor: ", "Powering down...")
			fmt.Printf("monitor: %#v\n", stats)
			slog.Log("monitor: ", fmt.Sprintf("monitor: %#v\n", stats))
			return

		}
	}
}
