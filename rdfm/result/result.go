package result

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	slog "github.com/DynamoGraph/syslog"
)

const (
	edge = 1
	node = 2
)

type Subject = string

type Result struct {
	subject string
	Cnt     int
}

const syslogId = "output "

/////////////////////////////////////
//
// register gSubject start
//
type rCntMap map[Subject]int

var rCnt rCntMap

var register = make(chan *Result)
var Log = make(chan *Result)
var Print = make(chan struct{})

func init() {
	rCnt = make(rCntMap)
}

func New(r string) *Result {

	l := &Result{subject: r}
	register <- l
	return l
}

type output []string

func (o output) Less(i, j int) bool {
	return o[i] < o[j]
}

func (o output) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}
func (o output) Len() int {
	return len(o)
}

var out output

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()

	slog.Log(syslogId, "Powering on...")
	wp.Done()

	for {

		select {

		case r := <-register: // change the ceiling by passing in Limiter struct. As struct is a non-ref type, l is a copy of struct passed into channel. Ref typs, spmfc - slice, pointer, map, func, channel

			if _, ok := rCnt[r.subject]; !ok {
				rCnt[r.subject] = 0
			}

		case r := <-Log:

			rCnt[r.subject] += r.Cnt

		case <-Print:

			slog.On()
			slog.Log(syslogId, " Printing...")
			for k := range rCnt {
				if strings.Index(k, "->") == -1 {
					// node
					out = append(out, k)
				}
			}
			sort.Sort(out)
			var (
				cnt     int
				printed []string
			)
			printed = append(printed, out...)

			for _, v := range out {
				cnt = rCnt[v]
				printed = append(printed, v)
				slog.Log(syslogId, fmt.Sprintf("%s:    %d\n", v, cnt))
				for vv, cnt := range rCnt {
					if strings.Index(vv, "->") > -1 {
						if v == vv[:strings.Index(vv, "->")] {
							printed = append(printed, vv)
							slog.Log(syslogId, fmt.Sprintf("%s:    %d\n", vv, cnt))
						}
					}
				}
			}
			// print those log entries not already output
			var found bool
			for k, v := range rCnt {
				found = false
				for _, printed := range printed {
					if k == printed {
						found = true
						break
					}
				}
				if !found {
					slog.Log(syslogId, fmt.Sprintf("%s:    %d\n", k, v))
				}
			}

		case <-ctx.Done():

			// TODO: Done should be in a separate select. If a request and Done occur simultaneously then go will randomly pick one.
			// separating them means we have control. Is that the solution. Ideally we should control outside of uuid func().
			slog.Log(syslogId, "Powering down...")
			return

		}

	}
}
