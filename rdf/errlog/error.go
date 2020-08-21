package errlog

import (
	"context"
	"sync"

	slog "github.com/DynamoGraph/syslog"
)

type ErrorS []error

var (
	Add        chan error
	ListCh     chan error
	ClearCh    chan struct{}
	PrintLogCh chan ErrorS
	checkLimit chan chan bool
	ListReqCh  chan chan ErrorS
	AddBatch   chan []error
)

func CheckLimit(lc chan bool) {
	checkLimit <- lc
}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()

	slog.Log("errlog: ", "Powering on...")
	wp.Done()

	var (
		errors   ErrorS
		errLimit = 5
		e        error
		eb       []error
		lc       chan bool
		l        chan ErrorS
	)

	Add = make(chan error)
	ListReqCh = make(chan chan ErrorS)
	ClearCh = make(chan struct{})
	PrintLogCh = make(chan ErrorS)
	checkLimit = make(chan chan bool)
	AddBatch = make(chan []error)

	for {

		select {

		case e = <-Add:

			errors = append(errors, e)

		case eb = <-AddBatch:

			errors = append(errors, eb...)

		case lc = <-checkLimit:

			lc <- len(errors) > errLimit

		case l = <-ListReqCh:

			l <- errors

		case <-ClearCh:

			errors = nil

		case <-ctx.Done():
			slog.Log("errlog: ", "Powering down...")
			return

		}
	}
}
