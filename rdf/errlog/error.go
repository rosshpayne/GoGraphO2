package errlog

import (
	"context"
	"sync"

	slog "github.com/DynamoGraph/syslog"
)

type errorS []error

var (
	errors     errorS
	AddCh      chan error
	ListCh     chan error
	ClearCh    chan struct{}
	PrintLogCh chan errorS
)

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()

	slog.Log("errlog: ", "Powering on...")
	wp.Done()

	AddCh = make(chan error)
	ListCh = make(chan error)
	ClearCh = make(chan struct{})
	PrintLogCh = make(chan errorS)

	for {

		select {

		case e := <-AddCh:

			errors = append(errors, e)

		case <-ListCh:

			PrintLogCh <- errors

		case <-ClearCh:

			errors = nil

		case <-ctx.Done():
			slog.Log("errlog: ", "Powering down...")
			return

		}
	}
}
