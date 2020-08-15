package db

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// var (
// 	// error categories - returned from Unwrap()
// 	NoItemFoundErr  = errors.New("no item found")
// 	SystemErr       = errors.New("DB system error")
// 	MarshalingErr   = errors.New("DB marshaling error")
// 	UnmarshalingErr = errors.New("DB unmarshaling error")
// )

type DBExprErr struct {
	routine string
	pkey    string
	sortk   string
	err     error // aws dynamo expression error,InvalidParameterError, UnsetParameterError use errors.As
}

func newDBExprErr(rt string, pk string, sk string, err error) error {
	er := DBExprErr{routine: rt, pkey: pk, sortk: sk, err: err}
	logerr(er)
	return er
}

func (e DBExprErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Expression error in %s [%s, $s]. %s", e.routine, e.pkey, e.sortk, e.err.Error())
	}
	if len(e.pkey) > 0 {
		return fmt.Sprintf("Expression error in %s [%s]. %s", e.routine, e.pkey, e.err.Error())
	}
	return fmt.Sprintf("Expression error in %s. %s", e.routine, e.err.Error())
}

func (e DBExprErr) Unwrap() error {
	return e.err
}

type DBSysErr struct {
	routine string
	api     string // DB statement
	err     error  // aws database error
}

func newDBSysErr(rt string, api string, err error) error {
	syserr := DBSysErr{routine: rt, api: api, err: err}
	logerr(syserr)
	return syserr
}

func (e DBSysErr) Unwrap() error {
	return e.err
}

var ErrItemSizeExceeded = errors.New("Item size has exceeded the maximum allowed size")

func (e DBSysErr) Error() string {

	var aerr awserr.Error
	if errors.As(e.err, &aerr) {
		if aerr.Message() == "Item size has exceeded the maximum allowed size" {
			return fmt.Sprintf("DB system error: %s in %s of %s. %w", aerr.Code(), e.api, e.routine, ErrItemSizeExceeded)
		}
		return fmt.Sprintf("DB system error: %s in %s of %s. %s", aerr.Code(), e.api, e.routine, aerr.Message())
	}
	return fmt.Sprintf("DB Sytem error in %s of %s. %s", e.api, e.routine, e.Error())
}

type DBNoItemFound struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
}

func newDBNoItemFound(rt string, pk string, sk string, api string) error {
	e := DBNoItemFound{routine: rt, pkey: pk, sortk: sk, api: api}
	logerr(e)
	return e
}

func (e DBNoItemFound) Error() string {

	if e.api == "Scan" {
		return fmt.Sprintf("No item found during %s operation in %s [%q]", e.api, e.routine, e.pkey)
	}
	if len(e.sortk) > 0 {
		return fmt.Sprintf("No item found during %s in %s for Pkey %q, Sortk %q", e.api, e.routine, e.pkey, e.sortk)
	}
	return fmt.Sprintf("No item found during %s in %s for Pkey %q", e.api, e.routine, e.pkey)

}

type DBMarshalingErr struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
	err     error  // aws database error
}

func newDBMarshalingErr(rt string, pk string, sk string, api string, err error) error {
	e := DBMarshalingErr{routine: rt, pkey: pk, sortk: sk, api: api, err: err}
	logerr(e)
	return e
}

func (e DBMarshalingErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Marshalling error during %s in %s. [%q, %q]. Error: ", e.api, e.routine, e.pkey, e.sortk, e.pkey, e.err.Error())
	}
	return fmt.Sprintf("Marshalling error during %s in %s. [%q]. Error: ", e.api, e.routine, e.pkey, e.err.Error())
}

func (e DBMarshalingErr) Unwrap() error {
	return e.err
}

type DBUnmarshalErr struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
	err     error  // aws database error
}

func newDBUnmarshalErr(rt string, pk string, sk string, api string, err error) error {
	e := DBUnmarshalErr{routine: rt, pkey: pk, sortk: sk, api: api, err: err}
	logerr(e)
	return e
}

func (e DBUnmarshalErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Unmarshalling error during %s in %s. [%q, %q]. Error: %s ", e.api, e.routine, e.pkey, e.sortk, e.err.Error())
	}
	return fmt.Sprintf("Unmarshalling error during %s in %s. [%q]. Error: %s ", e.api, e.routine, e.pkey, e.err.Error())
}

func (e DBUnmarshalErr) Unwrap() error {
	return e.err
}
