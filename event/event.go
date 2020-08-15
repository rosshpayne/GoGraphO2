package event

import (
	"fmt"

	"github.com/DynamoGraph/util"
)

func New() (util.UID, error) {

	// eventlock = new(eventLock)
	// eventlock.Lock()

	// create event UID
	uid, err := util.MakeUID()
	if err != nil {
		return nil, fmt.Errorf("Failed to make event UID for Event New(): %w", err)
	}
	return uid, nil
}

type Event interface {
	event_()
}

type EventMeta struct {
	EID    util.UID
	SEQ    int
	OP     string
	Status string
	Start  string
	Dur    string
	Err    string
}

func (e EventMeta) event_() {}

type AttachNode struct {
	EventMeta
	CID []byte
	PID []byte
	SK  string
}

type DetachNode struct {
	EventMeta
	CID []byte
	PID []byte
	SK  string
}
