package store

import (
	"errors"
	"sync"
	"time"

	"gwpool/internal/types"
)

const (
	slotWriteQueueSize = 128
	slotWriteBatchSize = 64
	slotWriteBatchWait = time.Millisecond
)

type slotMutation struct {
	account int64
	gateway string
	mutate  func(*types.Slot)
}

type slotWriteRequest struct {
	mutation slotMutation
	done     chan error
}

type slotWriter struct {
	// Keep admission through completion: Close waits for all admitted callers
	// before closing the queue, so no request can be stranded behind shutdown.
	admission sync.RWMutex
	closed    bool
	queue     chan slotWriteRequest
	done      chan struct{}
}

func (s *Store) startSlotWriter() {
	s.slotWriter.queue = make(chan slotWriteRequest, slotWriteQueueSize)
	s.slotWriter.done = make(chan struct{})
	go s.writeSlots()
}

func (s *Store) queueSlotWrite(mutation slotMutation) error {
	writer := &s.slotWriter
	writer.admission.RLock()
	defer writer.admission.RUnlock()
	if writer.closed {
		return errors.New("state store is closed")
	}
	request := slotWriteRequest{mutation: mutation, done: make(chan error, 1)}
	writer.queue <- request
	return <-request.done
}

func (s *Store) writeSlots() {
	defer close(s.slotWriter.done)
	for first := range s.slotWriter.queue {
		batch := []slotWriteRequest{first}
		timer := time.NewTimer(slotWriteBatchWait)
	collect:
		for len(batch) < slotWriteBatchSize {
			select {
			case next, open := <-s.slotWriter.queue:
				if !open {
					break collect
				}
				batch = append(batch, next)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		s.commitSlots(batch)
	}
}

func (s *Store) commitSlots(batch []slotWriteRequest) {
	s.mu.Lock()
	type beforeImage struct {
		slot types.Slot
		had  bool
	}
	before := make(map[string]beforeImage, len(batch))
	changed := make(map[string]types.Slot, len(batch))
	var err error
	for _, request := range batch {
		mutation := request.mutation
		key := SlotKey(mutation.account, mutation.gateway)
		old, had := s.st.Slots[key]
		if _, captured := before[key]; !captured {
			before[key] = beforeImage{old, had}
		}
		row := old
		if !had {
			row = types.Slot{AccountID: mutation.account, Gateway: mutation.gateway, LastVerdict: types.VerdictUnknown}
		}
		row, err = runSlotMutation(row, mutation.mutate)
		if err != nil {
			break
		}
		row.AccountID, row.Gateway = mutation.account, mutation.gateway
		if had && row == old {
			continue
		}
		s.st.Slots[key] = row
		changed[key] = row
	}
	if err == nil && len(changed) > 0 {
		// One synced record publishes the final state of all mutations in this
		// batch. Same-key mutations execute in queue order under the same lock.
		err = s.save(stateChange{Slots: changed})
	}
	if err != nil {
		for key, image := range before {
			if image.had {
				s.st.Slots[key] = image.slot
			} else {
				delete(s.st.Slots, key)
			}
		}
	}
	s.mu.Unlock()
	for _, request := range batch {
		request.done <- err
	}
}

func runSlotMutation(row types.Slot, mutate func(*types.Slot)) (out types.Slot, err error) {
	out = row
	defer func() {
		if recover() != nil {
			// A callback used to execute under the request's panic boundary.
			// Moving it to the writer must not crash the process or strand ACKs.
			err = errors.New("slot mutation callback panicked")
		}
	}()
	mutate(&out)
	return out, nil
}

func (s *Store) stopSlotWriter() {
	writer := &s.slotWriter
	writer.admission.Lock()
	defer writer.admission.Unlock()
	writer.closed = true
	close(writer.queue)
	<-writer.done
}
