package client

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/rs/zerolog/log"
	"sync"
	"sync/atomic"
	"time"
)

type inFlightRequestsHandler struct {
	connectionId string
	maxInFlight  int
	maxPending   int
	timeout      time.Duration
	streamIds    chan int16
	inFlight     map[int16]*inFlightRequest
	inFlightLock *sync.RWMutex
	closed       int32
}

func (h *inFlightRequestsHandler) String() string {
	return fmt.Sprintf("%v: [in-flight handler]", h.connectionId)
}

func newInFlightRequestsHandler(connectionId string, maxInFlight int, maxPending int, timeout time.Duration) *inFlightRequestsHandler {
	handler := &inFlightRequestsHandler{
		connectionId: connectionId,
		maxInFlight:  maxInFlight,
		maxPending:   maxPending,
		timeout:      timeout,
		streamIds:    make(chan int16, maxInFlight),
		inFlight:     make(map[int16]*inFlightRequest, maxInFlight),
		inFlightLock: &sync.RWMutex{},
	}
	for i := 1; i <= maxInFlight; i++ {
		handler.streamIds <- int16(i)
	}
	return handler
}

func (h *inFlightRequestsHandler) onOutgoingFrameEnqueued(f *frame.Frame) (InFlightRequest, error) {
	if h.isClosed() {
		return nil, fmt.Errorf("%v: handler closed", h)
	}
	var err error
	streamId := f.Header.StreamId
	managedStreamId := streamId == ManagedStreamId
	if managedStreamId {
		if streamId, err = h.borrowStreamId(); err != nil {
			return nil, err
		} else {
			f.Header.StreamId = streamId
		}
	}
	h.inFlightLock.RLock()
	if len(h.inFlight) == h.maxInFlight {
		err = fmt.Errorf("%v: too many in-flight requests: %v", h, h.maxInFlight)
	} else if _, found := h.inFlight[streamId]; found {
		err = fmt.Errorf("%v: stream id already in use: %d", h, streamId)
	}
	h.inFlightLock.RUnlock()
	if err == nil {
		var inFlight *inFlightRequest
		inFlight, err = h.addInFlight(streamId, managedStreamId)
		if err == nil {
			inFlight.startTimeout()
			return inFlight, nil
		}
	}
	return nil, err
}

func (h *inFlightRequestsHandler) onIncomingFrameReceived(f *frame.Frame) error {
	if h.isClosed() {
		return fmt.Errorf("%v: handler closed", h)
	}
	streamId := f.Header.StreamId
	var err error
	var inFlight *inFlightRequest
	var found bool
	h.inFlightLock.RLock()
	if inFlight, found = h.inFlight[streamId]; !found {
		err = fmt.Errorf("%v: unknown stream id: %d", h, streamId)
	}
	h.inFlightLock.RUnlock()
	if err == nil {
		if isLastFrame(f) {
			h.removeInFlight(streamId)
			if inFlight.managedStreamId {
				if err := h.releaseStreamId(streamId); err != nil {
					return err
				}
			}
		}
		err = inFlight.onFrameReceived(f)
	}
	return err
}

func (h *inFlightRequestsHandler) addInFlight(streamId int16, managedStreamId bool) (*inFlightRequest, error) {
	inFlight := newInFlightRequest(h.String(), streamId, managedStreamId, h.maxPending, h.timeout)
	h.inFlightLock.Lock()
	defer h.inFlightLock.Unlock()
	if h.isClosed() {
		return nil, fmt.Errorf("%v: handler closed", h)
	}
	h.inFlight[streamId] = inFlight
	return inFlight, nil
}

func (h *inFlightRequestsHandler) removeInFlight(streamId int16) {
	h.inFlightLock.Lock()
	defer h.inFlightLock.Unlock()
	if _, found := h.inFlight[streamId]; found {
		delete(h.inFlight, streamId)
	}
}

func (h *inFlightRequestsHandler) borrowStreamId() (int16, error) {
	if h.isClosed() {
		return -1, fmt.Errorf("%v: handler closed", h)
	}
	select {
	case id, ok := <-h.streamIds:
		if !ok {
			return -1, fmt.Errorf("%v: handler closed", h)
		}
		log.Debug().Msgf("%v: borrowed stream id: %v", h, id)
		return id, nil
	default:
		return -1, fmt.Errorf("%v: no stream id available", h)
	}
}

func (h *inFlightRequestsHandler) releaseStreamId(id int16) error {
	if h.isClosed() {
		return fmt.Errorf("%v: handler closed", h)
	}
	select {
	case h.streamIds <- id:
		log.Debug().Msgf("%v: released stream id: %v", h, id)
		return nil
	default:
		return fmt.Errorf("%v: stream id %d: release failed", h, id)
	}
}

func (h *inFlightRequestsHandler) isClosed() bool {
	return atomic.LoadInt32(&h.closed) == 1
}

func (h *inFlightRequestsHandler) setClosed() bool {
	return atomic.CompareAndSwapInt32(&h.closed, 0, 1)
}

func (h *inFlightRequestsHandler) close() {
	if h.setClosed() {
		log.Trace().Msgf("%v: closing", h)
		h.inFlightLock.Lock()
		for streamId, inFlight := range h.inFlight {
			delete(h.inFlight, streamId)
			inFlight.close(fmt.Errorf("%v: handler closed", h))
		}
		h.inFlightLock.Unlock()
		streamIds := h.streamIds
		h.streamIds = nil
		close(streamIds)
		log.Trace().Msgf("%v: successfully closed", h)
	}
}

type inFlightRequest struct {
	handlerId       string
	streamId        int16
	managedStreamId bool
	_incoming       chan *frame.Frame // used internally; will be set to nil on close
	incoming        chan *frame.Frame // exposed externally; never nil
	err             error
	closed          int32
	timeout         time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	timeoutCtx      context.Context
	timeoutCancel   context.CancelFunc

	// lock guards the closing of incoming chan and the assignment of err;
	// required to fulfill the interface contract:
	// if Incoming() is closed because of an error, Err() must return that error.
	lock *sync.RWMutex
}

func (r *inFlightRequest) StreamId() int16 {
	return r.streamId
}

func (r *inFlightRequest) Incoming() <-chan *frame.Frame {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.incoming
}

func (r *inFlightRequest) Err() error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.err
}

func newInFlightRequest(handlerId string, streamId int16, managedStreamId bool, maxPending int, timeout time.Duration) *inFlightRequest {
	ctx, cancel := context.WithCancel(context.Background())
	incoming := make(chan *frame.Frame, maxPending)
	return &inFlightRequest{
		handlerId:       handlerId,
		streamId:        streamId,
		managedStreamId: managedStreamId,
		_incoming:       incoming,
		incoming:        incoming,
		timeout:         timeout,
		ctx:             ctx,
		cancel:          cancel,
		lock:            &sync.RWMutex{},
	}
}

func (r *inFlightRequest) String() string {
	return fmt.Sprintf("%v [stream id %d]", r.handlerId, r.streamId)
}

func (r *inFlightRequest) onFrameReceived(f *frame.Frame) error {
	select {
	case r._incoming <- f:
		if isLastFrame(f) {
			r.stopTimeout()
			r.close(nil)
		} else {
			r.resetTimeout()
		}
		return nil
	case <-r.ctx.Done():
		return fmt.Errorf("%v: request closed", r)
	default:
		err := fmt.Errorf("%v: too many pending incoming frames: %d", r, len(r.incoming))
		r.close(err)
		return err
	}
}

func (r *inFlightRequest) startTimeout() {
	r.timeoutCtx, r.timeoutCancel = context.WithTimeout(r.ctx, r.timeout)
	log.Trace().Msgf("%v: timeout started", r)
	go func() {
		select {
		case <-r.timeoutCtx.Done():
			switch r.timeoutCtx.Err() {
			case context.DeadlineExceeded:
				err := fmt.Errorf("%v: timed out waiting for incoming frames", r)
				r.close(err)
			case context.Canceled:
				log.Trace().Msgf("%v: timeout canceled", r)
			}
		}
	}()
}

func (r *inFlightRequest) stopTimeout() {
	if r.timeoutCtx != nil {
		r.timeoutCancel()
	}
}

func (r inFlightRequest) resetTimeout() {
	r.stopTimeout()
	r.startTimeout()
}

func (r *inFlightRequest) isClosed() bool {
	return atomic.LoadInt32(&r.closed) == 1
}

func (r *inFlightRequest) setClosed() bool {
	return atomic.CompareAndSwapInt32(&r.closed, 0, 1)
}

func (r *inFlightRequest) close(err error) {
	if r.setClosed() {
		log.Trace().Msgf("%v: closing", r)
		r.cancel()
		r.lock.Lock()
		// set _incoming to nil first to avoid potential panic in onFrameReceived
		r._incoming = nil
		close(r.incoming)
		r.err = err
		r.lock.Unlock()
		log.Trace().Msgf("%v: successfully closed", r)
	}
}

func isLastFrame(f *frame.Frame) bool {
	if f.Header.OpCode == primitive.OpCodeResult {
		result := f.Body.Message.(message.Result)
		if result.GetResultType() == primitive.ResultTypeRows {
			rows := result.(*message.RowsResult)
			if rows.Metadata.Flags()&primitive.RowsFlagDseContinuousPaging != 0 {
				return rows.Metadata.LastContinuousPage
			}
		}
	}
	return true
}
