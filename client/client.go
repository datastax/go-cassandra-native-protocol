// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
	"github.com/rs/zerolog/log"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultConnectTimeout = time.Second * 5
	DefaultReadTimeout    = time.Second * 12
)

const (
	DefaultMaxInFlight = 1024
	DefaultMaxPending  = 10
)

const ManagedStreamId int16 = 0

// EventHandler An event handler is a callback function that gets invoked whenever a CqlClientConnection receives an incoming
// event.
type EventHandler func(event *frame.Frame, conn *CqlClientConnection)

// CqlClient is a client for Cassandra-compatible backends. It is preferable to create CqlClient instances using the
// constructor function NewCqlClient. Once the client is created and properly configured, use Connect or ConnectAndInit
// to establish new connections to the server.
type CqlClient struct {
	// The remote contact point address to connect to.
	RemoteAddress string
	// The AuthCredentials for authenticated servers. If nil, no authentication will be used.
	Credentials *AuthCredentials
	// The compression to use; if unspecified, no compression will be used.
	Compression primitive.Compression
	// The maximum number of in-flight requests to apply for each connection created with Connect. Must be strictly
	// positive.
	MaxInFlight int
	// The maximum number of pending responses awaiting delivery to store per request. Must be strictly positive.
	// This is only useful when using continuous paging, a feature specific to DataStax Enterprise.
	MaxPending int
	// The timeout to apply when establishing new connections.
	ConnectTimeout time.Duration
	// The timeout to apply when waiting for incoming responses.
	ReadTimeout time.Duration
	// An optional list of handlers to handle incoming events.
	EventHandlers []EventHandler
}

// NewCqlClient Creates a new CqlClient with default options. Leave credentials nil to opt out from authentication.
func NewCqlClient(remoteAddress string, credentials *AuthCredentials) *CqlClient {
	return &CqlClient{
		RemoteAddress:  remoteAddress,
		Credentials:    credentials,
		MaxInFlight:    DefaultMaxInFlight,
		MaxPending:     DefaultMaxPending,
		ConnectTimeout: DefaultConnectTimeout,
		ReadTimeout:    DefaultReadTimeout,
	}
}

func (client *CqlClient) String() string {
	return fmt.Sprintf("CQL client [%v]", client.RemoteAddress)
}

// Connect establishes a new TCP connection to the client's remote address.
// Set ctx to context.Background if no parent context exists.
// The returned CqlClientConnection is ready to use, but one must initialize it manually, for example by calling
// CqlClientConnection.InitiateHandshake. Alternatively, use ConnectAndInit to get a fully-initialized connection.
func (client *CqlClient) Connect(ctx context.Context) (*CqlClientConnection, error) {
	log.Debug().Msgf("%v: connecting", client)
	dialer := net.Dialer{}
	connectCtx, connectCancel := context.WithTimeout(ctx, client.ConnectTimeout)
	defer connectCancel()
	if conn, err := dialer.DialContext(connectCtx, "tcp", client.RemoteAddress); err != nil {
		return nil, fmt.Errorf("%v: cannot establish TCP connection: %w", client, err)
	} else {
		log.Debug().Msgf("%v: new TCP connection established", client)
		if connection, err := newCqlClientConnection(
			conn,
			ctx,
			client.Credentials,
			client.Compression,
			client.MaxInFlight,
			client.MaxPending,
			client.ReadTimeout,
			client.EventHandlers,
		); err != nil {
			log.Err(err).Msgf("%v: cannot establish CQL connection", client)
			_ = conn.Close()
			return nil, err
		} else {
			log.Info().Msgf("%v: new CQL connection established: %v", client, connection)
			return connection, nil
		}
	}
}

// ConnectAndInit establishes a new TCP connection to the server, then initiates a handshake procedure using the
// specified protocol version. The CqlClientConnection connection will be fully initialized when this method returns.
// Use stream id zero to activate automatic stream id management.
// Set ctx to context.Background if no parent context exists.
func (client *CqlClient) ConnectAndInit(
	ctx context.Context,
	version primitive.ProtocolVersion,
	streamId int16,
) (*CqlClientConnection, error) {
	if connection, err := client.Connect(ctx); err != nil {
		return nil, err
	} else {
		return connection, connection.InitiateHandshake(version, streamId)
	}
}

// CqlClientConnection encapsulates a TCP client connection to a remote Cassandra-compatible backend.
// CqlClientConnection instances should be created by calling CqlClient.Connect or CqlClient.ConnectAndInit.
type CqlClientConnection struct {
	conn               net.Conn
	frameCodec         frame.Codec
	segmentCodec       segment.Codec
	compression        primitive.Compression
	modernLayout       bool
	readTimeout        time.Duration
	credentials        *AuthCredentials
	handlers           []EventHandler
	inFlightHandler    *inFlightRequestsHandler
	outgoing           chan *frame.Frame
	events             chan *frame.Frame
	waitGroup          *sync.WaitGroup
	closed             int32
	ctx                context.Context
	cancel             context.CancelFunc
	payloadAccumulator *payloadAccumulator
}

func newCqlClientConnection(
	conn net.Conn,
	ctx context.Context,
	credentials *AuthCredentials,
	compression primitive.Compression,
	maxInFlight int,
	maxPending int,
	readTimeout time.Duration,
	handlers []EventHandler,
) (*CqlClientConnection, error) {
	if conn == nil {
		return nil, fmt.Errorf("TCP connection cannot be nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context cannot be nil")
	}
	if maxInFlight < 1 {
		return nil, fmt.Errorf("max in-flight: expecting positive, got: %v", maxInFlight)
	} else if maxInFlight > math.MaxInt16 {
		return nil, fmt.Errorf("max in-flight: expecting <= %v, got: %v", math.MaxInt16, maxInFlight)
	}
	if maxPending < 1 {
		return nil, fmt.Errorf("max pending: expecting positive, got: %v", maxInFlight)
	}
	frameCodec := frame.NewCodecWithCompression(NewBodyCompressor(compression))
	segmentCodec := segment.NewCodecWithCompression(NewPayloadCompressor(compression))
	if compression == "" {
		compression = primitive.CompressionNone
	}
	connection := &CqlClientConnection{
		conn:         conn,
		frameCodec:   frameCodec,
		segmentCodec: segmentCodec,
		compression:  compression,
		readTimeout:  readTimeout,
		credentials:  credentials,
		handlers:     handlers,
		outgoing:     make(chan *frame.Frame, maxInFlight),
		events:       make(chan *frame.Frame, maxInFlight),
		waitGroup:    &sync.WaitGroup{},
		payloadAccumulator: &payloadAccumulator{
			frameCodec: frame.NewRawCodec(), // without compression
		},
	}
	connection.ctx, connection.cancel = context.WithCancel(ctx)
	connection.inFlightHandler = newInFlightRequestsHandler(connection.String(), connection.ctx, maxInFlight, maxPending, readTimeout)
	connection.incomingLoop()
	connection.outgoingLoop()
	connection.awaitDone()
	return connection, nil
}

func (c *CqlClientConnection) String() string {
	return fmt.Sprintf("CQL client conn [L:%v <-> R:%v]", c.conn.LocalAddr(), c.conn.RemoteAddr())
}

// LocalAddr returns the connection's local address (that is, the client address).
func (c *CqlClientConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the connection's remote address (that is, the server address).
func (c *CqlClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Credentials returns a copy of the connection's AuthCredentials, if any, or nil if no authentication was configured.
func (c *CqlClientConnection) Credentials() *AuthCredentials {
	if c.credentials == nil {
		return nil
	}
	return c.credentials.Copy()
}

type payloadAccumulator struct {
	targetLength    int
	accumulatedData []byte
	frameCodec      frame.RawCodec
}

func (a *payloadAccumulator) reset() {
	a.targetLength = 0
	a.accumulatedData = nil
}

func (c *CqlClientConnection) incomingLoop() {
	log.Debug().Msgf("%v: listening for incoming frames...", c)
	c.waitGroup.Add(1)
	go func() {
		abort := false
		for !abort && !c.IsClosed() {
			if source, err := c.waitForIncomingData(); err != nil {
				abort = c.reportConnectionFailure(err, true)
			} else if c.modernLayout {
				abort = c.readSegment(source)
			} else {
				abort = c.readFrame(source)
			}
		}
		c.waitGroup.Done()
		if abort {
			c.abort()
		}
	}()
}

func (c *CqlClientConnection) outgoingLoop() {
	log.Debug().Msgf("%v: listening for outgoing frames...", c)
	c.waitGroup.Add(1)
	go func() {
		abort := false
		for !abort && !c.IsClosed() {
			if outgoing, ok := <-c.outgoing; !ok {
				if !c.IsClosed() {
					log.Error().Msgf("%v: outgoing frame channel was closed unexpectedly, closing connection", c)
					abort = true
				}
				break
			} else {
				log.Debug().Msgf("%v: sending outgoing frame: %v", c, outgoing)
				if c.modernLayout {
					// TODO write coalescer
					abort = c.writeSegment(outgoing, c.conn)
				} else {
					abort = c.writeFrame(outgoing, c.conn)
				}
			}
		}
		c.waitGroup.Done()
		if abort {
			c.abort()
		}
	}()
}

func (c *CqlClientConnection) waitForIncomingData() (io.Reader, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	} else {
		return io.MultiReader(bytes.NewReader(buf), c.conn), nil
	}
}

func (c *CqlClientConnection) readSegment(source io.Reader) (abort bool) {
	if incoming, err := c.segmentCodec.DecodeSegment(source); err != nil {
		abort = c.reportConnectionFailure(err, true)
	} else if incoming.Header.IsSelfContained {
		log.Debug().Msgf("%v: received incoming self-contained segment: %v", c, incoming)
		abort = c.readSelfContainedSegment(incoming, abort)
	} else {
		log.Debug().Msgf("%v: received incoming multi-segment part: %v", c, incoming)
		abort = c.addMultiSegmentPayload(incoming.Payload)
	}
	return abort
}

func (c *CqlClientConnection) readSelfContainedSegment(incoming *segment.Segment, abort bool) bool {
	payloadReader := bytes.NewReader(incoming.Payload.UncompressedData)
	for payloadReader.Len() > 0 {
		if abort = c.readFrame(payloadReader); abort {
			break
		}
	}
	return abort
}

func (c *CqlClientConnection) addMultiSegmentPayload(payload *segment.Payload) (abort bool) {
	accumulator := c.payloadAccumulator
	if accumulator.targetLength == 0 {
		// First reader, read ahead to find the target length
		if header, err := accumulator.frameCodec.DecodeHeader(bytes.NewReader(payload.UncompressedData)); err != nil {
			log.Error().Err(err).Msgf("%v: error decoding first frame header in multi-segment payload, closing connection", c)
			return true
		} else {
			accumulator.targetLength = int(primitive.FrameHeaderLengthV3AndHigher + header.BodyLength)
		}
	}
	accumulator.accumulatedData = append(accumulator.accumulatedData, payload.UncompressedData...)
	if accumulator.targetLength == len(accumulator.accumulatedData) {
		// We've received enough data to reassemble the whole frame
		encodedFrame := bytes.NewReader(accumulator.accumulatedData)
		accumulator.reset()
		return c.readFrame(encodedFrame)
	}
	return false
}

func (c *CqlClientConnection) writeSegment(outgoing *frame.Frame, dest io.Writer) (abort bool) {
	// never compress frames individually when included in a segment
	outgoing.Header.Flags = outgoing.Header.Flags.Remove(primitive.HeaderFlagCompressed)
	encodedFrame := &bytes.Buffer{}
	if abort = c.writeFrame(outgoing, encodedFrame); abort {
		abort = true
	} else {
		seg := &segment.Segment{
			Header:  &segment.Header{IsSelfContained: true},
			Payload: &segment.Payload{UncompressedData: encodedFrame.Bytes()},
		}
		if err := c.segmentCodec.EncodeSegment(seg, dest); err != nil {
			abort = c.reportConnectionFailure(err, false)
		} else {
			log.Debug().Msgf("%v: outgoing segment successfully written: %v (frame: %v)", c, seg, outgoing)
		}
	}
	return abort
}

func (c *CqlClientConnection) readFrame(source io.Reader) (abort bool) {
	if incoming, err := c.frameCodec.DecodeFrame(source); err != nil {
		abort = c.reportConnectionFailure(err, true)
	} else {
		c.maybeSwitchToModernLayout(incoming)
		abort = c.processIncomingFrame(incoming)
	}
	return abort
}

func (c *CqlClientConnection) maybeSwitchToModernLayout(incoming *frame.Frame) {
	if !c.modernLayout &&
		incoming.Header.Version.SupportsModernFramingLayout() &&
		(isReady(incoming) || isAuthenticate(incoming)) {
		// Changing this value could be racy if some outgoing frame is being processed;
		// but in theory, this should never happen during handshake.
		log.Debug().Msgf("%v: switching to modern framing layout", c)
		c.modernLayout = true
	}
}

func (c *CqlClientConnection) writeFrame(outgoing *frame.Frame, dest io.Writer) (abort bool) {
	if err := c.frameCodec.EncodeFrame(outgoing, dest); err != nil {
		abort = c.reportConnectionFailure(err, false)
	} else {
		log.Debug().Msgf("%v: outgoing frame successfully written: %v", c, outgoing)
	}
	return abort
}

func (c *CqlClientConnection) reportConnectionFailure(err error, read bool) (abort bool) {
	if !c.IsClosed() {
		if errors.Is(err, io.EOF) {
			log.Info().Msgf("%v: connection reset by peer, closing", c)
		} else {
			if read {
				log.Error().Err(err).Msgf("%v: error reading, closing connection", c)
			} else {
				log.Error().Err(err).Msgf("%v: error writing, closing connection", c)
			}
		}
		abort = true
	}
	return abort
}

func (c *CqlClientConnection) processIncomingFrame(incoming *frame.Frame) (abort bool) {
	log.Debug().Msgf("%v: received incoming frame: %v", c, incoming)
	if incoming.Header.OpCode == primitive.OpCodeEvent {
		for _, handler := range c.handlers {
			handler(incoming, c)
		}
		select {
		case c.events <- incoming:
			log.Debug().Msgf("%v: incoming event frame successfully delivered: %v", c, incoming)
		default:
			log.Error().Msgf("%v: events queue is full, discarding event frame: %v", c, incoming)
		}
	} else {
		if err := c.inFlightHandler.onIncomingFrameReceived(incoming); err != nil {
			log.Error().Err(err).Msgf("%v: incoming frame delivery failed: %v", c, incoming)
		} else {
			log.Debug().Msgf("%v: incoming frame successfully delivered: %v", c, incoming)
		}
		if incoming.Header.OpCode == primitive.OpCodeError {
			e := incoming.Body.Message.(message.Error)
			if e.GetErrorCode().IsFatalError() {
				log.Error().Msgf("%v: server replied with fatal error code %v, closing connection", c, e.GetErrorCode())
				abort = true
			}
		}
	}
	return
}

func (c *CqlClientConnection) awaitDone() {
	c.waitGroup.Add(1)
	go func() {
		<-c.ctx.Done()
		log.Debug().Err(c.ctx.Err()).Msgf("%v: context was closed", c)
		c.waitGroup.Done()
		c.abort()
	}()
}

// NewStartupRequest is a convenience method to create a new STARTUP request frame. The compression option will be
// automatically set to the appropriate compression algorithm, depending on whether the connection was configured to
// use a compressor. Use stream id zero to activate automatic stream id management.
func (c *CqlClientConnection) NewStartupRequest(version primitive.ProtocolVersion, streamId int16) (*frame.Frame, error) {
	startup := message.NewStartup()
	if c.compression != primitive.CompressionNone {
		if version.SupportsCompression(c.compression) {
			startup.SetCompression(c.compression)
		} else {
			return nil, fmt.Errorf("%v does not support compression %v", version, c.compression)
		}
	}
	startup.SetDriverName("DataStax Go client")
	return frame.NewFrame(version, streamId, startup), nil
}

// InFlightRequest is an in-flight request sent through CqlClientConnection.Send.
type InFlightRequest interface {

	// StreamId is the in-flight request stream id.
	StreamId() int16

	// Incoming returns a channel to receive incoming frames for this in-flight request. Typically the channel will
	// only ever emit one single frame, except when using continuous paging (DataStax Enterprise only).
	// The returned channel is never nil. It is closed after receiving the last frame, or if an error occurs
	// (typically a timeout), whichever happens first; when the channel is closed, IsDone returns true.
	// If the channel is closed because of an error, Err will return that error, otherwise it will return nil.
	// Successive calls to Incoming return the same channel.
	Incoming() <-chan *frame.Frame

	// IsDone returns true if Incoming is closed, and false otherwise.
	IsDone() bool

	// Err returns nil if Incoming is not yet closed.
	// If Incoming is closed, Err returns either nil if the channel was closed normally, or a non-nil error explaining
	// why the channel was closed abnormally.
	// After Err returns a non-nil error, successive calls to Err return the same error.
	Err() error
}

// Send sends the given request frame and returns a receive channel that can be used to receive response frames and
// errors matching the request's stream id. The channel will be closed after receiving the last frame, or if the
// configured read timeout is triggered, or if the connection itself is closed, whichever happens first.
// Stream id management: if the frame's stream id is ManagedStreamId (0), it is assumed that the frame's stream id is
// to be automatically assigned by the connection upon write. Users are free to choose between managed stream ids or
// manually assigned ones, but it is not recommended mixing managed stream ids with non-managed ones on the same
// connection.
func (c *CqlClientConnection) Send(f *frame.Frame) (InFlightRequest, error) {
	if f == nil {
		return nil, fmt.Errorf("%v: frame cannot be nil", c)
	}
	if c.IsClosed() {
		return nil, fmt.Errorf("%v: connection closed", c)
	}
	log.Debug().Msgf("%v: enqueuing outgoing frame: %v", c, f)
	if inFlight, err := c.inFlightHandler.onOutgoingFrameEnqueued(f); err != nil {
		return nil, fmt.Errorf("%v: failed to register in-flight handler for frame: %v: %w", c, f, err)
	} else {
		select {
		case c.outgoing <- f:
			log.Debug().Msgf("%v: outgoing frame successfully enqueued: %v", c, f)
			return inFlight, nil
		default:
			return nil, fmt.Errorf("%v: failed to enqueue outgoing frame: %v", c, f)
		}
	}
}

// Receive is a convenience method that takes an InFlightRequest obtained through Send and waits until the next response
// frame is received, or an error occurs, whichever happens first.
// If the in-flight request is completed already without returning more frames, this method return a nil frame and a
// nil error.
func (c *CqlClientConnection) Receive(ch InFlightRequest) (*frame.Frame, error) {
	if ch == nil {
		return nil, fmt.Errorf("%v: response channel cannot be nil", c)
	}
	log.Debug().Msgf("%v: waiting for incoming frame", c)
	if incoming, ok := <-ch.Incoming(); !ok {
		if ch.Err() == nil {
			log.Debug().Msgf("%v: in-flight request closed for stream id: %d", c, ch.StreamId())
			return nil, nil
		} else {
			return nil, fmt.Errorf("%v: failed to retrieve incoming frame: %w", c, ch.Err())
		}
	} else {
		log.Debug().Msgf("%v: incoming frame successfully received: %v", c, incoming)
		return incoming, nil
	}
}

// SendAndReceive is a convenience method chaining a call to Send to a call to Receive.
func (c *CqlClientConnection) SendAndReceive(f *frame.Frame) (*frame.Frame, error) {
	if ch, err := c.Send(f); err != nil {
		return nil, err
	} else {
		return c.Receive(ch)
	}
}

// EventChannel is a receive-only channel for incoming events. A receive channel can be obtained through
// CqlClientConnection.EventChannel.
type EventChannel <-chan *frame.Frame

// EventChannel returns a channel for listening to incoming events received on this connection. This channel will be
// closed when the connection is closed. If this connection has already been closed, this method returns nil.
func (c *CqlClientConnection) EventChannel() EventChannel {
	return c.events
}

// ReceiveEvent waits until an event frame is received, or the configured read timeout is triggered, or the connection
// is closed, whichever happens first. Returns the event frame, if any.
func (c *CqlClientConnection) ReceiveEvent() (*frame.Frame, error) {
	if c.IsClosed() {
		return nil, fmt.Errorf("%v: connection closed", c)
	}
	select {
	case incoming, ok := <-c.events:
		if !ok {
			return nil, fmt.Errorf("%v: incoming events channel closed", c)
		}
		return incoming, nil
	case <-time.After(c.readTimeout):
		return nil, fmt.Errorf("%v: timed out waiting for incoming events", c)
	}
}

func (c *CqlClientConnection) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

func (c *CqlClientConnection) setClosed() bool {
	return atomic.CompareAndSwapInt32(&c.closed, 0, 1)
}

func (c *CqlClientConnection) Close() (err error) {
	if c.setClosed() {
		log.Debug().Msgf("%v: closing", c)
		c.cancel()
		err = c.conn.Close()
		outgoing := c.outgoing
		events := c.events
		c.outgoing = nil
		c.events = nil
		close(outgoing)
		close(events)
		c.inFlightHandler.close()
		c.waitGroup.Wait()
		if err != nil {
			err = fmt.Errorf("%v: error closing: %w", c, err)
		} else {
			log.Info().Msgf("%v: successfully closed", c)
		}
	} else {
		log.Debug().Err(err).Msgf("%v: already closed", c)
	}
	return err
}

func (c *CqlClientConnection) abort() {
	log.Debug().Msgf("%v: forcefully closing", c)
	if err := c.Close(); err != nil {
		log.Error().Err(err).Msgf("%v: error closing", c)
	}
}
