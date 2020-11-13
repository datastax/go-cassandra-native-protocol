package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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

// CqlClient is a client for Cassandra-compatible backends. It is preferable to create CqlClient instances using the
// constructor function NewCqlClient. Once the client is created and properly configured, use Connect or ConnectAndInit
// to establish new connections to the server.
type CqlClient struct {
	// The remote contact point address to connect to.
	RemoteAddress string
	// The AuthCredentials for authenticated servers. If nil, no authentication will be used.
	Credentials *AuthCredentials
	// The frame.Codec to use; if none provided, a default codec will be used.
	Codec frame.Codec
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
}

// Creates a new CqlClient with default options. Leave credentials nil to opt out from authentication.
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
	connectCtx, _ := context.WithTimeout(ctx, client.ConnectTimeout)
	if conn, err := dialer.DialContext(connectCtx, "tcp", client.RemoteAddress); err != nil {
		return nil, fmt.Errorf("%v: cannot establish TCP connection: %w", client, err)
	} else {
		connection, err := newCqlClientConnection(
			conn,
			ctx,
			client.Credentials,
			client.Codec,
			client.MaxInFlight,
			client.MaxPending,
			client.ReadTimeout,
		)
		log.Info().Msgf("%v: new TCP connection established: %v", client, connection)
		return connection, err
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
	conn            net.Conn
	codec           frame.Codec
	readTimeout     time.Duration
	credentials     *AuthCredentials
	inFlightHandler *inFlightRequestsHandler
	outgoing        chan *frame.Frame
	events          chan *frame.Frame
	waitGroup       *sync.WaitGroup
	closed          int32
	ctx             context.Context
	cancel          context.CancelFunc
}

func newCqlClientConnection(
	conn net.Conn,
	ctx context.Context,
	credentials *AuthCredentials,
	codec frame.Codec,
	maxInFlight int,
	maxPending int,
	readTimeout time.Duration,
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
	if codec == nil {
		codec = frame.NewCodec()
	}
	connection := &CqlClientConnection{
		conn:        conn,
		codec:       codec,
		readTimeout: readTimeout,
		credentials: credentials,
		outgoing:    make(chan *frame.Frame, maxInFlight),
		events:      make(chan *frame.Frame, maxInFlight),
		waitGroup:   &sync.WaitGroup{},
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

// Returns the connection's local address (that is, the client address).
func (c *CqlClientConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// Returns the connection's remote address (that is, the server address).
func (c *CqlClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Returns a copy of the connection's AuthCredentials, if any, or nil if no authentication was configured.
func (c *CqlClientConnection) Credentials() *AuthCredentials {
	if c.credentials == nil {
		return nil
	}
	return c.credentials.Copy()
}

func (c *CqlClientConnection) incomingLoop() {
	log.Debug().Msgf("%v: listening for incoming frames...", c)
	c.waitGroup.Add(1)
	go func() {
		abort := false
		for !c.IsClosed() {
			if incoming, err := c.codec.DecodeFrame(c.conn); err != nil {
				if !c.IsClosed() {
					if errors.Is(err, io.EOF) {
						log.Info().Msgf("%v: connection reset by peer, closing", c)
					} else {
						log.Error().Err(err).Msgf("%v: error reading, closing connection", c)
					}
					abort = true
				}
				break
			} else {
				log.Debug().Msgf("%v: received incoming frame: %v", c, incoming)
				if incoming.Header.OpCode == primitive.OpCodeEvent {
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
					if fatalError, errorCode := c.isFatalError(incoming); fatalError {
						log.Error().Msgf("%v: server replied with fatal error code %v, closing connection", c, errorCode)
						abort = true
						break
					}
				}
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
		for !c.IsClosed() {
			if outgoing, ok := <-c.outgoing; !ok {
				if !c.IsClosed() {
					log.Error().Msgf("%v: outgoing frame channel was closed unexpectedly, closing connection", c)
					abort = true
				}
				break
			} else {
				log.Debug().Msgf("%v: sending outgoing frame: %v", c, outgoing)
				if err := c.codec.EncodeFrame(outgoing, c.conn); err != nil {
					if !c.IsClosed() {
						if errors.Is(err, io.EOF) {
							log.Info().Msgf("%v: connection reset by peer, closing", c)
						} else {
							log.Error().Err(err).Msgf("%v: error writing, closing connection", c)
						}
						abort = true
					}
					break
				} else {
					log.Debug().Msgf("%v: outgoing frame successfully written: %v", c, outgoing)
				}
			}
		}
		c.waitGroup.Done()
		if abort {
			c.abort()
		}
	}()
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

// Convenience method to create a new STARTUP request frame. The compression option will be automatically set to the
// appropriate compression algorithm, depending on whether the frame codec has a body compressor or not. Use stream id
// zero to activate automatic stream id management.
func (c *CqlClientConnection) NewStartupRequest(version primitive.ProtocolVersion, streamId int16) *frame.Frame {
	startup := message.NewStartup()
	if c.codec.GetBodyCompressor() != nil {
		startup.SetCompression(c.codec.GetBodyCompressor().Algorithm())
	}
	startup.SetDriverName("DataStax Go client")
	request, _ := frame.NewRequestFrame(version, streamId, false, nil, startup, false)
	return request
}

// A in-flight request sent through CqlClientConnection.Send.
type InFlightRequest interface {

	// The in-flight request stream id.
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

	// If Incoming is not yet closed, Err returns nil.
	// If Incoming is closed, Err returns either nil if the channel was closed normally, or a non-nil error explaining
	// why the channel was closed abnormally.
	// After Err returns a non-nil error, successive calls to Err return the same error.
	Err() error
}

// Sends the given request frame and returns a receive channel that can be used to receive response frames and errors
// matching the request's stream id. The channel will be closed after receiving the last frame, or if the configured
// read timeout is triggered, or if the connection itself is closed, whichever happens first.
// Stream id management: if the frame's stream id is ManagedStreamId (0), it is assumed that the frame's stream id is
// to be automatically assigned by the connection upon write. Users are free to choose between managed stream ids or
// manually assigned ones, but it is not recommended to mix managed stream ids with non-managed ones on the same
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

// Convenience method that takes an InFlightRequest obtained through Send and waits until the next response frame
// is received, or an error occurs, whichever happens first.
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

// Convenience method chaining a call to Send to a call to Receive.
func (c *CqlClientConnection) SendAndReceive(f *frame.Frame) (*frame.Frame, error) {
	if ch, err := c.Send(f); err != nil {
		return nil, err
	} else {
		return c.Receive(ch)
	}
}

// A receive-only channel for incoming events. A receive channel can be obtained through
// CqlClientConnection.EventChannel.
type EventChannel <-chan *frame.Frame

// Returns a channel for listening to incoming events received on this connection. This channel will be closed when the
// connection is closed. If this connection has already been closed, this method returns nil.
func (c *CqlClientConnection) EventChannel() EventChannel {
	return c.events
}

// Waits until an event frame is received, or the configured read timeout is triggered, or the connection is closed,
// whichever happens first. Returns the event frame, if any.
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

func (c *CqlClientConnection) isFatalError(incoming *frame.Frame) (bool, primitive.ErrorCode) {
	if incoming.Header.OpCode == primitive.OpCodeError {
		e := incoming.Body.Message.(message.Error)
		switch e.GetErrorCode() {
		case primitive.ErrorCodeServerError:
			fallthrough
		case primitive.ErrorCodeProtocolError:
			fallthrough
		case primitive.ErrorCodeAuthenticationError:
			fallthrough
		case primitive.ErrorCodeOverloaded:
			fallthrough
		case primitive.ErrorCodeIsBootstrapping:
			return true, e.GetErrorCode()
		}
	}
	return false, -1
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
