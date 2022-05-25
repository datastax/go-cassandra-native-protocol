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
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/segment"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/rs/zerolog/log"
)

const (
	DefaultAcceptTimeout = time.Second * 60
	DefaultIdleTimeout   = time.Hour
)

const DefaultMaxConnections = 128

const (
	ServerStateNotStarted = int32(iota)
	ServerStateRunning    = int32(iota)
	ServerStateClosed     = int32(iota)
)

// RequestHandlerContext is the RequestHandler invocation context. Each invocation of a given RequestHandler will be
// passed one instance of a RequestHandlerContext, that remains the same between invocations. This allows
// handlers to become stateful if required.
type RequestHandlerContext interface {
	// PutAttribute puts the given value in this context under the given key name.
	// Will override any previously-stored value under that key.
	PutAttribute(name string, value interface{})
	// GetAttribute retrieves the value stored in this context under the given key name.
	// Returns nil if nil is stored, or if the key does not exist.
	GetAttribute(name string) interface{}
}

type requestHandlerContext map[string]interface{}

func (ctx requestHandlerContext) PutAttribute(name string, value interface{}) {
	ctx[name] = value
}

func (ctx requestHandlerContext) GetAttribute(name string) interface{} {
	return ctx[name]
}

// RequestHandler is a callback function that gets invoked whenever a CqlServerConnection receives an incoming
// frame. The handler function should inspect the request frame and determine if it can handle the response for it.
// If so, it should return a non-nil response frame. When that happens, no further handlers will be tried for the
// incoming request.
// If a handler returns nil, it is assumed that it was not able to handle the request, in which case another handler,
// if any, may be tried.
type RequestHandler func(request *frame.Frame, conn *CqlServerConnection, ctx RequestHandlerContext) (response *frame.Frame)

// RawRequestHandler is similar to RequestHandler but returns an already encoded response in byte slice format, this can be used to return responses that the
// embedded codecs can't encode
type RawRequestHandler func(request *frame.Frame, conn *CqlServerConnection, ctx RequestHandlerContext) (encodedResponse []byte)

// CqlServer is a minimalistic server stub that can be used to mimic CQL-compatible backends. It is preferable to
// create CqlServer instances using the constructor function NewCqlServer. Once the server is properly created and
// configured, use Start to start the server, then call Accept or AcceptAny to accept incoming client connections.
type CqlServer struct {
	// ListenAddress is the address to listen to.
	ListenAddress string
	// Credentials is the AuthCredentials to use. If nil, no authentication will be used; otherwise, clients will be
	// required to authenticate with plain-text auth using the same credentials.
	Credentials *AuthCredentials
	// MaxConnections is the maximum number of open client connections to accept. Must be strictly positive.
	MaxConnections int
	// MaxInFlight is the maximum number of in-flight requests to apply for each connection created with Accept. Must
	// be strictly positive.
	MaxInFlight int
	// AcceptTimeout is the timeout to apply when accepting new connections.
	AcceptTimeout time.Duration
	// IdleTimeout is the timeout to apply for closing idle connections.
	IdleTimeout time.Duration
	// RequestHandlers is an optional list of handlers to handle incoming requests.
	RequestHandlers []RequestHandler
	// RequestRawHandlers is an optional list of handlers to handle incoming requests and return a response in a byte slice format.
	RequestRawHandlers []RawRequestHandler
	// TLSConfig is the TLS configuration to use.
	TLSConfig *tls.Config

	ctx                context.Context
	cancel             context.CancelFunc
	listener           net.Listener
	connectionsHandler *clientConnectionHandler
	waitGroup          *sync.WaitGroup
	state              int32
}

// NewCqlServer creates a new CqlServer with default options. Leave credentials nil to opt out from authentication.
func NewCqlServer(listenAddress string, credentials *AuthCredentials) *CqlServer {
	return &CqlServer{
		ListenAddress:  listenAddress,
		Credentials:    credentials,
		MaxConnections: DefaultMaxConnections,
		MaxInFlight:    DefaultMaxInFlight,
		AcceptTimeout:  DefaultAcceptTimeout,
		IdleTimeout:    DefaultIdleTimeout,
	}
}

func (server *CqlServer) String() string {
	return fmt.Sprintf("CQL server [%v]", server.ListenAddress)
}

func (server *CqlServer) getState() int32 {
	return atomic.LoadInt32(&server.state)
}

func (server *CqlServer) IsNotStarted() bool {
	return server.getState() == ServerStateNotStarted
}

func (server *CqlServer) IsRunning() bool {
	return server.getState() == ServerStateRunning
}

func (server *CqlServer) IsClosed() bool {
	return server.getState() == ServerStateClosed
}

func (server *CqlServer) transitionState(old int32, new int32) bool {
	return atomic.CompareAndSwapInt32(&server.state, old, new)
}

// Start starts the server and binds to its listen address. This method must be called before calling Accept.
// Set ctx to context.Background if no parent context exists.
func (server *CqlServer) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}
	if server.transitionState(ServerStateNotStarted, ServerStateRunning) {
		log.Debug().Msgf("%v: server is starting", server)
		server.connectionsHandler, err = newClientConnectionHandler(server.String(), server.MaxConnections)
		if err != nil {
			return fmt.Errorf("%v: start failed: %w", server, err)
		}
		if server.TLSConfig != nil {
			server.listener, err = tls.Listen("tcp", server.ListenAddress, server.TLSConfig)
		} else {
			server.listener, err = net.Listen("tcp", server.ListenAddress)
		}
		if err != nil {
			return fmt.Errorf("%v: start failed: %w", server, err)
		}
		server.ctx, server.cancel = context.WithCancel(ctx)
		server.waitGroup = &sync.WaitGroup{}
		server.acceptLoop()
		server.awaitDone()
		log.Info().Msgf("%v: successfully started", server)
	} else {
		log.Debug().Msgf("%v: already started or closed", server)
	}
	return err
}

func (server *CqlServer) Close() (err error) {
	if server.transitionState(ServerStateRunning, ServerStateClosed) {
		log.Debug().Msgf("%v: closing", server)
		err = server.listener.Close()
		server.connectionsHandler.close()
		server.cancel()
		server.waitGroup.Wait()
		if err != nil {
			log.Debug().Err(err).Msgf("%v: could not close server", server)
			err = fmt.Errorf("%v: could not close server: %w", server, err)
		} else {
			log.Info().Msgf("%v: successfully closed", server)
		}
	} else {
		log.Debug().Msgf("%v: not started or already closed", server)
	}
	return err
}

func (server *CqlServer) abort() {
	log.Debug().Msgf("%v: forcefully closing", server)
	if err := server.Close(); err != nil {
		log.Error().Err(err).Msgf("%v: error closing", server)
	}
}

func (server *CqlServer) acceptLoop() {
	server.waitGroup.Add(1)
	go func() {
		abort := false
		for server.IsRunning() {
			if conn, err := server.listener.Accept(); err != nil {
				if !server.IsClosed() {
					log.Error().Err(err).Msgf("%v: error accepting client connections, closing server", server)
					abort = true
				}
				break
			} else {
				log.Debug().Msgf("%v: new TCP connection accepted", server)
				if connection, err := newCqlServerConnection(
					conn,
					server.ctx,
					server.Credentials,
					server.MaxInFlight,
					server.IdleTimeout,
					server.RequestHandlers,
					server.RequestRawHandlers,
					server.connectionsHandler.onConnectionClosed,
				); err != nil {
					log.Error().Msgf("%v: failed to accept incoming CQL client connection: %v", server, connection)
					_ = conn.Close()
				} else if err := server.connectionsHandler.onConnectionAccepted(connection); err != nil {
					log.Error().Msgf("%v: handler rejected incoming CQL client connection: %v", server, connection)
					_ = conn.Close()
				} else {
					log.Info().Msgf("%v: accepted new incoming CQL client connection: %v", server, connection)
				}
			}
		}
		server.waitGroup.Done()
		if abort {
			server.abort()
		}
	}()
}

func (server *CqlServer) awaitDone() {
	server.waitGroup.Add(1)
	go func() {
		<-server.ctx.Done()
		log.Debug().Err(server.ctx.Err()).Msgf("%v: context was closed", server)
		server.waitGroup.Done()
		server.abort()
	}()
}

// Accept waits until the given client address is accepted, the configured timeout is triggered, or the server is
// closed, whichever happens first.
func (server *CqlServer) Accept(client *CqlClientConnection) (*CqlServerConnection, error) {
	if server.IsClosed() {
		return nil, fmt.Errorf("%v: server closed", server)
	}
	log.Debug().Msgf("%v: waiting for incoming client connection to be accepted: %v", server, client)
	if serverConnectionChannel, err := server.connectionsHandler.onConnectionAcceptRequested(client); err != nil {
		return nil, err
	} else {
		select {
		case serverConnection, ok := <-serverConnectionChannel:
			if !ok {
				return nil, fmt.Errorf("%v: incoming client connection channel closed unexpectedly", server)
			}
			log.Debug().Msgf("%v: returning accepted client connection: %v", server, serverConnection)
			return serverConnection, nil
		case <-time.After(server.AcceptTimeout):
			return nil, fmt.Errorf("%v: timed out waiting for incoming client connection", server)
		}
	}
}

// AcceptAny waits until any client is accepted, the configured timeout is triggered, or the server is closed,
// whichever happens first. This method is useful when the client is not known in advance.
func (server *CqlServer) AcceptAny() (*CqlServerConnection, error) {
	if server.IsClosed() {
		return nil, fmt.Errorf("%v: server closed", server)
	}
	log.Debug().Msgf("%v: waiting for any incoming client connection to be accepted", server)
	anyConn := server.connectionsHandler.anyConnectionChannel()
	select {
	case serverConnection, ok := <-anyConn:
		if !ok {
			return nil, fmt.Errorf("%v: incoming client connection channel closed unexpectedly", server)
		}
		log.Debug().Msgf("%v: returning accepted client connection: %v", server, serverConnection)
		return serverConnection, nil
	case <-time.After(server.AcceptTimeout):
		return nil, fmt.Errorf("%v: timed out waiting for incoming client connection", server)
	}
}

// AllAcceptedClients returns a list of all the currently active server connections.
func (server *CqlServer) AllAcceptedClients() ([]*CqlServerConnection, error) {
	if server.IsClosed() {
		return nil, fmt.Errorf("%v: server closed", server)
	}
	return server.connectionsHandler.allAcceptedClients(), nil
}

// Bind is a convenience method to connect a CqlClient to this CqlServer. The returned connections will be open, but not
// initialized (i.e., no handshake performed). The server must be started prior to calling this method.
func (server *CqlServer) Bind(client *CqlClient, ctx context.Context) (*CqlClientConnection, *CqlServerConnection, error) {
	if server.IsNotStarted() {
		return nil, nil, fmt.Errorf("%v: server not started", server)
	} else if server.IsClosed() {
		return nil, nil, fmt.Errorf("%v: server closed", server)
	} else if clientConn, err := client.Connect(ctx); err != nil {
		return nil, nil, fmt.Errorf("%v: bind failed, client %v could not connect: %w", server, client, err)
	} else if serverConn, err := server.Accept(clientConn); err != nil {
		return nil, nil, fmt.Errorf("%v: bind failed, client %v wasn't accepted: %w", server, client, err)
	} else {
		log.Debug().Msgf("%v: bind successful: %v", server, serverConn)
		return clientConn, serverConn, nil
	}
}

// BindAndInit is a convenience method to connect a CqlClient to this CqlServer. The returned connections will be open
// and initialized (i.e., handshake is already performed). The server must be started prior to calling this method.
// Use stream id zero to activate automatic stream id management.
func (server *CqlServer) BindAndInit(
	client *CqlClient,
	ctx context.Context,
	version primitive.ProtocolVersion,
	streamId int16,
) (*CqlClientConnection, *CqlServerConnection, error) {
	if clientConn, serverConn, err := server.Bind(client, ctx); err != nil {
		return nil, nil, err
	} else {
		return clientConn, serverConn, PerformHandshake(clientConn, serverConn, version, streamId)
	}
}

type response struct {
	responseFrame *frame.Frame
	rawResponse   []byte
}

func newFrameResponse(frameResponse *frame.Frame) *response {
	return &response{
		responseFrame: frameResponse,
	}
}

func NewRawResponse(rawResponse []byte) *response {
	return &response{
		rawResponse: rawResponse,
	}
}

// CqlServerConnection encapsulates a TCP server connection to a remote CQL client.
// CqlServerConnection instances should be created by calling CqlServer.Accept or CqlServer.Bind.
type CqlServerConnection struct {
	conn               net.Conn
	credentials        *AuthCredentials
	frameCodec         frame.Codec
	segmentCodec       segment.Codec
	compression        primitive.Compression
	modernLayout       bool
	idleTimeout        time.Duration
	handlers           []RequestHandler
	rawHandlers        []RawRequestHandler
	handlerCtx         []RequestHandlerContext
	incoming           chan *frame.Frame
	outgoing           chan *response
	waitGroup          *sync.WaitGroup
	closed             int32
	onClose            func(*CqlServerConnection)
	ctx                context.Context
	cancel             context.CancelFunc
	payloadAccumulator *payloadAccumulator
}

func newCqlServerConnection(
	conn net.Conn,
	ctx context.Context,
	credentials *AuthCredentials,
	maxInFlight int,
	idleTimeout time.Duration,
	handlers []RequestHandler,
	rawHandlers []RawRequestHandler,
	onClose func(*CqlServerConnection),
) (*CqlServerConnection, error) {
	if conn == nil {
		return nil, fmt.Errorf("TCP connection cannot be nil")
	}
	if maxInFlight < 1 {
		return nil, fmt.Errorf("max in-flight: expecting positive, got: %v", maxInFlight)
	} else if maxInFlight > math.MaxInt16 {
		return nil, fmt.Errorf("max in-flight: expecting <= %v, got: %v", math.MaxInt16, maxInFlight)
	}
	frameCodec := frame.NewCodec()
	segmentCodec := segment.NewCodec()
	connection := &CqlServerConnection{
		conn:         conn,
		frameCodec:   frameCodec,
		segmentCodec: segmentCodec,
		compression:  primitive.CompressionNone,
		credentials:  credentials,
		idleTimeout:  idleTimeout,
		handlers:     handlers,
		rawHandlers:  rawHandlers,
		handlerCtx:   make([]RequestHandlerContext, len(handlers)),
		incoming:     make(chan *frame.Frame, maxInFlight),
		outgoing:     make(chan *response, maxInFlight),
		waitGroup:    &sync.WaitGroup{},
		onClose:      onClose,
	}
	for i := range handlers {
		connection.handlerCtx[i] = requestHandlerContext{}
	}
	connection.ctx, connection.cancel = context.WithCancel(ctx)
	connection.incomingLoop()
	connection.outgoingLoop()
	connection.awaitDone()
	return connection, nil
}

func (c *CqlServerConnection) String() string {
	return fmt.Sprintf("CQL server conn [L:%v <-> R:%v]", c.conn.LocalAddr(), c.conn.RemoteAddr())
}

// LocalAddr Returns the connection's local address (that is, the client address).
func (c *CqlServerConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr Returns the connection's remote address (that is, the server address).
func (c *CqlServerConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Credentials Returns a copy of the connection's AuthCredentials, if any, or nil if no authentication was configured.
func (c *CqlServerConnection) Credentials() *AuthCredentials {
	if c.credentials == nil {
		return nil
	}
	return c.credentials.Copy()
}

func (c *CqlServerConnection) GetConn() net.Conn {
	return c.conn
}

func (c *CqlServerConnection) incomingLoop() {
	log.Debug().Msgf("%v: listening for incoming frames...", c)
	c.waitGroup.Add(1)
	go func() {
		abort := false
		for !abort && !c.IsClosed() {
			if abort = c.setIdleTimeout(); !abort {
				if source, err := c.waitForIncomingData(); err != nil {
					abort = c.reportConnectionFailure(err, true)
				} else if c.modernLayout {
					abort = c.readSegment(source)
				} else {
					abort = c.readFrame(source)
				}
			}
		}
		c.waitGroup.Done()
		if abort {
			c.abort()
		}
	}()
}

func (c *CqlServerConnection) outgoingLoop() {
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
				if outgoing.rawResponse != nil {
					abort = c.writeRawResponse(outgoing.rawResponse, c.conn)
					log.Debug().Msgf("%v: sending outgoing raw response: %v", c, outgoing.rawResponse)
				} else {
					if c.compression != primitive.CompressionNone {
						outgoing.responseFrame.Header.Flags = outgoing.responseFrame.Header.Flags.Add(primitive.HeaderFlagCompressed)
					}
					log.Debug().Msgf("%v: sending outgoing frame: %v", c, outgoing.responseFrame)
					if c.modernLayout {
						// TODO write coalescer
						abort = c.writeSegment(outgoing.responseFrame, c.conn)
					} else {
						abort = c.writeFrame(outgoing.responseFrame, c.conn)
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

func (c *CqlServerConnection) waitForIncomingData() (io.Reader, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	} else {
		return io.MultiReader(bytes.NewReader(buf), c.conn), nil
	}
}

func (c *CqlServerConnection) setIdleTimeout() (abort bool) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		if !c.IsClosed() {
			log.Error().Err(err).Msgf("%v: error setting idle timeout, closing connection", c)
			abort = true
		}
	}
	return abort
}

func (c *CqlServerConnection) readSegment(source io.Reader) (abort bool) {
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

func (c *CqlServerConnection) readSelfContainedSegment(incoming *segment.Segment, abort bool) bool {
	payloadReader := bytes.NewReader(incoming.Payload.UncompressedData)
	for payloadReader.Len() > 0 {
		if abort = c.readFrame(payloadReader); abort {
			break
		}
	}
	return abort
}

func (c *CqlServerConnection) addMultiSegmentPayload(payload *segment.Payload) (abort bool) {
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

func (c *CqlServerConnection) writeSegment(outgoing *frame.Frame, dest io.Writer) (abort bool) {
	// never compress frames individually when included in a segment
	outgoing.Header.Flags.Remove(primitive.HeaderFlagCompressed)
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

func (c *CqlServerConnection) readFrame(source io.Reader) (abort bool) {
	if incoming, err := c.frameCodec.DecodeFrame(source); err != nil {
		abort = c.reportConnectionFailure(err, true)
	} else {
		if startup, ok := incoming.Body.Message.(*message.Startup); ok {
			c.compression = startup.GetCompression()
			c.frameCodec = frame.NewCodecWithCompression(NewBodyCompressor(c.compression))
			c.segmentCodec = segment.NewCodecWithCompression(NewPayloadCompressor(c.compression))
		}
		c.processIncomingFrame(incoming)
	}
	return abort
}

func (c *CqlServerConnection) writeFrame(outgoing *frame.Frame, dest io.Writer) (abort bool) {
	c.maybeSwitchToModernLayout(outgoing)
	if err := c.frameCodec.EncodeFrame(outgoing, dest); err != nil {
		abort = c.reportConnectionFailure(err, false)
	} else {
		log.Debug().Msgf("%v: outgoing frame successfully written: %v", c, outgoing)
	}
	return abort
}

func (c *CqlServerConnection) writeRawResponse(outgoing []byte, dest io.Writer) (abort bool) {
	if _, err := dest.Write(outgoing); err != nil {
		abort = c.reportConnectionFailure(err, false)
	} else {
		log.Debug().Msgf("%v: outgoing raw response successfully written: %v", c, outgoing)
	}
	return abort
}

func (c *CqlServerConnection) maybeSwitchToModernLayout(outgoing *frame.Frame) {
	if !c.modernLayout &&
		outgoing.Header.Version.SupportsModernFramingLayout() &&
		(isReady(outgoing) || isAuthenticate(outgoing)) {
		// Changing this value could be racy if some incoming frame is being processed;
		// but in theory, this should never happen during handshake.
		log.Debug().Msgf("%v: switching to modern framing layout", c)
		c.modernLayout = true
	}
}

func (c *CqlServerConnection) reportConnectionFailure(err error, read bool) (abort bool) {
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

func (c *CqlServerConnection) processIncomingFrame(incoming *frame.Frame) {
	log.Debug().Msgf("%v: received incoming frame: %v", c, incoming)
	select {
	case c.incoming <- incoming:
		log.Debug().Msgf("%v: incoming frame successfully delivered: %v", c, incoming)
	default:
		log.Error().Msgf("%v: incoming frames queue is full, discarding frame: %v", c, incoming)
	}
	if len(c.handlers) > 0 {
		c.invokeRequestHandlers(incoming)
	}
}

func (c *CqlServerConnection) awaitDone() {
	c.waitGroup.Add(1)
	go func() {
		<-c.ctx.Done()
		log.Debug().Err(c.ctx.Err()).Msgf("%v: context was closed", c)
		c.waitGroup.Done()
		c.abort()
	}()
}

func (c *CqlServerConnection) invokeRequestHandlers(request *frame.Frame) {
	c.waitGroup.Add(1)
	go func() {
		log.Debug().Msgf("%v: invoking request handlers for incoming request: %v", c, request)
		var err error
		var rawResponse []byte
		for i, rawHandler := range c.rawHandlers {
			if rawResponse = rawHandler(request, c, c.handlerCtx[i]); rawResponse != nil {
				log.Debug().Msgf("%v: raw request handler %v produced response: %v", c, i, rawResponse)
				if err = c.SendRaw(rawResponse); err != nil {
					log.Error().Err(err).Msgf("%v: send failed for frame: %v", c, rawResponse)
				}
				break
			}
		}
		if rawResponse == nil {
			var response *frame.Frame
			for i, handler := range c.handlers {
				if response = handler(request, c, c.handlerCtx[i]); response != nil {
					log.Debug().Msgf("%v: request handler %v produced response: %v", c, i, response)
					if err = c.Send(response); err != nil {
						log.Error().Err(err).Msgf("%v: send failed for frame: %v", c, response)
					}
					break
				}
			}
			if response == nil {
				log.Debug().Msgf("%v: no request handler could handle the request: %v", c, request)
			}
		}
		c.waitGroup.Done()
	}()
}

// Send sends the given response frame.
func (c *CqlServerConnection) Send(f *frame.Frame) error {
	if c.IsClosed() {
		return fmt.Errorf("%v: connection closed", c)
	}
	log.Debug().Msgf("%v: enqueuing outgoing frame: %v", c, f)
	select {
	case c.outgoing <- newFrameResponse(f):
		log.Debug().Msgf("%v: outgoing frame successfully enqueued: %v", c, f)
		return nil
	default:
		return fmt.Errorf("%v: failed to enqueue outgoing frame: %v", c, f)
	}
}

// SendRaw sends the given response frame (already encoded).
func (c *CqlServerConnection) SendRaw(rawResponse []byte) error {
	if c.IsClosed() {
		return fmt.Errorf("%v: connection closed", c)
	}
	log.Debug().Msgf("%v: enqueuing outgoing raw response: %v", c, rawResponse)
	select {
	case c.outgoing <- NewRawResponse(rawResponse):
		log.Debug().Msgf("%v: outgoing frame successfully enqueued: %v", c, rawResponse)
		return nil
	default:
		return fmt.Errorf("%v: failed to send outgoing raw response: %v", c, rawResponse)
	}
}

// Receive waits until the next request frame is received, or the configured idle timeout is triggered, or the
// connection itself is closed, whichever happens first.
func (c *CqlServerConnection) Receive() (*frame.Frame, error) {
	if c.IsClosed() {
		return nil, fmt.Errorf("%v: connection closed", c)
	}
	log.Debug().Msgf("%v: waiting for incoming frame", c)
	if incoming, ok := <-c.incoming; !ok {
		if c.IsClosed() {
			return nil, fmt.Errorf("%v: connection closed", c)
		} else {
			return nil, fmt.Errorf("%v: incoming frame channel closed unexpectedly", c)
		}
	} else {
		log.Debug().Msgf("%v: incoming frame successfully received: %v", c, incoming)
		return incoming, nil
	}
}

func (c *CqlServerConnection) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

func (c *CqlServerConnection) setClosed() bool {
	return atomic.CompareAndSwapInt32(&c.closed, 0, 1)
}

func (c *CqlServerConnection) Close() (err error) {
	if c.setClosed() {
		log.Debug().Msgf("%v: closing", c)
		c.cancel()
		err = c.conn.Close()
		incoming := c.incoming
		outgoing := c.outgoing
		c.incoming = nil
		c.outgoing = nil
		close(incoming)
		close(outgoing)
		c.waitGroup.Wait()
		c.onClose(c)
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

func (c *CqlServerConnection) abort() {
	log.Debug().Msgf("%v: forcefully closing", c)
	if err := c.Close(); err != nil {
		log.Error().Err(err).Msgf("%v: error closing", c)
	}
}
