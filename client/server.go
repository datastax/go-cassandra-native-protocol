package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
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
	DefaultAcceptTimeout = time.Second * 60
	DefaultIdleTimeout   = time.Hour
)

const DefaultMaxConnections = 128

const (
	ServerStateNotStarted = int32(iota)
	ServerStateRunning    = int32(iota)
	ServerStateClosed     = int32(iota)
)

// The RequestHandler invocation context. Each invocation of a given RequestHandler will be passed
// one instance of a RequestHandlerContext, that remains the same between invocations. This allows
// handlers to become stateful if required.
type RequestHandlerContext interface {
	// Puts the given value in this context under the given key name.
	// Will override any previously-stored value under that key.
	PutAttribute(name string, value interface{})
	// Retrieves the value stored in this context under the given key name.
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

// A request handler is a callback function that gets invoked whenever a CqlServerConnection receives an incoming
// frame. The handler function should inspect the request frame and determine if it can handle the response for it.
// If so, it should return a non-nil response frame. When that happens, no further handlers will be tried for the
// incoming request.
// If a handler returns nil, it is assumed that it was not able to handle the request, in which case another handler,
// if any, may be tried.
type RequestHandler func(request *frame.Frame, conn *CqlServerConnection, ctx RequestHandlerContext) (response *frame.Frame)

// CqlServer is a minimalistic server stub that can be used to mimic CQL-compatible backends. It is preferable to
// create CqlServer instances using the constructor function NewCqlServer. Once the server is properly created and
// configured, use Start to start the server, then call Accept or AcceptAny to accept incoming client connections.
type CqlServer struct {
	// The address to listen to.
	ListenAddress string
	// The AuthCredentials to use. If nil, no authentication will used; otherwise, clients will be required to
	// authenticate with plain-text auth using the same credentials.
	Credentials *AuthCredentials
	// The frame.Codec to use; if none provided, a default codec will be used.
	Codec frame.Codec
	// The maximum number of open client connections to accept. Must be strictly positive.
	MaxConnections int
	// The maximum number of in-flight requests to apply for each connection created with Accept. Must be strictly
	// positive.
	MaxInFlight int
	// The timeout to apply when accepting new connections.
	AcceptTimeout time.Duration
	// The timeout to apply for closing idle connections.
	IdleTimeout time.Duration
	// An optional list of handlers to handle incoming requests.
	RequestHandlers []RequestHandler

	ctx                context.Context
	cancel             context.CancelFunc
	listener           net.Listener
	connectionsHandler *clientConnectionHandler
	waitGroup          *sync.WaitGroup
	state              int32
}

// Creates a new CqlServer with default options. Leave credentials nil to opt out from authentication.
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

// Starts the server and binds to its listen address. This method must be called before calling Accept.
// Set ctx to context.Background if no parent context exists.
func (server *CqlServer) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}
	if server.transitionState(ServerStateNotStarted, ServerStateRunning) {
		log.Debug().Msgf("%v: server is starting", server)
		if server.connectionsHandler, err = newClientConnectionHandler(server.String(), server.MaxConnections); err != nil {
			return fmt.Errorf("%v: start failed: %w", server, err)
		} else if server.listener, err = net.Listen("tcp", server.ListenAddress); err != nil {
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
				if connection, err := newCqlServerConnection(
					conn,
					server.ctx,
					server.Credentials,
					server.Codec,
					server.MaxInFlight,
					server.IdleTimeout,
					server.RequestHandlers,
					server.connectionsHandler.onConnectionClosed,
				); err != nil {
					log.Error().Msgf("%v: failed to create incoming client connection: %v", server, connection)
				} else if err := server.connectionsHandler.onConnectionAccepted(connection); err == nil {
					log.Info().Msgf("%v: accepted new incoming client connection: %v", server, connection)
				} else {
					log.Error().Msgf("%v: failed to accept client connection: %v", server, connection)
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

// Waits until the given client address is accepted, the configured timeout is triggered, or the server is closed,
// whichever happens first.
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

// Waits until any client is accepted, the configured timeout is triggered, or the server is closed,
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

// Returns a list of all the currently active server connections.
func (server *CqlServer) AllAcceptedClients() ([]*CqlServerConnection, error) {
	if server.IsClosed() {
		return nil, fmt.Errorf("%v: server closed", server)
	}
	return server.connectionsHandler.allAcceptedClients(), nil
}

// Convenience method to connect a CqlClient to this CqlServer. The returned connections will be open, but not
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

// Convenience method to connect a CqlClient to this CqlServer. The returned connections will be open and
// initialized (i.e., handshake is already performed). The server must be started prior to calling this method.
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

// CqlServerConnection encapsulates a TCP server connection to a remote CQL client.
// CqlServerConnection instances should be created by calling CqlServer.Accept or CqlServer.Bind.
type CqlServerConnection struct {
	conn        net.Conn
	credentials *AuthCredentials
	codec       frame.Codec
	idleTimeout time.Duration
	handlers    []RequestHandler
	handlerCtx  []RequestHandlerContext
	incoming    chan *frame.Frame
	outgoing    chan *frame.Frame
	waitGroup   *sync.WaitGroup
	closed      int32
	onClose     func(*CqlServerConnection)
	ctx         context.Context
	cancel      context.CancelFunc
}

func newCqlServerConnection(
	conn net.Conn,
	ctx context.Context,
	credentials *AuthCredentials,
	codec frame.Codec,
	maxInFlight int,
	idleTimeout time.Duration,
	handlers []RequestHandler,
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
	if codec == nil {
		codec = frame.NewCodec()
	}
	connection := &CqlServerConnection{
		conn:        conn,
		codec:       codec,
		credentials: credentials,
		idleTimeout: idleTimeout,
		handlers:    handlers,
		handlerCtx:  make([]RequestHandlerContext, len(handlers)),
		incoming:    make(chan *frame.Frame, maxInFlight),
		outgoing:    make(chan *frame.Frame, maxInFlight),
		waitGroup:   &sync.WaitGroup{},
		onClose:     onClose,
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

// Returns the connection's local address (that is, the client address).
func (c *CqlServerConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// Returns the connection's remote address (that is, the server address).
func (c *CqlServerConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Returns a copy of the connection's AuthCredentials, if any, or nil if no authentication was configured.
func (c *CqlServerConnection) Credentials() *AuthCredentials {
	if c.credentials == nil {
		return nil
	}
	return c.credentials.Copy()
}

func (c *CqlServerConnection) incomingLoop() {
	log.Debug().Msgf("%v: listening for incoming frames...", c)
	c.waitGroup.Add(1)
	go func() {
		abort := false
		for !c.IsClosed() {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.idleTimeout)); err != nil {
				if !c.IsClosed() {
					log.Error().Err(err).Msgf("%v: error setting idle timeout, closing connection", c)
					abort = true
				}
				break
			} else if incoming, err := c.codec.DecodeFrame(c.conn); err != nil {
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
		var response *frame.Frame
		var err error
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
		c.waitGroup.Done()
	}()
}

// Sends the given response frame.
func (c *CqlServerConnection) Send(f *frame.Frame) error {
	if c.IsClosed() {
		return fmt.Errorf("%v: connection closed", c)
	}
	log.Debug().Msgf("%v: enqueuing outgoing frame: %v", c, f)
	select {
	case c.outgoing <- f:
		log.Debug().Msgf("%v: outgoing frame successfully enqueued: %v", c, f)
		return nil
	default:
		return fmt.Errorf("%v: failed to enqueue outgoing frame: %v", c, f)
	}
}

// Waits  until the next request frame is received, or the configured idle timeout is triggered, or the connection
// itself is closed, whichever happens first.
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
