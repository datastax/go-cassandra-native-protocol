package client

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"net"
	"sync"
	"sync/atomic"
)

type connectionHolder struct {
	ch   chan *CqlServerConnection
	conn *CqlServerConnection
}

type clientConnectionHandler struct {
	serverId        string
	maxConnections  int
	connections     map[string]*connectionHolder
	anyConnChan     chan *CqlServerConnection
	connectionsLock *sync.Mutex
	closed          int32
}

func (h *clientConnectionHandler) String() string {
	return fmt.Sprintf("%v: [conn. handler]", h.serverId)
}

func newClientConnectionHandler(serverId string, maxClientConnections int) (*clientConnectionHandler, error) {
	if maxClientConnections < 1 {
		return nil, fmt.Errorf("max connections: expecting positive, got: %v", maxClientConnections)
	}
	return &clientConnectionHandler{
		serverId:        serverId,
		maxConnections:  maxClientConnections,
		connections:     make(map[string]*connectionHolder, maxClientConnections),
		anyConnChan:     make(chan *CqlServerConnection, maxClientConnections),
		connectionsLock: &sync.Mutex{},
	}, nil
}

func (h *clientConnectionHandler) anyConnectionChannel() <-chan *CqlServerConnection {
	return h.anyConnChan
}

func (h *clientConnectionHandler) onConnectionAcceptRequested(clientAddr net.Addr) (<-chan *CqlServerConnection, error) {
	if h.isClosed() {
		return nil, fmt.Errorf("%v: handler closed", h)
	}
	if clientAddr, err := h.asMapKey(clientAddr); err != nil {
		return nil, err
	} else {
		log.Trace().Msgf("%v: client accept requested: %v", h, clientAddr)
		h.connectionsLock.Lock()
		defer h.connectionsLock.Unlock()
		holder, found := h.connections[clientAddr]
		if !found {
			log.Trace().Msgf("%v: client address unknown, registering new channel: %v", h, clientAddr)
			if len(h.connections) == h.maxConnections {
				return nil, fmt.Errorf("%v: too many connections: %v", h, h.maxConnections)
			}
			holder = &connectionHolder{
				ch: make(chan *CqlServerConnection, 1),
			}
			h.connections[clientAddr] = holder
		}
		return holder.ch, nil
	}
}

func (h *clientConnectionHandler) onConnectionAccepted(connection *CqlServerConnection) error {
	if h.isClosed() {
		return fmt.Errorf("%v: handler closed", h)
	}
	if clientAddr, err := h.asMapKey(connection.conn.RemoteAddr()); err != nil {
		return err
	} else {
		log.Trace().Msgf("%v: client accepted: %v", h, connection.conn.RemoteAddr())
		h.connectionsLock.Lock()
		defer h.connectionsLock.Unlock()
		holder, found := h.connections[clientAddr]
		if found {
			holder.conn = connection
		} else {
			log.Trace().Msgf("%v: client address unknown, registering new channel: %v", h, connection.conn.RemoteAddr())
			if len(h.connections) == h.maxConnections {
				return fmt.Errorf("%v: too many connections: %v", h, h.maxConnections)
			}
			holder = &connectionHolder{
				ch:   make(chan *CqlServerConnection, 1),
				conn: connection,
			}
			h.connections[clientAddr] = holder
		}
		holder.ch <- connection
		h.anyConnChan <- connection
		return nil
	}
}

func (h *clientConnectionHandler) onConnectionClosed(connection *CqlServerConnection) {
	if !h.isClosed() {
		if clientAddr, err := h.asMapKey(connection.conn.RemoteAddr()); err == nil {
			log.Trace().Msgf("%v: client address closed, removing: %v", h, connection.conn.RemoteAddr())
			h.connectionsLock.Lock()
			defer h.connectionsLock.Unlock()
			if holder, found := h.connections[clientAddr]; found {
				log.Trace().Msgf("%v: client address removed: %v", h, connection.conn.RemoteAddr())
				delete(h.connections, clientAddr)
				close(holder.ch)
			} else {
				log.Trace().Msgf("%v: client address not found, ignoring: %v", h, connection.conn.RemoteAddr())
			}
		}
	}
}

func (h *clientConnectionHandler) isClosed() bool {
	return atomic.LoadInt32(&h.closed) == 1
}

func (h *clientConnectionHandler) setClosed() bool {
	return atomic.CompareAndSwapInt32(&h.closed, 0, 1)
}

func (h *clientConnectionHandler) close() {
	if h.setClosed() {
		log.Trace().Msgf("%v: closing", h)
		h.connectionsLock.Lock()
		for clientAddr, holder := range h.connections {
			delete(h.connections, clientAddr)
			if err := holder.conn.Close(); err != nil {
				log.Error().Err(err).Msg(err.Error())
			}
			close(holder.ch)
		}
		anyConnChan := h.anyConnChan
		h.anyConnChan = nil
		close(anyConnChan)
		h.connectionsLock.Unlock()
		log.Trace().Msgf("%v: successfully closed", h)
	}
}

func (h *clientConnectionHandler) asMapKey(clientAddr net.Addr) (string, error) {
	if tcpAddr, ok := clientAddr.(*net.TCPAddr); !ok {
		return "", fmt.Errorf("%v: expected TCP address, got: %v", h, clientAddr)
	} else {
		return fmt.Sprintf("%v__%v__%v", string(tcpAddr.IP), tcpAddr.Port, tcpAddr.Zone), nil
	}
}
