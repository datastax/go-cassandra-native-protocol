package client

import (
	"errors"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"net"
	"time"
)

type CqlServer struct {
	address  string
	codec    *frame.Codec
	listener net.Listener
}

func NewCqlServer(address string, codec *frame.Codec) *CqlServer {
	return &CqlServer{
		address: address,
		codec:   codec,
	}
}

func (server *CqlServer) Listen() (err error) {
	if server.listener == nil {
		server.listener, err = net.Listen("tcp", server.address)
	}
	return err
}

func (server *CqlServer) Bind(client *CqlClient) (clientConn *CqlConnection, serverConn *CqlConnection, err error) {
	if err = server.Listen(); err != nil {
		return nil, nil, err
	}
	type result struct {
		conn *CqlConnection
		err  error
	}
	clientChan := make(chan *result)
	serverChan := make(chan *result)
	go func() {
		conn, err := client.Connect()
		clientChan <- &result{conn, err}
	}()
	go func() {
		conn, err := server.listener.Accept()
		serverChan <- &result{&CqlConnection{conn, server.codec}, err}
	}()
	select {
	case result := <-clientChan:
		if result.err != nil {
			return nil, nil, err
		}
		clientConn = result.conn
	case <-time.After(time.Minute):
		return nil, nil, errors.New("timeout waiting for client")
	}
	select {
	case result := <-serverChan:
		if result.err != nil {
			return nil, nil, err
		}
		serverConn = result.conn
	case <-time.After(time.Minute):
		return nil, nil, errors.New("timeout waiting for server")
	}
	return
}

func (server *CqlServer) Close() error {
	if server.listener != nil {
		return server.listener.Close()
	}
	return nil
}
