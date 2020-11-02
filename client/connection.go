package client

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"net"
)

type CqlConnection struct {
	conn  net.Conn
	codec frame.RawCodec
}

func (c *CqlConnection) Send(f *frame.Frame) error {
	return c.codec.EncodeFrame(f, c.conn)
}

func (c *CqlConnection) Receive() (*frame.Frame, error) {
	return c.codec.DecodeFrame(c.conn)
}

func (c *CqlConnection) ReceiveHeader() (header *frame.Header, err error) {
	if header, err = c.codec.DecodeHeader(c.conn); err != nil {
		return nil, err
	} else if err = c.codec.DiscardBody(header, c.conn); err != nil {
		return nil, err
	}
	return header, nil
}

func (c *CqlConnection) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
