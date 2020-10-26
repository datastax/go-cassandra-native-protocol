package client

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"net"
)

type CqlConnection struct {
	conn  net.Conn
	codec *frame.Codec
}

func (c *CqlConnection) Send(f *frame.Frame) error {
	return c.codec.Encode(f, c.conn)
}

func (c *CqlConnection) Receive() (*frame.Frame, error) {
	return c.codec.Decode(c.conn)
}

func (c *CqlConnection) ReceiveHeader() (header *frame.RawHeader, err error) {
	if header, err = c.codec.DecodeHeader(c.conn); err != nil {
		return nil, err
	} else if err = c.codec.DiscardBody(header.BodyLength, c.conn); err != nil {
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
