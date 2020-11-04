package client

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"net"
)

type CqlClient struct {
	address string
	codec   frame.RawCodec
}

func NewCqlClient(address string, codec frame.RawCodec) *CqlClient {
	return &CqlClient{
		address: address,
		codec:   codec,
	}
}

func (client *CqlClient) Connect() (*CqlConnection, error) {
	c, err := net.Dial("tcp", client.address)
	if err != nil {
		return nil, err
	}
	return NewCqlConnection(c, client.codec), nil
}
