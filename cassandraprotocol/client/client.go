package client

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"net"
)

type CqlClient struct {
	address string
	codec   *frame.Codec
}

func NewCqlClient(address string, codec *frame.Codec) *CqlClient {
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
	return &CqlConnection{
		conn:  c,
		codec: client.codec,
	}, nil
}
