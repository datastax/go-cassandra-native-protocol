package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type QueryCodec struct {
}

func (c QueryCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) (err error) {
	query := msg.(*message.Query)
	dest, err = primitives.WriteLongString(query.Query, dest)
	if err == nil {
		_, err = EncodeQueryOptions(query.Options, dest, version)
	}
	return err
}

func (q QueryCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	query := msg.(*message.Query)
	return primitives.SizeOfLongString(query.Query) + SizeOfQueryOptions(query.Options, version), nil
}

func (q QueryCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (msg message.Message, err error) {
	var query string
	query, source, err = primitives.ReadLongString(source)
	if err == nil {
		var options *message.QueryOptions
		options, source, err = DecodeQueryOptions(source, version)
		if err == nil {
			return message.Query{Query: query, Options: options}, nil
		}
	}
	return nil, err
}
