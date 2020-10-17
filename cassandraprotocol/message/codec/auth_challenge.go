package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type AuthChallengeCodec struct{}

func (c AuthChallengeCodec) Encode(msg message.Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authChallenge := msg.(*message.AuthChallenge)
	_, err := primitives.WriteBytes(authChallenge.Token, dest)
	return err
}

func (c AuthChallengeCodec) EncodedSize(msg message.Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authChallenge := msg.(*message.AuthChallenge)
	return primitives.LengthOfBytes(authChallenge.Token), nil
}

func (c AuthChallengeCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (message.Message, error) {
	token, _, err := primitives.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &message.AuthChallenge{Token: token}, nil
}
