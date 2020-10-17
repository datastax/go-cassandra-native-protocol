package message

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type AuthChallenge struct {
	Token []byte
}

func (m AuthChallenge) IsResponse() bool {
	return true
}

func (m AuthChallenge) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthChallenge
}

func (m AuthChallenge) String() string {
	return "AUTH_CHALLENGE " + string(m.Token)
}

type AuthChallengeCodec struct{}

func (c AuthChallengeCodec) Encode(msg Message, dest []byte, _ cassandraprotocol.ProtocolVersion) error {
	authChallenge := msg.(*AuthChallenge)
	_, err := primitives.WriteBytes(authChallenge.Token, dest)
	return err
}

func (c AuthChallengeCodec) EncodedSize(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authChallenge := msg.(*AuthChallenge)
	return primitives.LengthOfBytes(authChallenge.Token), nil
}

func (c AuthChallengeCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	token, _, err := primitives.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthChallenge{Token: token}, nil
}
