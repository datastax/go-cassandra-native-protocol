package message

import (
	"encoding/base64"
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type AuthChallenge struct {
	Token []byte
}

func (m *AuthChallenge) IsResponse() bool {
	return true
}

func (m *AuthChallenge) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthChallenge
}

func (m *AuthChallenge) String() string {
	return "AUTH_CHALLENGE token: " + base64.StdEncoding.EncodeToString(m.Token)
}

type AuthChallengeCodec struct{}

func (c *AuthChallengeCodec) Encode(msg Message, dest io.Writer, _ cassandraprotocol.ProtocolVersion) error {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	if authChallenge.Token == nil {
		return errors.New("AUTH_CHALLENGE token cannot be nil")
	}
	return primitives.WriteBytes(authChallenge.Token, dest)
}

func (c *AuthChallengeCodec) EncodedLength(msg Message, _ cassandraprotocol.ProtocolVersion) (int, error) {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	return primitives.LengthOfBytes(authChallenge.Token), nil
}

func (c *AuthChallengeCodec) Decode(source io.Reader, _ cassandraprotocol.ProtocolVersion) (Message, error) {
	if token, err := primitives.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthChallenge{Token: token}, nil
	}
}

func (c *AuthChallengeCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeAuthChallenge
}
