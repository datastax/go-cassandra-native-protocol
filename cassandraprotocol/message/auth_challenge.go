package message

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type AuthChallenge struct {
	Token []byte
}

func (m *AuthChallenge) IsResponse() bool {
	return true
}

func (m *AuthChallenge) GetOpCode() primitives.OpCode {
	return primitives.OpCodeAuthChallenge
}

func (m *AuthChallenge) String() string {
	return "AUTH_CHALLENGE token: " + base64.StdEncoding.EncodeToString(m.Token)
}

type AuthChallengeCodec struct{}

func (c *AuthChallengeCodec) Encode(msg Message, dest io.Writer, _ primitives.ProtocolVersion) error {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	if authChallenge.Token == nil {
		return errors.New("AUTH_CHALLENGE token cannot be nil")
	}
	return primitives.WriteBytes(authChallenge.Token, dest)
}

func (c *AuthChallengeCodec) EncodedLength(msg Message, _ primitives.ProtocolVersion) (int, error) {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	return primitives.LengthOfBytes(authChallenge.Token), nil
}

func (c *AuthChallengeCodec) Decode(source io.Reader, _ primitives.ProtocolVersion) (Message, error) {
	if token, err := primitives.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthChallenge{Token: token}, nil
	}
}

func (c *AuthChallengeCodec) GetOpCode() primitives.OpCode {
	return primitives.OpCodeAuthChallenge
}
