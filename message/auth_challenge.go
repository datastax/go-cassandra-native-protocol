package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type AuthChallenge struct {
	Token []byte
}

func (m *AuthChallenge) IsResponse() bool {
	return true
}

func (m *AuthChallenge) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthChallenge
}

func (m *AuthChallenge) String() string {
	return "AUTH_CHALLENGE"
}

type authChallengeCodec struct{}

func (c *authChallengeCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	if authChallenge.Token == nil {
		return errors.New("AUTH_CHALLENGE token cannot be nil")
	}
	return primitive.WriteBytes(authChallenge.Token, dest)
}

func (c *authChallengeCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	authChallenge, ok := msg.(*AuthChallenge)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.AuthChallenge, got %T", msg))
	}
	return primitive.LengthOfBytes(authChallenge.Token), nil
}

func (c *authChallengeCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if token, err := primitive.ReadBytes(source); err != nil {
		return nil, err
	} else {
		return &AuthChallenge{Token: token}, nil
	}
}

func (c *authChallengeCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthChallenge
}
