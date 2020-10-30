package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Authenticate struct {
	Authenticator string
}

func (m *Authenticate) IsResponse() bool {
	return true
}

func (m *Authenticate) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthenticate
}

func (m *Authenticate) String() string {
	return "AUTHENTICATE " + m.Authenticator
}

type authenticateCodec struct{}

func (c *authenticateCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	authenticate, ok := msg.(*Authenticate)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Authenticate, got %T", msg))
	}
	if authenticate.Authenticator == "" {
		return errors.New("AUTHENTICATE authenticator cannot be empty")
	}
	return primitive.WriteString(authenticate.Authenticator, dest)
}

func (c *authenticateCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	authenticate, ok := msg.(*Authenticate)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Authenticate, got %T", msg))
	}
	return primitive.LengthOfString(authenticate.Authenticator), nil
}

func (c *authenticateCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if authenticator, err := primitive.ReadString(source); err != nil {
		return nil, err
	} else {
		return &Authenticate{Authenticator: authenticator}, nil
	}
}

func (c *authenticateCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeAuthenticate
}
