package client

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func NewStartupRequest(c *CqlConnection, version primitive.ProtocolVersion, streamId int16) *frame.Frame {
	var startup *message.Startup
	startup = message.NewStartup()
	if c.codec.CompressionAlgorithm() != "NONE" {
		startup.SetCompression(c.codec.CompressionAlgorithm())
	}
	request, _ := frame.NewRequestFrame(version, streamId, false, nil, startup)
	return request
}

func Handshake(client *CqlConnection, version primitive.ProtocolVersion, streamId int16) error {
	if err := client.Send(NewStartupRequest(client, version, streamId)); err != nil {
		return err
	} else if response, err := client.Receive(); err != nil {
		return err
	} else if _, ok := response.Body.Message.(*message.Ready); !ok {
		return fmt.Errorf("expected READY, got %T", response.Body.Message)
	} else {
		return nil
	}
}

func HandshakeAuth(client *CqlConnection, version primitive.ProtocolVersion, streamId int16, username string, password string) error {
	authenticator := &PlainTextAuthenticator{
		username: username,
		password: password,
	}
	if err := client.Send(NewStartupRequest(client, version, streamId)); err != nil {
		return err
	} else if response, err := client.Receive(); err != nil {
		return err
	} else if authenticate, ok := response.Body.Message.(*message.Authenticate); !ok {
		return fmt.Errorf("expected AUTHENTICATE, got %v", response.Body.Message)
	} else if initialResponse, err := authenticator.InitialResponse(authenticate.Authenticator); err != nil {
		return err
	} else if authResponse, err := frame.NewRequestFrame(version, streamId, false, nil, &message.AuthResponse{Token: initialResponse}); err != nil {
		return err
	} else if err := client.Send(authResponse); err != nil {
		return err
	} else if response, err := client.Receive(); err != nil {
		return err
	} else if _, isAuthSuccess := response.Body.Message.(*message.AuthSuccess); isAuthSuccess {
		return nil
	} else if authChallenge, isAuthChallenge := response.Body.Message.(*message.AuthChallenge); !isAuthChallenge {
		return fmt.Errorf("expected AUTH_CHALLENGE or AUTH_SUCCESS, got %v", response.Body.Message)
	} else if challenge, err := authenticator.EvaluateChallenge(authChallenge.Token); err != nil {
		return err
	} else if authResponse, err := frame.NewRequestFrame(version, streamId, false, nil, &message.AuthResponse{Token: challenge}); err != nil {
		return err
	} else if err := client.Send(authResponse); err != nil {
		return err
	} else if response, err := client.Receive(); err != nil {
		return err
	} else if _, isAuthSuccess := response.Body.Message.(*message.AuthSuccess); !isAuthSuccess {
		return fmt.Errorf("expected AUTH_SUCCESS, got %v", response.Body.Message)
	} else {
		return nil
	}
}
