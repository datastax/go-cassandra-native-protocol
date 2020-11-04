package client

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func NewStartupRequest(c *CqlConnection, version primitive.ProtocolVersion, streamId int16) *frame.Frame {
	startup := message.NewStartup()
	if c.codec.GetBodyCompressor() != nil {
		startup.SetCompression(c.codec.GetBodyCompressor().Algorithm())
	}
	request, _ := frame.NewRequestFrame(version, streamId, false, nil, startup, false)
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

func HandshakeAuth(
	client *CqlConnection,
	version primitive.ProtocolVersion,
	streamId int16,
	username string,
	password string,
	optionalAuth bool) error {
	authenticator := &PlainTextAuthenticator{
		username: username,
		password: password,
	}
	if err := client.Send(NewStartupRequest(client, version, streamId)); err != nil {
		return err
	}

	response, err := client.Receive()
	if err != nil {
		return err
	}

	var authenticate *message.Authenticate
	switch responseMsg := response.Body.Message.(type) {
	case *message.Authenticate:
		authenticate = responseMsg
	case *message.Ready:
		if optionalAuth {
			return nil
		} else {
			return fmt.Errorf("got READY but optionalAuth is false")
		}
	default:
		return fmt.Errorf("expected AUTHENTICATE or READY, got %v", responseMsg)
	}

	if initialResponse, err := authenticator.InitialResponse(authenticate.Authenticator); err != nil {
		return err
	} else if authResponse, err := frame.NewRequestFrame(version, streamId, false, nil, &message.AuthResponse{Token: initialResponse}, false); err != nil {
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
	} else if authResponse, err := frame.NewRequestFrame(version, streamId, false, nil, &message.AuthResponse{Token: challenge}, false); err != nil {
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
