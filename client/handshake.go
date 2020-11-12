package client

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/rs/zerolog/log"
)

// Performs a handshake between the given client and server connections, using the provided protocol version. The
// handshake will use stream id 1, unless the client connection is in managed mode.
func PerformHandshake(clientConn *CqlClientConnection, serverConn *CqlServerConnection, version primitive.ProtocolVersion, streamId int16) error {
	clientChan := make(chan error)
	serverChan := make(chan error)
	go func() {
		clientChan <- clientConn.InitiateHandshake(version, streamId)
	}()
	go func() {
		serverChan <- serverConn.AcceptHandshake()
	}()
	for clientChan != nil || serverChan != nil {
		select {
		case err := <-clientChan:
			if err != nil {
				return fmt.Errorf("client handshake failed: %w", err)
			}
			clientChan = nil
		case err := <-serverChan:
			if err != nil {
				return fmt.Errorf("server handshake failed %w", err)
			}
			serverChan = nil
		}
	}
	return nil
}

// Initiates the handshake procedure to initialize the client connection, using the given protocol version.
// The handshake will use authentication if the connection was created with auth credentials; otherwise it will
// proceed without authentication. Use stream id zero to activate automatic stream id management.
func (c *CqlClientConnection) InitiateHandshake(version primitive.ProtocolVersion, streamId int16) (err error) {
	log.Debug().Msgf("%v: performing handshake", c)
	startup := c.NewStartupRequest(version, streamId)
	var response *frame.Frame
	if response, err = c.SendAndReceive(startup); err == nil {
		if c.credentials == nil {
			if _, authSuccess := response.Body.Message.(*message.Ready); !authSuccess {
				err = fmt.Errorf("expected READY, got %v", response.Body.Message)
			}
		} else {
			switch msg := response.Body.Message.(type) {
			case *message.Ready:
				log.Warn().Msgf("%v: expected AUTHENTICATE, got READY – is authentication required?", c)
				break
			case *message.Authenticate:
				authenticator := &PlainTextAuthenticator{c.credentials}
				var initialResponse []byte
				if initialResponse, err = authenticator.InitialResponse(msg.Authenticator); err == nil {
					authResponse, _ := frame.NewRequestFrame(version, streamId, false, nil, &message.AuthResponse{Token: initialResponse}, false)
					if response, err = c.SendAndReceive(authResponse); err != nil {
						err = fmt.Errorf("could not send AUTH RESPONSE: %w", err)
					} else {
						switch msg := response.Body.Message.(type) {
						case *message.AuthSuccess:
							break
						case *message.AuthChallenge:
							var challenge []byte
							if challenge, err = authenticator.EvaluateChallenge(msg.Token); err == nil {
								authResponse, _ := frame.NewRequestFrame(version, streamId, false, nil, &message.AuthResponse{Token: challenge}, false)
								if response, err = c.SendAndReceive(authResponse); err != nil {
									err = fmt.Errorf("could not send AUTH RESPONSE: %w", err)
								} else if _, authSuccess := response.Body.Message.(*message.AuthSuccess); !authSuccess {
									err = fmt.Errorf("expected AUTH_SUCCESS, got %v", response.Body.Message)
								}
							}
						default:
							err = fmt.Errorf("expected AUTH_CHALLENGE or AUTH_SUCCESS, got %v", response.Body.Message)
						}
					}
				}
			default:
				err = fmt.Errorf("expected AUTHENTICATE or READY, got %v", response.Body.Message)
			}
		}
	}
	if err == nil {
		log.Info().Msgf("%v: handshake successful", c)
	} else {
		log.Error().Err(err).Msgf("%v: handshake failed", c)
	}
	return err
}

// Listens for a client STARTUP request and proceeds with the server-side handshake procedure. Authentication will be
// required if the connection was created with auth credentials; otherwise the handshake will proceed without
// authentication.
func (c *CqlServerConnection) AcceptHandshake() (err error) {
	log.Debug().Msgf("%v: performing handshake", c)
	var request *frame.Frame
	authSuccess := false
	done := false
	for !done && err == nil {
		if request, err = c.Receive(); err == nil {
			switch request.Body.Message.(type) {
			case *message.Options:
				supported, _ := frame.NewResponseFrame(request.Header.Version, request.Header.StreamId, nil, nil, nil, &message.Supported{}, false)
				err = c.Send(supported)
				continue
			case *message.Startup:
				if c.credentials == nil {
					authSuccess = true
					ready, _ := frame.NewResponseFrame(request.Header.Version, request.Header.StreamId, nil, nil, nil, &message.Ready{}, false)
					err = c.Send(ready)
				} else {
					authenticate, _ := frame.NewResponseFrame(request.Header.Version, request.Header.StreamId, nil, nil, nil, &message.Authenticate{Authenticator: "org.apache.cassandra.auth.PasswordAuthenticator"}, false)
					if err = c.Send(authenticate); err == nil {
						if request, err = c.Receive(); err == nil {
							if authResponse, ok := request.Body.Message.(*message.AuthResponse); !ok {
								err = fmt.Errorf("expected AUTH RESPONSE, got %v", request.Body.Message)
							} else {
								credentials := &AuthCredentials{}
								if err = credentials.Unmarshal(authResponse.Token); err == nil {
									if credentials.Username == c.credentials.Username && credentials.Password == c.credentials.Password {
										authSuccess = true
										authSuccess, _ := frame.NewResponseFrame(request.Header.Version, request.Header.StreamId, nil, nil, nil, &message.AuthSuccess{}, false)
										err = c.Send(authSuccess)
									} else {
										authError, _ := frame.NewResponseFrame(request.Header.Version, request.Header.StreamId, nil, nil, nil, &message.AuthenticationError{ErrorMessage: "invalid credentials"}, false)
										err = c.Send(authError)
									}
								}
							}
						}
					}
				}
				done = true
			default:
				err = fmt.Errorf("expected STARTUP or OPTIONS, got %v", request.Body.Message)
				done = true
			}
		}
	}
	if err == nil {
		if authSuccess {
			log.Info().Msgf("%v: handshake successful", c)
		} else {
			log.Error().Msgf("%v: authentication error: invalid credentials", c)
		}
	} else {
		log.Error().Err(err).Msgf("%v: handshake failed", c)
	}
	return err
}
