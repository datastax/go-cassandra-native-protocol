// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// PerformHandshake performs a handshake between the given client and server connections, using the provided protocol
// version. The handshake will use stream id 1, unless the client connection is in managed mode.
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

// InitiateHandshake initiates the handshake procedure to initialize the client connection, using the given protocol
// version. The handshake will use authentication if the connection was created with auth credentials; otherwise it will
// proceed without authentication. Use stream id zero to activate automatic stream id management.
func (c *CqlClientConnection) InitiateHandshake(version primitive.ProtocolVersion, streamId int16) (err error) {
	log.Debug().Msgf("%v: performing handshake", c)
	if startup, err := c.NewStartupRequest(version, streamId); err != nil {
		return err
	} else {
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
						authResponse := frame.NewFrame(version, streamId, &message.AuthResponse{Token: initialResponse})
						if response, err = c.SendAndReceive(authResponse); err != nil {
							err = fmt.Errorf("could not send AUTH RESPONSE: %w", err)
						} else {
							switch msg := response.Body.Message.(type) {
							case *message.AuthSuccess:
								break
							case *message.AuthChallenge:
								var challenge []byte
								if challenge, err = authenticator.EvaluateChallenge(msg.Token); err == nil {
									authResponse := frame.NewFrame(version, streamId, &message.AuthResponse{Token: challenge})
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
}

// AcceptHandshake Listens for a client STARTUP request and proceeds with the server-side handshake procedure.
// Authentication will be required if the connection was created with auth credentials; otherwise the handshake will
// proceed without authentication.
// This method is intended for use when server-side handshake should be triggered manually. For automatic server-side
// handshake, consider using HandshakeHandler instead.
func (c *CqlServerConnection) AcceptHandshake() (err error) {
	log.Debug().Msgf("%v: performing handshake", c)
	var request *frame.Frame
	authSuccess := false
	done := false
	for !done && err == nil {
		if request, err = c.Receive(); err == nil {
			switch request.Body.Message.(type) {
			case *message.Options:
				supported := frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Supported{})
				err = c.Send(supported)
				continue
			case *message.Startup:
				if c.credentials == nil {
					authSuccess = true
					ready := frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
					err = c.Send(ready)
				} else {
					authenticate := frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Authenticate{Authenticator: "org.apache.cassandra.auth.PasswordAuthenticator"})
					if err = c.Send(authenticate); err == nil {
						if request, err = c.Receive(); err == nil {
							if authResponse, ok := request.Body.Message.(*message.AuthResponse); !ok {
								err = fmt.Errorf("expected AUTH RESPONSE, got %v", request.Body.Message)
							} else {
								credentials := &AuthCredentials{}
								if err = credentials.Unmarshal(authResponse.Token); err == nil {
									if credentials.Username == c.credentials.Username && credentials.Password == c.credentials.Password {
										authSuccess = true
										authSuccess := frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.AuthSuccess{})
										err = c.Send(authSuccess)
									} else {
										authError := frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.AuthenticationError{ErrorMessage: "invalid credentials"})
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

const (
	handshakeStateKey     = "HANDSHAKE"
	handshakeStateStarted = "STARTED"
	handshakeStateDone    = "DONE"
)

// HandshakeHandler is a RequestHandler to handle server-side handshakes. This is an alternative to
// CqlServerConnection.AcceptHandshake to make the server connection automatically handle all incoming handshake
// attempts.
var HandshakeHandler RequestHandler = func(request *frame.Frame, conn *CqlServerConnection, ctx RequestHandlerContext) (response *frame.Frame) {
	if ctx.GetAttribute(handshakeStateKey) == handshakeStateDone {
		return
	}
	version := request.Header.Version
	id := request.Header.StreamId
	switch msg := request.Body.Message.(type) {
	case *message.Options:
		log.Debug().Msgf("%v: [handshake handler]: intercepted OPTIONS before STARTUP", conn)
		response = frame.NewFrame(version, id, &message.Supported{})
	case *message.Startup:
		if conn.Credentials() == nil {
			ctx.PutAttribute(handshakeStateKey, handshakeStateDone)
			log.Info().Msgf("%v: [handshake handler]: handshake successful", conn)
			response = frame.NewFrame(version, id, &message.Ready{})
		} else {
			ctx.PutAttribute(handshakeStateKey, handshakeStateStarted)
			response = frame.NewFrame(version, id, &message.Authenticate{Authenticator: "org.apache.cassandra.auth.PasswordAuthenticator"})
		}
	case *message.AuthResponse:
		if ctx.GetAttribute(handshakeStateKey) == handshakeStateStarted {
			userCredentials := &AuthCredentials{}
			if err := userCredentials.Unmarshal(msg.Token); err == nil {
				serverCredentials := conn.Credentials()
				if userCredentials.Username == serverCredentials.Username &&
					userCredentials.Password == serverCredentials.Password {
					log.Info().Msgf("%v: [handshake handler]: handshake successful", conn)
					response = frame.NewFrame(version, id, &message.AuthSuccess{})
				} else {
					log.Error().Msgf("%v: [handshake handler]: authentication error: invalid credentials", conn)
					response = frame.NewFrame(version, id, &message.AuthenticationError{ErrorMessage: "invalid credentials"})
				}
				ctx.PutAttribute(handshakeStateKey, handshakeStateDone)
			}
		} else {
			ctx.PutAttribute(handshakeStateKey, handshakeStateDone)
			log.Error().Msgf("%v: [handshake handler]: expected STARTUP, got AUTH_RESPONSE", conn)
			response = frame.NewFrame(version, id, &message.ProtocolError{ErrorMessage: "handshake failed"})
		}
	default:
		ctx.PutAttribute(handshakeStateKey, handshakeStateDone)
		log.Error().Msgf("%v: [handshake handler]: expected OPTIONS, STARTUP or AUTH_RESPONSE, got %v", conn, msg)
		response = frame.NewFrame(version, id, &message.ProtocolError{ErrorMessage: "handshake failed"})
	}
	return
}

func isReady(f *frame.Frame) bool {
	_, ok := f.Body.Message.(*message.Ready)
	return ok
}

func isAuthenticate(f *frame.Frame) bool {
	_, ok := f.Body.Message.(*message.Authenticate)
	return ok
}
