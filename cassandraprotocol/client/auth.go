package client

import (
	"bytes"
	"errors"
	"fmt"
)

type Authenticator interface {
	InitialResponse(name string) ([]byte, error)
	EvaluateChallenge(challenge []byte) ([]byte, error)
	GetInitialServerChallenge() []byte
	GetMechanism() []byte
}

type PlainTextAuthenticator struct {
	username string
	password string
}

var (
	initialServerChallenge = []byte("PLAIN-START")
	mechanism              = []byte("PLAIN")
)

func (a *PlainTextAuthenticator) InitialResponse(authenticator string) ([]byte, error) {
	switch authenticator {
	case "com.datastax.bdp.cassandra.auth.DseAuthenticator":
		return a.GetMechanism(), nil
	case "org.apache.cassandra.auth.PasswordAuthenticator":
		return a.EvaluateChallenge(a.GetInitialServerChallenge())
	}
	return nil, fmt.Errorf("unknown authenticator: %v", authenticator)
}

func (a *PlainTextAuthenticator) EvaluateChallenge(challenge []byte) ([]byte, error) {
	if challenge == nil || bytes.Compare(challenge, initialServerChallenge) != 0 {
		return nil, errors.New("incorrect SASL challenge from server")
	}
	buffer := make([]byte, len(a.username)+len(a.password)+2)
	buffer[0] = 0
	buffer = append(buffer[0:1], []byte(a.username)...)
	buffer = append(buffer, 0)
	buffer = append(buffer, []byte(a.password)...)
	return buffer, nil
}

func (a *PlainTextAuthenticator) GetInitialServerChallenge() []byte {
	return initialServerChallenge
}

func (a *PlainTextAuthenticator) GetMechanism() []byte {
	return mechanism
}
