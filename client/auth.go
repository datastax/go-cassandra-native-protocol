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
	token := bytes.NewBuffer(make([]byte, 0, len(a.username)+len(a.password)+2))
	token.WriteByte(0)
	token.WriteString(a.username)
	token.WriteByte(0)
	token.WriteString(a.password)
	return token.Bytes(), nil
}

func (a *PlainTextAuthenticator) GetInitialServerChallenge() []byte {
	return initialServerChallenge
}

func (a *PlainTextAuthenticator) GetMechanism() []byte {
	return mechanism
}
