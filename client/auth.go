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
	"bytes"
	"fmt"
)

// AuthCredentials encapsulates a username and a password to use with plain-text authenticators.
type AuthCredentials struct {
	Username string
	Password string
}

func (c *AuthCredentials) String() string {
	return fmt.Sprintf("AuthCredentials{username: %v}", c.Username)
}

// Marshal serializes the current credentials to an authentication token with the expected format for
// PasswordAuthenticator.
func (c *AuthCredentials) Marshal() []byte {
	token := bytes.NewBuffer(make([]byte, 0, len(c.Username)+len(c.Password)+2))
	token.WriteByte(0)
	token.WriteString(c.Username)
	token.WriteByte(0)
	token.WriteString(c.Password)
	return token.Bytes()
}

// Unmarshal deserializes an authentication token with the expected format for PasswordAuthenticator into the current
// AuthCredentials.
func (c *AuthCredentials) Unmarshal(token []byte) error {
	token = append(token, 0)
	source := bytes.NewBuffer(token)
	if _, err := source.ReadByte(); err != nil {
		return err
	} else if username, err := source.ReadString(0); err != nil {
		return err
	} else if password, err := source.ReadString(0); err != nil {
		return err
	} else {
		c.Username = username[:len(username)-1]
		c.Password = password[:len(password)-1]
		return nil
	}
}

func (c AuthCredentials) Copy() *AuthCredentials {
	return &c
}

// A simple authenticator to perform plain-text authentications for CQL clients.
type PlainTextAuthenticator struct {
	Credentials *AuthCredentials
}

var (
	expectedChallenge = []byte("PLAIN-START")
	mechanism         = []byte("PLAIN")
)

func (a *PlainTextAuthenticator) InitialResponse(authenticator string) ([]byte, error) {
	switch authenticator {
	case "com.datastax.bdp.cassandra.auth.DseAuthenticator":
		return mechanism, nil
	case "org.apache.cassandra.auth.PasswordAuthenticator":
		return a.Credentials.Marshal(), nil
	}
	return nil, fmt.Errorf("unknown authenticator: %v", authenticator)
}

func (a *PlainTextAuthenticator) EvaluateChallenge(challenge []byte) ([]byte, error) {
	if challenge == nil || bytes.Compare(challenge, expectedChallenge) != 0 {
		return nil, fmt.Errorf("incorrect SASL challenge from server, expecting PLAIN-START, got: %v", string(challenge))
	}
	return a.Credentials.Marshal(), nil
}
