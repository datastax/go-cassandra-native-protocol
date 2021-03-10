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

package message

import (
	"bytes"
	"errors"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthenticate_Clone(t *testing.T) {
	msg := &Authenticate{
		Authenticator: "auth",
	}
	cloned := msg.Clone().(*Authenticate)
	assert.Equal(t, msg, cloned)
	cloned.Authenticator = "auth2"
	assert.Equal(t, "auth", msg.Authenticator)
	assert.Equal(t, "auth2", cloned.Authenticator)
	assert.NotEqual(t, msg, cloned)
}

func TestAuthenticateCodec_Encode(t *testing.T) {
	codec := &authenticateCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"simple authenticate",
					&Authenticate{"dummy"},
					[]byte{0, 5, d, u, m, m, y},
					nil,
				},
				{
					"not an authenticate",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					nil,
					errors.New("expected *message.Authenticate, got *message.AuthChallenge"),
				},
				{
					"authenticate nil authenticator",
					&Authenticate{""},
					nil,
					errors.New("AUTHENTICATE authenticator cannot be empty"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestAuthenticateCodec_EncodedLength(t *testing.T) {
	codec := &authenticateCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"simple authenticate",
					&Authenticate{"dummy"},
					primitive.LengthOfString("dummy"),
					nil,
				},
				{
					"not an authenticate",
					&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
					-1,
					errors.New("expected *message.Authenticate, got *message.AuthChallenge"),
				},
				{
					"authenticate nil authenticator",
					&Authenticate{""},
					primitive.LengthOfString(""),
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestAuthenticateCodec_Decode(t *testing.T) {
	codec := &authenticateCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"simple authenticate",
					[]byte{0, 5, d, u, m, m, y},
					&Authenticate{"dummy"},
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}
