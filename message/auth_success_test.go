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

func TestAuthSuccessCodec_Encode(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &authSuccessCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"simple auth success",
					&AuthSuccess{token},
					[]byte{0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe},
					nil,
				},
				{
					"not an auth success",
					&AuthChallenge{token},
					nil,
					errors.New("expected *message.AuthSuccess, got *message.AuthChallenge"),
				},
				{
					"auth success empty token",
					&AuthSuccess{[]byte{}},
					[]byte{0, 0, 0, 0},
					nil,
				},
				{
					"auth success nil token",
					&AuthSuccess{nil},
					[]byte{0xff, 0xff, 0xff, 0xff},
					nil,
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

func TestAuthSuccessCodec_EncodedLength(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &authSuccessCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"simple auth success",
					&AuthSuccess{token},
					primitive.LengthOfBytes(token),
					nil,
				},
				{
					"not an auth success",
					&AuthResponse{token},
					-1,
					errors.New("expected *message.AuthSuccess, got *message.AuthResponse"),
				},
				{
					"auth success nil token",
					&AuthSuccess{nil},
					primitive.LengthOfBytes(nil),
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

func TestAuthSuccessCodec_Decode(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &authSuccessCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"simple auth success",
					[]byte{0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe},
					&AuthSuccess{token},
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
