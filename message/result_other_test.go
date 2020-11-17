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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResultCodec_Encode_Other(test *testing.T) {
	codec := &resultCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"void result",
					&VoidResult{},
					[]byte{
						0, 0, 0, 1, // result type
					},
					nil,
				},
				{
					"set keyspace result",
					&SetKeyspaceResult{Keyspace: "ks1"},
					[]byte{
						0, 0, 0, 3, // result type
						0, 3, k, s, _1,
					},
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestResultCodec_EncodedLength_Other(test *testing.T) {
	codec := &resultCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"void result",
					&VoidResult{},
					primitive.LengthOfInt,
					nil,
				},
				{
					"set keyspace result",
					&SetKeyspaceResult{Keyspace: "ks1"},
					primitive.LengthOfInt + primitive.LengthOfString("ks1"),
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestResultCodec_Decode_Other(test *testing.T) {
	codec := &resultCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"void result",
					[]byte{
						0, 0, 0, 1, // result type
					},
					&VoidResult{},
					nil,
				},
				{
					"set keyspace result",
					[]byte{
						0, 0, 0, 3, // result type
						0, 3, k, s, _1,
					},
					&SetKeyspaceResult{Keyspace: "ks1"},
					nil,
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}
