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

func TestSupportedCodec_Encode(test *testing.T) {
	codec := &supportedCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected [][]byte // required because there can be multiple valid encodings
				err      error
			}{
				{
					"supported with nil options",
					&Supported{},
					[][]byte{{0, 0}},
					nil,
				},
				{
					"supported with empty options",
					&Supported{Options: map[string][]string{}},
					[][]byte{{0, 0}},
					nil,
				},
				{
					"supported with 1 option",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}}},
					[][]byte{{
						0, 1, // map length
						// key "option1"
						0, 7, o, p, t, i, o, n, _1,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _1, a,
						// value1b
						0, 7, v, a, l, u, e, _1, b,
					}},
					nil,
				},
				{
					"supported with 2 options",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}, "option2": {"value2a", "value2b"}}},
					// we have two possible encodings because maps do not have deterministic iteration order
					[][]byte{
						{
							0, 2, // map length
							// key "option1"
							0, 7, o, p, t, i, o, n, _1,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _1, a,
							// value1b
							0, 7, v, a, l, u, e, _1, b,
							// key "option2"
							0, 7, o, p, t, i, o, n, _2,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _2, a,
							// value1b
							0, 7, v, a, l, u, e, _2, b,
						},
						{
							0, 2, // map length
							// key "option2"
							0, 7, o, p, t, i, o, n, _2,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _2, a,
							// value1b
							0, 7, v, a, l, u, e, _2, b,
							// key "option1"
							0, 7, o, p, t, i, o, n, _1,
							// list length
							0, 2,
							// value1a
							0, 7, v, a, l, u, e, _1, a,
							// value1b
							0, 7, v, a, l, u, e, _1, b,
						},
					},
					nil,
				},
				{
					"not a supported",
					&Options{},
					nil,
					errors.New("expected *message.Supported, got *message.Options"),
				},
			}
			for _, tt := range tests {
				test.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					if err == nil {
						assert.Contains(t, tt.expected, dest.Bytes())
						assert.Nil(t, tt.err)
					} else {
						assert.Equal(t, tt.err, err)
					}
				})
			}
		})
	}
}

func TestSupportedCodec_EncodedLength(test *testing.T) {
	codec := &supportedCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"supported with nil options",
					&Supported{},
					primitive.LengthOfShort, // map length
					nil,
				},
				{
					"supported with empty options",
					&Supported{Options: map[string][]string{}},
					primitive.LengthOfShort, // map length
					nil,
				},
				{
					"supported with 1 option",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}}},
					primitive.LengthOfShort + // map length
						primitive.LengthOfString("option1") + // map key
						primitive.LengthOfShort + // list length
						primitive.LengthOfString("value1a") + // map value
						primitive.LengthOfString("value1b"), // map value
					nil,
				},
				{
					"supported with 2 options",
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}, "option2": {"value2a", "value2b"}}},
					primitive.LengthOfShort + // map length
						primitive.LengthOfString("option1") + // map key
						primitive.LengthOfShort + // list length
						primitive.LengthOfString("value1a") + // map value
						primitive.LengthOfString("value1b") + // map value
						primitive.LengthOfString("option2") + // map key
						primitive.LengthOfShort + // list length
						primitive.LengthOfString("value2a") + // map value
						primitive.LengthOfString("value2b"), // map value
					nil,
				},
				{
					"not a supported",
					&Options{},
					-1,
					errors.New("expected *message.Supported, got *message.Options"),
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

func TestSupportedCodec_Decode(test *testing.T) {
	codec := &supportedCodec{}
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"supported with empty options",
					[]byte{0, 0},
					&Supported{Options: map[string][]string{}},
					nil,
				},
				{
					"supported with 1 option",
					[]byte{
						0, 1, // map length
						// key "option1"
						0, 7, o, p, t, i, o, n, _1,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _1, a,
						// value1b
						0, 7, v, a, l, u, e, _1, b,
					},
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}}},
					nil,
				},
				{
					"supported with 2 options",
					// we have two possible encodings because maps do not have deterministic iteration order
					[]byte{
						0, 2, // map length
						// key "option1"
						0, 7, o, p, t, i, o, n, _1,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _1, a,
						// value1b
						0, 7, v, a, l, u, e, _1, b,
						// key "option2"
						0, 7, o, p, t, i, o, n, _2,
						// list length
						0, 2,
						// value1a
						0, 7, v, a, l, u, e, _2, a,
						// value1b
						0, 7, v, a, l, u, e, _2, b,
					},
					&Supported{Options: map[string][]string{"option1": {"value1a", "value1b"}, "option2": {"value2a", "value2b"}}},
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
