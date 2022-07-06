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
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestServerError_DeepCopy(t *testing.T) {
	msg := &ServerError{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestProtocolError_DeepCopy(t *testing.T) {
	msg := &ProtocolError{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestAuthenticationError_DeepCopy(t *testing.T) {
	msg := &AuthenticationError{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestOverloaded_DeepCopy(t *testing.T) {
	msg := &Overloaded{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestIsBootstrapping_DeepCopy(t *testing.T) {
	msg := &IsBootstrapping{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestTruncateError_DeepCopy(t *testing.T) {
	msg := &TruncateError{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestSyntaxError_DeepCopy(t *testing.T) {
	msg := &SyntaxError{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestUnauthorized_DeepCopy(t *testing.T) {
	msg := &Unauthorized{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestInvalid_DeepCopy(t *testing.T) {
	msg := &Invalid{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestConfigError_DeepCopy(t *testing.T) {
	msg := &ConfigError{
		ErrorMessage: "msg",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
}

func TestUnavailable_DeepCopy(t *testing.T) {
	msg := &Unavailable{
		ErrorMessage: "msg",
		Consistency:  primitive.ConsistencyLevelAll,
		Required:     2,
		Alive:        1,
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Consistency = primitive.ConsistencyLevelEachQuorum
	cloned.Required = 3
	cloned.Alive = 2
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelAll, msg.Consistency)
	assert.EqualValues(t, 2, msg.Required)
	assert.EqualValues(t, 1, msg.Alive)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelEachQuorum, cloned.Consistency)
	assert.EqualValues(t, 3, cloned.Required)
	assert.EqualValues(t, 2, cloned.Alive)
}

func TestReadTimeout_DeepCopy(t *testing.T) {
	msg := &ReadTimeout{
		ErrorMessage: "msg",
		Consistency:  primitive.ConsistencyLevelAll,
		Received:     3,
		BlockFor:     4,
		DataPresent:  false,
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Consistency = primitive.ConsistencyLevelEachQuorum
	cloned.Received = 2
	cloned.BlockFor = 3
	cloned.DataPresent = true
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelAll, msg.Consistency)
	assert.EqualValues(t, 3, msg.Received)
	assert.EqualValues(t, 4, msg.BlockFor)
	assert.False(t, msg.DataPresent)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelEachQuorum, cloned.Consistency)
	assert.EqualValues(t, 2, cloned.Received)
	assert.EqualValues(t, 3, cloned.BlockFor)
	assert.True(t, cloned.DataPresent)
}

func TestWriteTimeout_DeepCopy(t *testing.T) {
	msg := &WriteTimeout{
		ErrorMessage: "msg",
		Consistency:  primitive.ConsistencyLevelAll,
		Received:     5,
		BlockFor:     6,
		WriteType:    primitive.WriteTypeBatch,
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Consistency = primitive.ConsistencyLevelEachQuorum
	cloned.Received = 2
	cloned.BlockFor = 3
	cloned.WriteType = primitive.WriteTypeBatchLog
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelAll, msg.Consistency)
	assert.EqualValues(t, 5, msg.Received)
	assert.EqualValues(t, 6, msg.BlockFor)
	assert.Equal(t, primitive.WriteTypeBatch, msg.WriteType)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelEachQuorum, cloned.Consistency)
	assert.EqualValues(t, 2, cloned.Received)
	assert.EqualValues(t, 3, cloned.BlockFor)
	assert.Equal(t, primitive.WriteTypeBatchLog, cloned.WriteType)
}

func TestReadFailure_DeepCopy(t *testing.T) {
	msg := &ReadFailure{
		ErrorMessage: "msg",
		Consistency:  primitive.ConsistencyLevelAll,
		Received:     1,
		BlockFor:     2,
		NumFailures:  1,
		FailureReasons: []*primitive.FailureReason{
			{
				Endpoint: net.IP{0x01},
				Code:     primitive.FailureCodeCdcSpaceFull,
			},
			{
				Endpoint: net.IP{0x02},
				Code:     primitive.FailureCodeCounterWrite,
			},
		},
		DataPresent: false,
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Consistency = primitive.ConsistencyLevelEachQuorum
	cloned.Received = 2
	cloned.BlockFor = 3
	cloned.NumFailures = 2
	cloned.FailureReasons = []*primitive.FailureReason{
		{
			Endpoint: net.IP{0x05},
			Code:     primitive.FailureCodeIndexNotAvailable,
		},
		{
			Endpoint: net.IP{0x06},
			Code:     primitive.FailureCodeKeyspaceNotFound,
		},
	}
	cloned.DataPresent = true
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelAll, msg.Consistency)
	assert.EqualValues(t, 1, msg.Received)
	assert.EqualValues(t, 2, msg.BlockFor)
	assert.EqualValues(t, 1, msg.NumFailures)
	assert.Equal(t, net.IP{0x01}, msg.FailureReasons[0].Endpoint)
	assert.Equal(t, primitive.FailureCodeCdcSpaceFull, msg.FailureReasons[0].Code)
	assert.Equal(t, net.IP{0x02}, msg.FailureReasons[1].Endpoint)
	assert.Equal(t, primitive.FailureCodeCounterWrite, msg.FailureReasons[1].Code)
	assert.False(t, msg.DataPresent)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelEachQuorum, cloned.Consistency)
	assert.EqualValues(t, 2, cloned.Received)
	assert.EqualValues(t, 3, cloned.BlockFor)
	assert.EqualValues(t, 2, cloned.NumFailures)
	assert.Equal(t, net.IP{0x05}, cloned.FailureReasons[0].Endpoint)
	assert.Equal(t, primitive.FailureCodeIndexNotAvailable, cloned.FailureReasons[0].Code)
	assert.Equal(t, net.IP{0x06}, cloned.FailureReasons[1].Endpoint)
	assert.Equal(t, primitive.FailureCodeKeyspaceNotFound, cloned.FailureReasons[1].Code)
	assert.True(t, cloned.DataPresent)
}

func TestWriteFailure_DeepCopy(t *testing.T) {
	msg := &WriteFailure{
		ErrorMessage: "msg",
		Consistency:  primitive.ConsistencyLevelAll,
		Received:     1,
		BlockFor:     2,
		NumFailures:  1,
		FailureReasons: []*primitive.FailureReason{
			{
				Endpoint: net.IP{0x01},
				Code:     primitive.FailureCodeCdcSpaceFull,
			},
			{
				Endpoint: net.IP{0x02},
				Code:     primitive.FailureCodeCounterWrite,
			},
		},
		WriteType: primitive.WriteTypeBatchLog,
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Consistency = primitive.ConsistencyLevelEachQuorum
	cloned.Received = 2
	cloned.BlockFor = 3
	cloned.NumFailures = 2
	cloned.FailureReasons = []*primitive.FailureReason{
		{
			Endpoint: net.IP{0x05},
			Code:     primitive.FailureCodeIndexNotAvailable,
		},
		{
			Endpoint: net.IP{0x06},
			Code:     primitive.FailureCodeKeyspaceNotFound,
		},
	}
	cloned.WriteType = primitive.WriteTypeCdc
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelAll, msg.Consistency)
	assert.EqualValues(t, 1, msg.Received)
	assert.EqualValues(t, 2, msg.BlockFor)
	assert.EqualValues(t, 1, msg.NumFailures)
	assert.Equal(t, net.IP{0x01}, msg.FailureReasons[0].Endpoint)
	assert.Equal(t, primitive.FailureCodeCdcSpaceFull, msg.FailureReasons[0].Code)
	assert.Equal(t, net.IP{0x02}, msg.FailureReasons[1].Endpoint)
	assert.Equal(t, primitive.FailureCodeCounterWrite, msg.FailureReasons[1].Code)
	assert.Equal(t, primitive.WriteTypeBatchLog, msg.WriteType)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, primitive.ConsistencyLevelEachQuorum, cloned.Consistency)
	assert.EqualValues(t, 2, cloned.Received)
	assert.EqualValues(t, 3, cloned.BlockFor)
	assert.EqualValues(t, 2, cloned.NumFailures)
	assert.Equal(t, net.IP{0x05}, cloned.FailureReasons[0].Endpoint)
	assert.Equal(t, primitive.FailureCodeIndexNotAvailable, cloned.FailureReasons[0].Code)
	assert.Equal(t, net.IP{0x06}, cloned.FailureReasons[1].Endpoint)
	assert.Equal(t, primitive.FailureCodeKeyspaceNotFound, cloned.FailureReasons[1].Code)
	assert.Equal(t, primitive.WriteTypeCdc, cloned.WriteType)
}

func TestFunctionFailure_DeepCopy(t *testing.T) {
	msg := &FunctionFailure{
		ErrorMessage: "msg",
		Keyspace:     "ks1",
		Function:     "f1",
		Arguments:    []string{"arg1", "arg2"},
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Keyspace = "ks2"
	cloned.Function = "f2"
	cloned.Arguments = []string{"arg3"}
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "ks1", msg.Keyspace)
	assert.Equal(t, "f1", msg.Function)
	assert.Equal(t, []string{"arg1", "arg2"}, msg.Arguments)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, "ks2", cloned.Keyspace)
	assert.Equal(t, "f2", cloned.Function)
	assert.Equal(t, []string{"arg3"}, cloned.Arguments)
}

func TestUnprepared_DeepCopy(t *testing.T) {
	msg := &Unprepared{
		ErrorMessage: "msg",
		Id:           []byte{0x01},
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Id = []byte{0x02, 0x03}
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, []byte{0x01}, msg.Id)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, []byte{0x02, 0x03}, cloned.Id)
}

func TestAlreadyExists_DeepCopy(t *testing.T) {
	msg := &AlreadyExists{
		ErrorMessage: "msg",
		Keyspace:     "ks1",
		Table:        "table1",
	}
	cloned := msg.DeepCopy()
	assert.Equal(t, msg, cloned)
	cloned.ErrorMessage = "alt msg"
	cloned.Keyspace = "ks2"
	cloned.Table = "table2"
	assert.NotEqual(t, msg, cloned)
	assert.Equal(t, "msg", msg.ErrorMessage)
	assert.Equal(t, "ks1", msg.Keyspace)
	assert.Equal(t, "table1", msg.Table)
	assert.Equal(t, "alt msg", cloned.ErrorMessage)
	assert.Equal(t, "ks2", cloned.Keyspace)
	assert.Equal(t, "table2", cloned.Table)
}

func TestErrorCodec_Encode(test *testing.T) {
	codec := &errorCodec{}
	// errors encoded the same in all versions
	for _, version := range primitive.SupportedProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"server error",
					&ServerError{"BOOM"},
					[]byte{
						0, 0, 0, 0, // error code
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"protocol error",
					&ProtocolError{"BOOM"},
					[]byte{
						0, 0, 0, 10, // error code
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"authentication error",
					&AuthenticationError{"BOOM"},
					[]byte{
						0, 0, 1, 0, // error code
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"overloaded error",
					&Overloaded{"BOOM"},
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0001,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"is bootstrapping error",
					&IsBootstrapping{"BOOM"},
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0010,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"truncate error",
					&TruncateError{"BOOM"},
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0011,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"syntax error",
					&SyntaxError{"BOOM"},
					[]byte{
						0, 0, 0b_0010_0000, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"unauthorized error",
					&Unauthorized{"BOOM"},
					[]byte{
						0, 0, 0b_0010_0001, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"invalid error",
					&Invalid{"BOOM"},
					[]byte{
						0, 0, 0b_0010_0010, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"config error",
					&ConfigError{"BOOM"},
					[]byte{
						0, 0, 0b_0010_0011, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					nil,
				},
				{
					"unavailable",
					&Unavailable{"BOOM", primitive.ConsistencyLevelLocalQuorum, 3, 2},
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 3,
						0, 0, 0, 2,
					},
					nil,
				},
				{
					"read timeout",
					&ReadTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, true},
					[]byte{
						0, 0, 0b_0001_0010, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 1,
						0, 0, 0, 2,
						1, // data present
					},
					nil,
				},
				{
					"write timeout",
					&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog, 0},
					[]byte{
						0, 0, 0b_0001_0001, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 1,
						0, 0, 0, 2,
						0, 9, B, A, T, C, H, __, L, O, G,
					},
					nil,
				},
				{
					"function failure",
					&FunctionFailure{"BOOM", "ks1", "func1", []string{"int", "varchar"}},
					[]byte{
						0, 0, 0b_0001_0100, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 3, k, s, _1,
						0, 5, f, u, n, c, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					nil,
				},
				{
					"already exists",
					&AlreadyExists{"BOOM", "ks1", "table1"},
					[]byte{
						0, 0, 0b_0010_0100, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					nil,
				},
				{
					"unprepared",
					&Unprepared{"BOOM", []byte{1, 2, 3, 4}},
					[]byte{
						0, 0, 0b_0010_0101, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 4, 1, 2, 3, 4,
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
	// num failures v2, v3, v4
	for _, version := range primitive.SupportedProtocolVersionsLesserThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("read/write failure version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"read failure",
					&ReadFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						1,
						nil,
						false,
					},
					[]byte{
						0, 0, 0b_0001_0011, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1,
						0, // data present
					},
					nil,
				},
				{
					"write failure",
					&WriteFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						1,
						nil,
						primitive.WriteTypeBatchLog,
					},
					[]byte{
						0, 0, 0b_0001_0101, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1,
						0, 9, B, A, T, C, H, __, L, O, G,
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
	// reason map in v5, DSE v1, DSE v2
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion5) {
		test.Run(fmt.Sprintf("read/write failure version %v", version), func(test *testing.T) {
			tests := []encodeTestCase{
				{
					"read failure",
					&ReadFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						0,
						[]*primitive.FailureReason{{net.IPv4(192, 168, 1, 1), primitive.FailureCodeTooManyTombstonesRead}},
						false,
					},
					[]byte{
						0, 0, 0b_0001_0011, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1, // map length
						4, 192, 168, 1, 1, // map key
						0, 1, // map value (reason code)
						0, // data present
					},
					nil,
				},
				{
					"write failure",
					&WriteFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						0,
						[]*primitive.FailureReason{{net.IPv4(192, 168, 1, 1), primitive.FailureCodeTooManyTombstonesRead}},
						primitive.WriteTypeBatchLog,
					},
					[]byte{
						0, 0, 0b_0001_0101, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1, // map length
						4, 192, 168, 1, 1, // map key
						0, 1, // map value (reason code)
						0, 9, B, A, T, C, H, __, L, O, G,
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
	// write timeout with contentions in v5
	test.Run(fmt.Sprintf("write timeout with contentions version %v", primitive.ProtocolVersion5), func(test *testing.T) {
		tests := []encodeTestCase{
			{
				"write timeout CAS with contentions",
				&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeCas, 5},
				[]byte{
					0, 0, 0b_0001_0001, 0b_0000_0000,
					0, 4, B, O, O, M,
					0, 6, // consistency
					0, 0, 0, 1,
					0, 0, 0, 2,
					0, 3, C, A, S,
					0, 5,
				},
				nil,
			},
			{
				"write timeout not CAS",
				&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog, 5},
				[]byte{
					0, 0, 0b_0001_0001, 0b_0000_0000,
					0, 4, B, O, O, M,
					0, 6, // consistency
					0, 0, 0, 1,
					0, 0, 0, 2,
					0, 9, B, A, T, C, H, __, L, O, G,
				},
				nil,
			},
		}
		for _, tt := range tests {
			test.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := codec.Encode(tt.input, dest, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestErrorCodec_EncodedLength(test *testing.T) {
	codec := &errorCodec{}
	// errors encoded the same in all versions
	for _, version := range primitive.SupportedProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"server error",
					&ServerError{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"protocol error",
					&ProtocolError{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"authentication error",
					&AuthenticationError{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"overloaded error",
					&Overloaded{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"is bootstrapping error",
					&IsBootstrapping{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"truncate error",
					&TruncateError{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"syntax error",
					&SyntaxError{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"unauthorized error",
					&Unauthorized{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"invalid error",
					&Invalid{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"config error",
					&ConfigError{"BOOM"},
					primitive.LengthOfInt + primitive.LengthOfString("BOOM"),
					nil,
				},
				{
					"unavailable",
					&Unavailable{"BOOM", primitive.ConsistencyLevelLocalQuorum, 3, 2},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // required
						primitive.LengthOfInt, // alive
					nil,
				},
				{
					"read timeout",
					&ReadTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, true},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfByte, // data present
					nil,
				},
				{
					"write timeout",
					&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog, 0},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfString(string(primitive.WriteTypeBatchLog)), // write type
					nil,
				},
				{
					"function failure",
					&FunctionFailure{"BOOM", "ks1", "func1", []string{"int", "varchar"}},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfString("ks1") + // keyspace
						primitive.LengthOfString("func1") + // function
						primitive.LengthOfStringList([]string{"int", "varchar"}), // arguments
					nil,
				},
				{
					"already exists",
					&AlreadyExists{"BOOM", "ks1", "table1"},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfString("ks1") + // keyspace
						primitive.LengthOfString("table1"), // table
					nil,
				},
				{
					"unprepared",
					&Unprepared{"BOOM", []byte{1, 2, 3, 4}},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShortBytes([]byte{1, 2, 3, 4}),
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
	// num failures in v2, v3, v4
	for _, version := range primitive.SupportedProtocolVersionsLesserThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("read/write failure version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"read failure",
					&ReadFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						1,
						nil,
						false,
					},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfInt + // num failures
						primitive.LengthOfByte, // data present
					nil,
				},
				{
					"write failure",
					&WriteFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						1,
						nil,
						primitive.WriteTypeBatchLog,
					},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfInt + // num failures
						primitive.LengthOfString(string(primitive.WriteTypeBatchLog)), // write type
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
	// reason map in v5, DSE v1, DSE v2
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion5) {
		test.Run(fmt.Sprintf("read/write failure version %v", version), func(test *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"read failure",
					&ReadFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						0,
						[]*primitive.FailureReason{{net.IPv4(192, 168, 1, 1), primitive.FailureCodeTooManyTombstonesRead}},
						false,
					},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfInt + // map length
						primitive.LengthOfByte + net.IPv4len + // map key length
						primitive.LengthOfShort + // map value length (reason code)
						primitive.LengthOfByte, // data present
					nil,
				},
				{
					"write failure",
					&WriteFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						0,
						[]*primitive.FailureReason{{net.IPv4(192, 168, 1, 1), primitive.FailureCodeTooManyTombstonesRead}},
						primitive.WriteTypeBatchLog,
					},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfInt + // map length
						primitive.LengthOfByte + net.IPv4len + // map key length
						primitive.LengthOfShort + // map value length (reason code)
						primitive.LengthOfString(string(primitive.WriteTypeBatchLog)), // write type
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
	// write timeout with contentions in v5
	test.Run(fmt.Sprintf("write timeout with contentions version %v", primitive.ProtocolVersion5), func(test *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"write timeout CAS with contentions",
				&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeCas, 5},
				primitive.LengthOfInt +
					primitive.LengthOfString("BOOM") +
					primitive.LengthOfShort + // consistency
					primitive.LengthOfInt + // received
					primitive.LengthOfInt + // block for
					primitive.LengthOfString(string(primitive.WriteTypeCas)) + // write type
					primitive.LengthOfShort, // contentions
				nil,
			},
		}
		for _, tt := range tests {
			test.Run(tt.name, func(t *testing.T) {
				actual, err := codec.EncodedLength(tt.input, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestErrorCodec_Decode(test *testing.T) {
	codec := &errorCodec{}
	// errors encoded the same in all versions
	for _, version := range primitive.SupportedProtocolVersions() {
		test.Run(version.String(), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"server error",
					[]byte{
						0, 0, 0, 0, // error code
						0, 4, B, O, O, M,
					},
					&ServerError{"BOOM"},
					nil,
				},
				{
					"protocol error",
					[]byte{
						0, 0, 0, 10, // error code
						0, 4, B, O, O, M,
					},
					&ProtocolError{"BOOM"},
					nil,
				},
				{
					"authentication error",
					[]byte{
						0, 0, 1, 0, // error code
						0, 4, B, O, O, M,
					},
					&AuthenticationError{"BOOM"},
					nil,
				},
				{
					"overloaded error",
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0001,
						0, 4, B, O, O, M,
					},
					&Overloaded{"BOOM"},
					nil,
				},
				{
					"is bootstrapping error",
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0010,
						0, 4, B, O, O, M,
					},
					&IsBootstrapping{"BOOM"},
					nil,
				},
				{
					"truncate error",
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0011,
						0, 4, B, O, O, M,
					},
					&TruncateError{"BOOM"},
					nil,
				},
				{
					"syntax error",
					[]byte{
						0, 0, 0b_0010_0000, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					&SyntaxError{"BOOM"},
					nil,
				},
				{
					"unauthorized error",
					[]byte{
						0, 0, 0b_0010_0001, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					&Unauthorized{"BOOM"},
					nil,
				},
				{
					"invalid error",
					[]byte{
						0, 0, 0b_0010_0010, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					&Invalid{"BOOM"},
					nil,
				},
				{
					"config error",
					[]byte{
						0, 0, 0b_0010_0011, 0b_0000_0000,
						0, 4, B, O, O, M,
					},
					&ConfigError{"BOOM"},
					nil,
				},
				{
					"unavailable",
					[]byte{
						0, 0, 0b_0001_0000, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 3,
						0, 0, 0, 2,
					},
					&Unavailable{"BOOM", primitive.ConsistencyLevelLocalQuorum, 3, 2},
					nil,
				},
				{
					"read timeout",
					[]byte{
						0, 0, 0b_0001_0010, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 1,
						0, 0, 0, 2,
						1, // data present
					},
					&ReadTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, true},
					nil,
				},
				{
					"write timeout",
					[]byte{
						0, 0, 0b_0001_0001, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 1,
						0, 0, 0, 2,
						0, 9, B, A, T, C, H, __, L, O, G,
					},
					&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog, 0},
					nil,
				},
				{
					"function failure",
					[]byte{
						0, 0, 0b_0001_0100, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 3, k, s, _1,
						0, 5, f, u, n, c, _1,
						0, 2,
						0, 3, i, n, t,
						0, 7, v, a, r, c, h, a, r,
					},
					&FunctionFailure{"BOOM", "ks1", "func1", []string{"int", "varchar"}},
					nil,
				},
				{
					"already exists",
					[]byte{
						0, 0, 0b_0010_0100, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 3, k, s, _1,
						0, 6, t, a, b, l, e, _1,
					},
					&AlreadyExists{"BOOM", "ks1", "table1"},
					nil,
				},
				{
					"unprepared",
					[]byte{
						0, 0, 0b_0010_0101, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 4, 1, 2, 3, 4,
					},
					&Unprepared{"BOOM", []byte{1, 2, 3, 4}},
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
	// num failures in v3, v4
	for _, version := range primitive.SupportedProtocolVersionsLesserThanOrEqualTo(primitive.ProtocolVersion4) {
		test.Run(fmt.Sprintf("read/write failure version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"read failure",
					[]byte{
						0, 0, 0b_0001_0011, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1,
						0, // data present
					},
					&ReadFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						1,
						nil,
						false,
					},
					nil,
				},
				{
					"write failure",
					[]byte{
						0, 0, 0b_0001_0101, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1,
						0, 9, B, A, T, C, H, __, L, O, G,
					},
					&WriteFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						1,
						nil,
						primitive.WriteTypeBatchLog,
					},
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
	// reason map in v5, DSE v1, DSE v2
	for _, version := range primitive.SupportedProtocolVersionsGreaterThanOrEqualTo(primitive.ProtocolVersion5) {
		test.Run(fmt.Sprintf("read/write failure version %v", version), func(test *testing.T) {
			tests := []decodeTestCase{
				{
					"read failure",
					[]byte{
						0, 0, 0b_0001_0011, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1, // map length
						4, 192, 168, 1, 1, // map key
						0, 1, // map value (reason code)
						0, // data present
					},
					&ReadFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						0,
						[]*primitive.FailureReason{{net.IPv4(192, 168, 1, 1), primitive.FailureCodeTooManyTombstonesRead}},
						false,
					},
					nil,
				},
				{
					"write failure",
					[]byte{
						0, 0, 0b_0001_0101, 0b_0000_0000,
						0, 4, B, O, O, M,
						0, 6, // consistency
						0, 0, 0, 0,
						0, 0, 0, 2,
						0, 0, 0, 1, // map length
						4, 192, 168, 1, 1, // map key
						0, 1, // map value (reason code)
						0, 9, B, A, T, C, H, __, L, O, G,
					},
					&WriteFailure{
						"BOOM",
						primitive.ConsistencyLevelLocalQuorum,
						0,
						2,
						0,
						[]*primitive.FailureReason{{net.IPv4(192, 168, 1, 1), primitive.FailureCodeTooManyTombstonesRead}},
						primitive.WriteTypeBatchLog,
					},
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
	// write timeout with contentions in v5
	test.Run(fmt.Sprintf("write timeout wiht contentions version %v", primitive.ProtocolVersion5), func(test *testing.T) {
		tests := []decodeTestCase{
			{
				"write timeout CAS with contentions",
				[]byte{
					0, 0, 0b_0001_0001, 0b_0000_0000,
					0, 4, B, O, O, M,
					0, 6, // consistency
					0, 0, 0, 1,
					0, 0, 0, 2,
					0, 3, C, A, S,
					0, 5,
				},
				&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeCas, 5},
				nil,
			},
			{
				"write timeout not CAS",
				[]byte{
					0, 0, 0b_0001_0001, 0b_0000_0000,
					0, 4, B, O, O, M,
					0, 6, // consistency
					0, 0, 0, 1,
					0, 0, 0, 2,
					0, 9, B, A, T, C, H, __, L, O, G,
				},
				&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog, 0},
				nil,
			},
			{
				"write timeout CAS missing contentions",
				[]byte{
					0, 0, 0b_0001_0001, 0b_0000_0000,
					0, 4, B, O, O, M,
					0, 6, // consistency
					0, 0, 0, 1,
					0, 0, 0, 2,
					0, 3, C, A, S,
				},
				nil,
				fmt.Errorf("cannot read ERROR WRITE TIMEOUT contentions: %w", fmt.Errorf("cannot read [short]: %w", errors.New("EOF"))),
			},
		}
		for _, tt := range tests {
			test.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := codec.Decode(source, primitive.ProtocolVersion5)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}
