package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestErrorCodec_Encode(test *testing.T) {
	codec := &errorCodec{}
	// errors encoded the same in all versions
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
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
					&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog},
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
	// num failures v3, v4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4} {
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
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2} {
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
}

func TestErrorCodec_EncodedLength(test *testing.T) {
	codec := &errorCodec{}
	// errors encoded the same in all versions
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
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
					&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog},
					primitive.LengthOfInt +
						primitive.LengthOfString("BOOM") +
						primitive.LengthOfShort + // consistency
						primitive.LengthOfInt + // received
						primitive.LengthOfInt + // block for
						primitive.LengthOfString(primitive.WriteTypeBatchLog), // write type
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
	// num failures in v3, v4
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4} {
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
						primitive.LengthOfString(primitive.WriteTypeBatchLog), // write type
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
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2} {
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
						primitive.LengthOfString(primitive.WriteTypeBatchLog), // write type
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

func TestErrorCodec_Decode(test *testing.T) {
	codec := &errorCodec{}
	// errors encoded the same in all versions
	for _, version := range primitive.AllProtocolVersions() {
		test.Run(fmt.Sprintf("version %v", version), func(test *testing.T) {
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
					&WriteTimeout{"BOOM", primitive.ConsistencyLevelLocalQuorum, 1, 2, primitive.WriteTypeBatchLog},
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
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4} {
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
	for _, version := range []primitive.ProtocolVersion{primitive.ProtocolVersion5, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2} {
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
}
