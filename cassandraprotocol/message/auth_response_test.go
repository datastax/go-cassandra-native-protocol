package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthResponseCodec_Encode(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &AuthResponseCodec{}
	for _, version := range primitives.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodeTestCase{
				{
					"simple auth response",
					&AuthResponse{token},
					[]byte{0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe},
					nil,
				},
				{
					"not an auth response",
					&AuthChallenge{token},
					nil,
					errors.New("expected *message.AuthResponse, got *message.AuthChallenge"),
				},
				{
					"auth response nil token",
					&AuthResponse{nil},
					nil,
					errors.New("AUTH_RESPONSE token cannot be nil"),
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

func TestAuthResponseCodec_EncodedLength(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &AuthResponseCodec{}
	for _, version := range primitives.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []encodedLengthTestCase{
				{
					"simple auth response",
					&AuthResponse{token},
					primitives.LengthOfBytes(token),
					nil,
				},
				{
					"not an auth response",
					&AuthChallenge{token},
					-1,
					errors.New("expected *message.AuthResponse, got *message.AuthChallenge"),
				},
				{
					"auth response nil token",
					&AuthResponse{nil},
					primitives.LengthOfBytes(nil),
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

func TestAuthResponseCodec_Decode(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &AuthResponseCodec{}
	for _, version := range primitives.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []decodeTestCase{
				{
					"simple auth response",
					[]byte{0, 0, 0, 4, 0xca, 0xfe, 0xba, 0xbe},
					&AuthResponse{token},
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
