package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"testing"
)

func TestAuthSuccessCodec_Encode(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &AuthSuccessCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected []byte
				err      error
			}{
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
					"auth success nil token",
					&AuthSuccess{nil},
					nil,
					errors.New("AUTH_SUCCESS token cannot be nil"),
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
	codec := &AuthSuccessCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected int
				err      error
			}{
				{
					"simple auth success",
					&AuthSuccess{token},
					primitives.LengthOfBytes(token),
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

func TestAuthSuccessCodec_Decode(t *testing.T) {
	token := []byte{0xca, 0xfe, 0xba, 0xbe}
	codec := &AuthSuccessCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected Message
				err      error
			}{
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
