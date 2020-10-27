package message

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContinuousPagingOptions_Encode(t *testing.T) {
	version := cassandraprotocol.ProtocolVersionDse1
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected []byte
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
				},
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
				},
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := EncodeContinuousPagingOptions(tt.input, dest, version)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	version = cassandraprotocol.ProtocolVersionDse2
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected []byte
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
					0, 0, 0, 0, // next pages
				},
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
					NextPages:      100,
				},
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
					0, 0, 0, 100, // next pages
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := EncodeContinuousPagingOptions(tt.input, dest, version)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestContinuousPagingOptions_EncodedLength(t *testing.T) {
	version := cassandraprotocol.ProtocolVersionDse1
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected int
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				primitives.LengthOfInt * 2,
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
				},
				primitives.LengthOfInt * 2,
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := LengthOfContinuousPagingOptions(tt.input, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	version = cassandraprotocol.ProtocolVersionDse2
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected int
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				primitives.LengthOfInt * 3,
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
					NextPages:      100,
				},
				primitives.LengthOfInt * 3,
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := LengthOfContinuousPagingOptions(tt.input, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestContinuousPagingOptions_Decode(t *testing.T) {
	version := cassandraprotocol.ProtocolVersionDse1
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected *ContinuousPagingOptions
			err      error
		}{
			{
				"empty options",
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
				},
				&ContinuousPagingOptions{},
				nil,
			},
			{
				"simple options",
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
				},
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := DecodeContinuousPagingOptions(source, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	version = cassandraprotocol.ProtocolVersionDse2
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected *ContinuousPagingOptions
			err      error
		}{
			{
				"empty options",
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
					0, 0, 0, 0, // next pages
				},
				&ContinuousPagingOptions{},
				nil,
			},
			{
				"simple options",
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
					0, 0, 0, 100, // next pages
				},
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
					NextPages:      100,
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := DecodeContinuousPagingOptions(source, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}
