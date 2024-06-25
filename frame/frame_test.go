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

package frame

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestFrame_DeepCopy(t *testing.T) {
	f := NewFrame(primitive.ProtocolVersion4, 1, &message.Ready{})
	f.SetTracingId(&primitive.UUID{0x01, 0x02})

	cloned := f.DeepCopy()
	assert.Equal(t, f, cloned)

	cloned.SetTracingId(&primitive.UUID{0x02, 0x03})
	assert.Equal(t, primitive.ProtocolVersion4, f.Header.Version)
	assert.Equal(t, &primitive.UUID{0x01, 0x02}, f.Body.TracingId)

	assert.NotEqual(t, f, cloned)
	assert.Equal(t, primitive.ProtocolVersion4, cloned.Header.Version)
	assert.Equal(t, &primitive.UUID{0x02, 0x03}, cloned.Body.TracingId)
}

func TestRawFrame_DeepCopy(t *testing.T) {
	f := &RawFrame{
		Header: &Header{
			IsResponse: true,
			Version:    primitive.ProtocolVersion4,
			Flags:      0,
			StreamId:   1,
			OpCode:     primitive.OpCodeError,
			BodyLength: 1,
		},
		Body: []byte{0x01},
	}

	cloned := f.DeepCopy()
	assert.Equal(t, f, cloned)

	cloned.Body = []byte{0x03, 0x04}
	assert.Equal(t, primitive.ProtocolVersion4, f.Header.Version)
	assert.Equal(t, []byte{0x01}, f.Body)

	assert.NotEqual(t, f, cloned)
	assert.Equal(t, primitive.ProtocolVersion4, cloned.Header.Version)
	assert.Equal(t, []byte{0x03, 0x04}, cloned.Body)
}

func TestHeader_DeepCopy(t *testing.T) {
	h := &Header{
		IsResponse: true,
		Version:    primitive.ProtocolVersion4,
		Flags:      0,
		StreamId:   1,
		OpCode:     primitive.OpCodeError,
		BodyLength: 1,
	}

	cloned := h.DeepCopy()
	assert.Equal(t, h, cloned)

	cloned.IsResponse = false
	cloned.Version = primitive.ProtocolVersion3
	cloned.Flags = primitive.HeaderFlagTracing
	cloned.StreamId = 2
	cloned.OpCode = primitive.OpCodeStartup
	cloned.BodyLength = 2

	assert.NotEqual(t, h, cloned)
	assert.Equal(t, true, h.IsResponse)
	assert.Equal(t, primitive.ProtocolVersion4, h.Version)
	assert.EqualValues(t, 0, h.Flags)
	assert.EqualValues(t, 1, h.StreamId)
	assert.Equal(t, primitive.OpCodeError, h.OpCode)
	assert.EqualValues(t, 1, h.BodyLength)

	assert.Equal(t, false, cloned.IsResponse)
	assert.Equal(t, primitive.ProtocolVersion3, cloned.Version)
	assert.EqualValues(t, primitive.HeaderFlagTracing, cloned.Flags)
	assert.EqualValues(t, 2, cloned.StreamId)
	assert.Equal(t, primitive.OpCodeStartup, cloned.OpCode)
	assert.EqualValues(t, 2, cloned.BodyLength)
}

func TestBody_DeepCopy(t *testing.T) {
	b := &Body{
		TracingId: &primitive.UUID{0x01},
		CustomPayload: map[string][]byte{
			"opt1": {0x05},
		},
		Warnings: []string{"warn"},
		Message: &message.Query{
			Query: "q1",
		},
	}

	cloned := b.DeepCopy()
	assert.Equal(t, b, cloned)

	cloned.TracingId = &primitive.UUID{0x03}
	cloned.CustomPayload["opt1"] = []byte{0x01}
	cloned.CustomPayload["opt2"] = []byte{0x06}
	cloned.Warnings = []string{"w1", "w2"}
	cloned.Message.(*message.Query).Query = "q2"

	assert.NotEqual(t, b, cloned)

	assert.Equal(t, &primitive.UUID{0x01}, b.TracingId)
	assert.Equal(t, []byte{0x05}, b.CustomPayload["opt1"])
	assert.Equal(t, 1, len(b.CustomPayload))
	assert.Equal(t, "warn", b.Warnings[0])
	assert.Equal(t, 1, len(b.Warnings))
	assert.Equal(t, "q1", b.Message.(*message.Query).Query)

	assert.Equal(t, &primitive.UUID{0x03}, cloned.TracingId)
	assert.Equal(t, []byte{0x01}, cloned.CustomPayload["opt1"])
	assert.Equal(t, []byte{0x06}, cloned.CustomPayload["opt2"])
	assert.Equal(t, 2, len(cloned.CustomPayload))
	assert.Equal(t, "w1", cloned.Warnings[0])
	assert.Equal(t, "w2", cloned.Warnings[1])
	assert.Equal(t, 2, len(cloned.Warnings))
	assert.Equal(t, "q2", cloned.Message.(*message.Query).Query)
}

func TestBody_DeepCopy_WithNils(t *testing.T) {
	b := &Body{
		TracingId:     nil,
		CustomPayload: nil,
		Warnings:      nil,
		Message: &message.Query{
			Query: "q1",
		},
	}

	cloned := b.DeepCopy()
	assert.Equal(t, b, cloned)

	cloned.TracingId = &primitive.UUID{0x03}
	cloned.CustomPayload = map[string][]byte{}
	cloned.CustomPayload["opt1"] = []byte{0x01}
	cloned.CustomPayload["opt2"] = []byte{0x06}
	cloned.Warnings = []string{"w1", "w2"}
	cloned.Message.(*message.Query).Query = "q2"

	assert.NotEqual(t, b, cloned)

	assert.Nil(t, b.TracingId)
	assert.Nil(t, b.CustomPayload)
	assert.Nil(t, b.Warnings)
	assert.Equal(t, "q1", b.Message.(*message.Query).Query)

	assert.Equal(t, &primitive.UUID{0x03}, cloned.TracingId)
	assert.Equal(t, []byte{0x01}, cloned.CustomPayload["opt1"])
	assert.Equal(t, []byte{0x06}, cloned.CustomPayload["opt2"])
	assert.Equal(t, 2, len(cloned.CustomPayload))
	assert.Equal(t, "w1", cloned.Warnings[0])
	assert.Equal(t, "w2", cloned.Warnings[1])
	assert.Equal(t, 2, len(cloned.Warnings))
	assert.Equal(t, "q2", cloned.Message.(*message.Query).Query)
}
