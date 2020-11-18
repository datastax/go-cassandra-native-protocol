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

package primitive

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestReadStreamId(t *testing.T) {
	for _, version := range AllProtocolVersionsLesserThanOrEqualTo(ProtocolVersion2) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				expected int16
				err      error
			}{
				{
					"zero stream id",
					[]byte{0},
					int16(0),
					nil,
				},
				{
					"positive stream id",
					[]byte{0x7f},
					int16(127),
					nil,
				},
				{
					"negative stream id",
					[]byte{0x80},
					int16(-128),
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := bytes.NewBuffer(tt.source)
					actual, err := ReadStreamId(buf, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range AllProtocolVersionsGreaterThanOrEqualTo(ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				source   []byte
				expected int16
				err      error
			}{
				{
					"zero stream id",
					[]byte{0, 0},
					int16(0),
					nil,
				},
				{
					"positive stream id",
					[]byte{0x7f, 0xff}, // MaxInt16 = 32767
					math.MaxInt16,
					nil,
				},
				{
					"negative stream id",
					[]byte{0x80, 0x00}, // MinInt16 = -32768
					math.MinInt16,
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := bytes.NewBuffer(tt.source)
					actual, err := ReadStreamId(buf, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestWriteStreamId(t *testing.T) {
	for _, version := range AllProtocolVersionsLesserThanOrEqualTo(ProtocolVersion2) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    int16
				expected []byte
				err      error
			}{
				{
					"zero stream id",
					int16(0),
					[]byte{0},
					nil,
				},
				{
					"positive stream id",
					int16(127),
					[]byte{0x7f},
					nil,
				},
				{
					"negative stream id",
					int16(-128),
					[]byte{0x80},
					nil,
				},
				{
					"stream id out of range 1",
					int16(128),
					nil,
					fmt.Errorf("stream id out of range for %v: 128", version),
				},
				{
					"stream id out of range 2",
					int16(-129),
					nil,
					fmt.Errorf("stream id out of range for %v: -129", version),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := &bytes.Buffer{}
					err := WriteStreamId(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
	for _, version := range AllProtocolVersionsGreaterThanOrEqualTo(ProtocolVersion3) {
		t.Run(version.String(), func(t *testing.T) {
			tests := []struct {
				name     string
				input    int16
				expected []byte
				err      error
			}{
				{
					"zero stream id",
					int16(0),
					[]byte{0, 0},
					nil,
				},
				{
					"positive stream id",
					math.MaxInt16,
					[]byte{0x7f, 0xff}, // MaxInt16 = 32767
					nil,
				},
				{
					"negative stream id",
					math.MinInt16,
					[]byte{0x80, 0x00}, // MinInt16 = -32768
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					buf := &bytes.Buffer{}
					err := WriteStreamId(tt.input, buf, version)
					assert.Equal(t, tt.expected, buf.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}
