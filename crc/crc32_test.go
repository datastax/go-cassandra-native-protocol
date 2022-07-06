// Copyright 2021 DataStax
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

package crc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksumIEEE(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint32
	}{
		{
			"zero",
			[]byte{},
			initialChecksum,
		},
		// test cases below are snapshots taken from actual payloads processed by the DataStax Java driver
		{
			"QUERY (SELECT cluster_name FROM system.local)",
			[]byte{
				6, 16, 0, 0, 7, 0, 0, 0, 47, 0, 0, 0, 37, 83, 69, 76, 69, 67, 84, 32, 99, 108, 117, 115, 116, 101, 114,
				95, 110, 97, 109, 101, 32, 70, 82, 79, 77, 32, 115, 121, 115, 116, 101, 109, 46, 108, 111, 99, 97, 108,
				0, 1, 0, 0, 0, 0},
			37932456,
		},
		{
			"QUERY (SELECT * FROM system.local) + QUERY (SELECT * FROM system.peers_v2)",
			[]byte{
				6, 16, 0, 0, 7, 0, 0, 0, 36, 0, 0, 0, 26, 83, 69, 76, 69, 67, 84, 32, 42, 32, 70, 82, 79, 77, 32, 115,
				121, 115, 116, 101, 109, 46, 108, 111, 99, 97, 108, 0, 1, 0, 0, 0, 0, 6, 16, 0, 1, 7, 0, 0, 0, 39, 0,
				0, 0, 29, 83, 69, 76, 69, 67, 84, 32, 42, 32, 70, 82, 79, 77, 32, 115, 121, 115, 116, 101, 109, 46,
				112, 101, 101, 114, 115, 95, 118, 50, 0, 1, 0, 0, 0, 0},
			// Java driver CRC32 for this payload is the (signed) int -642004664;
			// Go's uint32 equivalent is -642004664 & 0xffffffff = 3652962632.
			uint32(int64(-642004664) & int64(0xffffffff)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ChecksumIEEE(tt.data)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
