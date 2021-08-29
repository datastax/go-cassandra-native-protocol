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
	"fmt"
	"testing"
)

func TestChecksumKoopman(t *testing.T) {
	tests := []struct {
		data uint64
		len  int
		want uint32
	}{
		// zero data
		{0, 0, 8867936},
		{0, 1, 59277},
		{0, 3, 8251255},
		{0, 5, 11185162},
		{0, 8, 9640737},
		// data = Long.MaxValue
		{9223372036854775807, 0, 8867936},
		{9223372036854775807, 1, 1294145},
		{9223372036854775807, 3, 8029951},
		{9223372036854775807, 5, 9326200},
		{9223372036854775807, 8, 5032370},
		// random data
		{131077, 3, 10131737},
		{17181442053, 5, 3672222},
		{34359607301, 5, 14445742},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("data %v len %v", tt.data, tt.len), func(t *testing.T) {
			if got := ChecksumKoopman(tt.data, tt.len); got != tt.want {
				t.Errorf("ChecksumKoopman() = %v, want %v", got, tt.want)
			}
		})
	}
}
