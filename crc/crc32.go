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

import "hash/crc32"

var table = crc32.MakeTable(crc32.IEEE)
var initialBytes = []byte{0xFA, 0x2D, 0x55, 0xCA}
var initialChecksum = crc32.Update(0, table, initialBytes)

// ChecksumIEEE returns the CRC-32 checksum of the given data. The algorithm is the one expected by Cassandra:
// the actual checksum is computed over the combination of 4 fixed initial bytes and the given byte slice.
func ChecksumIEEE(data []byte) uint32 {
	return crc32.Update(initialChecksum, table, data)
}
