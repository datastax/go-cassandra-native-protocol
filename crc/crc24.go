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

// Copied and adapted from the server-side version:
// https://github.com/apache/cassandra/blob/cassandra-4.0/src/java/org/apache/cassandra/net/Crc.java

const crc24Init uint32 = 0x875060
const crc24Poly uint32 = 0x1974F0B

// ChecksumKoopman returns the CRC-24 checksum of the given data.
// The parameter bytes is an up to 8-byte register containing bytes to compute the CRC over; bits will be read
// least-significant to most significant.
// The parameter len is the number of bytes, greater than 0 and fewer than 9, to be read from bytes.
// The polynomial is chosen from https://users.ece.cmu.edu/~koopman/crc/index.html, by Philip Koopman,
// and is licensed under the Creative Commons Attribution 4.0 International License
// (https://creativecommons.org/licenses/by/4.0).
// Koopman's own notation to represent the polynomial has been changed.
// This polynomial provides hamming distance of 8 for messages up to length 105 bits;
// we only support 8-64 bits at present, with an expected range of 40-48.
func ChecksumKoopman(data uint64, len int) uint32 {
	crc := crc24Init
	for i := 0; i < len; i++ {
		crc ^= (uint32)(data) << 16
		data >>= 8
		for j := 0; j < 8; j++ {
			crc <<= 1
			if (crc & 0x1000000) != 0 {
				crc ^= crc24Poly
			}
		}
	}
	return crc
}
