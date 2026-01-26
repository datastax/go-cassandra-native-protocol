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

package compression

type Compressor interface {
	// Frame compression for protocol v4, v3, v2. Length of uncompressed data is
	// encoded within output slice.
	CompressFrame(uncompressed []byte) ([]byte, error)
	DecompressFrame(compressed []byte) ([]byte, error)

	// Segment compression for protocol v5+. Passed buffers should have the
	// correct size. Length of uncompressed payload is passed in segment header,
	// it is known upfront, and does not need to be encoded in the payload.
	CompressSegment(uncompressed []byte) ([]byte, error)
	DecompressSegment(compressed []byte, uncompressed []byte) error
}
