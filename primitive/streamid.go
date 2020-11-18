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
	"fmt"
	"io"
	"math"
)

// ReadStreamId reads a stream id from the given source, using the given version to determine if the stream id
// is a 16-bit integer (versions 3+) or an 8-bit integer (versions 1 and 2).
func ReadStreamId(source io.Reader, version ProtocolVersion) (int16, error) {
	if version >= ProtocolVersion3 {
		id, err := ReadShort(source)
		return int16(id), err
	} else {
		id, err := ReadByte(source)
		return int16(int8(id)), err
	}
}

// WriteStreamId writes the given stream id to the given destination, using the given version to determine if the
// stream id is a 16-bit integer (versions 3+) or an 8-bit integer (versions 1 and 2).
func WriteStreamId(streamId int16, dest io.Writer, version ProtocolVersion) error {
	if version >= ProtocolVersion3 {
		return WriteShort(uint16(streamId), dest)
	} else if streamId > math.MaxInt8 || streamId < math.MinInt8 {
		return fmt.Errorf("stream id out of range for %v: %v", version, streamId)
	} else {
		return WriteByte(uint8(streamId), dest)
	}
}
