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
	"errors"
	"fmt"
	"io"
)

// [bytes]

func ReadBytes(source io.Reader) ([]byte, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [bytes] length: %w", err)
	} else if length < 0 {
		return nil, nil
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return nil, fmt.Errorf("cannot read [bytes] content: %w", err)
		} else if read != int(length) {
			return nil, errors.New("not enough bytes to read [bytes] content")
		}
		return decoded, nil
	}
}

func WriteBytes(b []byte, dest io.Writer) error {
	if b == nil {
		if err := WriteInt(-1, dest); err != nil {
			return fmt.Errorf("cannot write null [bytes]: %w", err)
		}
	} else {
		length := len(b)
		if err := WriteInt(int32(length), dest); err != nil {
			return fmt.Errorf("cannot write [bytes] length: %w", err)
		} else if n, err := dest.Write(b); err != nil {
			return fmt.Errorf("cannot write [bytes] content: %w", err)
		} else if n < length {
			return errors.New("not enough capacity to write [bytes] content")
		}
	}
	return nil
}

func LengthOfBytes(b []byte) int {
	return LengthOfInt + len(b)
}
