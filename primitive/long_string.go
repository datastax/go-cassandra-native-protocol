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

// [long string]

func ReadLongString(source io.Reader) (string, error) {
	if length, err := ReadInt(source); err != nil {
		return "", fmt.Errorf("cannot read [long string] length: %w", err)
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return "", fmt.Errorf("cannot read [long string] content: %w", err)
		} else if read != int(length) {
			return "", errors.New("not enough bytes to read [long string] content")
		}
		return string(decoded), nil
	}
}

func WriteLongString(s string, dest io.Writer) error {
	length := len(s)
	if err := WriteInt(int32(length), dest); err != nil {
		return fmt.Errorf("cannot write [long string] length: %w", err)
	} else if n, err := dest.Write([]byte(s)); err != nil {
		return fmt.Errorf("cannot write [long string] length: %w", err)
	} else if n < length {
		return errors.New("not enough capacity to write [long string] content")
	}
	return nil
}

func LengthOfLongString(s string) int {
	return LengthOfInt + len(s)
}
