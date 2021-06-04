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
)

// [string list]

func ReadStringList(source io.Reader) (decoded []string, err error) {
	var length uint16
	length, err = ReadShort(source)
	if err != nil {
		return nil, fmt.Errorf("cannot read [string list] length: %w", err)
	}

	if length < 0 {
		return nil, nil
	} else if length == 0 {
		return []string{}, nil
	}

	decoded = make([]string, length)
	for i := uint16(0); i < length; i++ {
		var str string
		str, err = ReadString(source)
		if err != nil {
			return nil, fmt.Errorf("cannot read [string list] element %d: %w", i, err)
		}
		decoded[i] = str
	}
	return decoded, nil
}

func WriteStringList(list []string, dest io.Writer) error {
	length := len(list)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write [string list] length: %w", err)
	}
	for i, s := range list {
		if err := WriteString(s, dest); err != nil {
			return fmt.Errorf("cannot write [string list] element %d: %w", i, err)
		}
	}
	return nil
}

func LengthOfStringList(list []string) int {
	length := LengthOfShort
	for _, s := range list {
		length += LengthOfString(s)
	}
	return length
}
