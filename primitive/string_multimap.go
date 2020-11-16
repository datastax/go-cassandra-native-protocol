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

// [string multimap]

func ReadStringMultiMap(source io.Reader) (decoded map[string][]string, err error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read [string multimap] length: %w", err)
	} else {
		decoded := make(map[string][]string, length)
		for i := uint16(0); i < length; i++ {
			if key, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read [string multimap] entry %d key: %w", i, err)
			} else if value, err := ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read [string multimap] entry %d value: %w", i, err)
			} else {
				decoded[key] = value
			}
		}
		return decoded, nil
	}
}

func WriteStringMultiMap(m map[string][]string, dest io.Writer) error {
	if err := WriteShort(uint16(len(m)), dest); err != nil {
		return fmt.Errorf("cannot write [string multimap] length: %w", err)
	}
	for key, value := range m {
		if err := WriteString(key, dest); err != nil {
			return fmt.Errorf("cannot write [string multimap] entry '%v' key: %w", key, err)
		}
		if err := WriteStringList(value, dest); err != nil {
			return fmt.Errorf("cannot write [string multimap] entry '%v' value: %w", key, err)
		}
	}
	return nil
}

func LengthOfStringMultiMap(m map[string][]string) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfStringList(value)
	}
	return length
}
