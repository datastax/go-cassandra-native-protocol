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

// [uuid]

const LengthOfUuid = 16

type UUID [16]byte

func (u *UUID) DeepCopy() *UUID {
	if u == nil {
		return nil
	}
	newUuid := *u
	return &newUuid
}

func (u *UUID) String() string {
	if u == nil {
		return ""
	}

	var offsets = [...]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}
	const hexString = "0123456789abcdef"
	r := make([]byte, 36)
	for i, b := range u {
		r[offsets[i]] = hexString[b>>4]
		r[offsets[i]+1] = hexString[b&0xF]
	}
	r[8] = '-'
	r[13] = '-'
	r[18] = '-'
	r[23] = '-'
	return string(r)
}

// Bytes returns the raw byte slice for this UUID. A UUID is always 128 bits
// (16 bytes) long.
func (u *UUID) Bytes() []byte {
	return u[:]
}

// ParseUuid parses a 32 digit hexadecimal number (that might contain hyphens)
// representing an UUID.
func ParseUuid(input string) (*UUID, error) {
	var u UUID
	i := 0
	for _, r := range input {
		switch {
		case r == '-' && i&1 == 0:
			continue
		case r >= '0' && r <= '9' && i < 32:
			u[i/2] |= byte(r-'0') << uint(4-i&1*4)
		case r >= 'a' && r <= 'f' && i < 32:
			u[i/2] |= byte(r-'a'+10) << uint(4-i&1*4)
		case r >= 'A' && r <= 'F' && i < 32:
			u[i/2] |= byte(r-'A'+10) << uint(4-i&1*4)
		default:
			return nil, fmt.Errorf("invalid UUID: %q", input)
		}
		i += 1
	}
	if i != 32 {
		return nil, fmt.Errorf("invalid UUID: %q", input)
	}
	return &u, nil
}

func ReadUuid(source io.Reader) (*UUID, error) {
	decoded := new(UUID)
	if _, err := io.ReadFull(source, decoded[:]); err != nil {
		return nil, fmt.Errorf("cannot read [uuid] content: %w", err)
	}
	return decoded, nil
}

func WriteUuid(uuid *UUID, dest io.Writer) error {
	if uuid == nil {
		return errors.New("cannot write nil [uuid]")
	} else if n, err := dest.Write(uuid[:]); err != nil {
		return fmt.Errorf("cannot write [uuid] content: %w", err)
	} else if n < LengthOfUuid {
		return errors.New("not enough capacity to write [uuid] content")
	}
	return nil
}
