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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// [uuid]

const LengthOfUuid = 16

type UUID [16]byte

func (u *UUID) Clone() *UUID {
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

	return hex.EncodeToString(u[:])
}

func ReadUuid(source io.Reader) (*UUID, error) {
	decoded := new(UUID)
	if read, err := source.Read(decoded[:]); err != nil {
		return nil, fmt.Errorf("cannot read [uuid] content: %w", err)
	} else if read != LengthOfUuid {
		return nil, errors.New("not enough bytes to read [uuid] content")
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
