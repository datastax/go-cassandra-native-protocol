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
	"encoding/binary"
	"fmt"
	"io"
)

const (
	LengthOfByte  = 1
	LengthOfShort = 2
	LengthOfInt   = 4
	LengthOfLong  = 8
)

// [byte] ([byte] is not defined in protocol specs but is used by other primitives)

func ReadByte(source io.Reader) (decoded uint8, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [byte]: %w", err)
	}
	return decoded, err
}

func WriteByte(b uint8, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, b); err != nil {
		return fmt.Errorf("cannot write [byte]: %w", err)
	}
	return nil
}

// [short]

func ReadShort(source io.Reader) (decoded uint16, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [short]: %w", err)
	}
	return decoded, err
}

func WriteShort(i uint16, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, i); err != nil {
		return fmt.Errorf("cannot write [short]: %w", err)
	}
	return nil
}

// [int]

func ReadInt(source io.Reader) (decoded int32, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [int]: %w", err)
	}
	return decoded, err
}

func WriteInt(i int32, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, i); err != nil {
		return fmt.Errorf("cannot write [int]: %w", err)
	}
	return nil
}

// [long]

func ReadLong(source io.Reader) (decoded int64, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [long]: %w", err)
	}
	return decoded, err
}

func WriteLong(l int64, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, l); err != nil {
		return fmt.Errorf("cannot write [long]: %w", err)
	}
	return nil
}
