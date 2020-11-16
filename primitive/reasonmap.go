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
	"net"
)

// <reasonmap> is a map of endpoint to failure reason codes, available from Protocol Version 5 onwards.
// The map is encoded starting with an [int] n followed by n pairs of <endpoint><failurecode> where
// <endpoint> is an [inetaddr] and <failurecode> is a [short].
// Note: a reasonmap is inherently a map, but it is not modeled as a map in Go because [inetaddr]
// is not a valid map key type.

type FailureReason struct {
	Endpoint net.IP
	Code     FailureCode
}

func ReadReasonMap(source io.Reader) ([]*FailureReason, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read reason map length: %w", err)
	} else {
		reasonMap := make([]*FailureReason, length)
		for i := 0; i < int(length); i++ {
			if addr, err := ReadInetAddr(source); err != nil {
				return nil, fmt.Errorf("cannot read reason map key for element %d: %w", i, err)
			} else if code, err := ReadShort(source); err != nil {
				return nil, fmt.Errorf("cannot read reason map value for element %d: %w", i, err)
			} else {
				reasonMap[i] = &FailureReason{addr, FailureCode(code)}
			}
		}
		return reasonMap, err
	}
}

func WriteReasonMap(reasonMap []*FailureReason, dest io.Writer) error {
	if err := WriteInt(int32(len(reasonMap)), dest); err != nil {
		return fmt.Errorf("cannot write reason map length: %w", err)
	}
	for i, reason := range reasonMap {
		if err := WriteInetAddr(reason.Endpoint, dest); err != nil {
			return fmt.Errorf("cannot write reason map key for element %d: %w", i, err)
		} else if err = WriteShort(uint16(reason.Code), dest); err != nil {
			return fmt.Errorf("cannot write reason map value for element %d: %w", i, err)
		}
	}
	return nil
}

func LengthOfReasonMap(reasonMap []*FailureReason) (int, error) {
	length := LengthOfInt
	for i, reason := range reasonMap {
		if inetAddrLength, err := LengthOfInetAddr(reason.Endpoint); err != nil {
			return -1, fmt.Errorf("cannot compute length of reason map key for element %d: %w", i, err)
		} else {
			length += inetAddrLength + LengthOfShort
		}
	}
	return length, nil
}
