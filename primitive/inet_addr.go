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
	"net"
)

// [inetaddr] is modeled by net.IP

func ReadInetAddr(source io.Reader) (net.IP, error) {
	if length, err := ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read [inetaddr] length: %w", err)
	} else {
		if length == net.IPv4len {
			decoded := make([]byte, net.IPv4len)
			if _, err := io.ReadFull(source, decoded); err != nil {
				return nil, fmt.Errorf("cannot read [inetaddr] IPv4 content: %w", err)
			}
			return net.IPv4(decoded[0], decoded[1], decoded[2], decoded[3]), nil
		} else if length == net.IPv6len {
			decoded := make([]byte, net.IPv6len)
			if _, err := io.ReadFull(source, decoded); err != nil {
				return nil, fmt.Errorf("cannot read [inetaddr] IPv6 content: %w", err)
			}
			return decoded, nil
		} else {
			return nil, errors.New("unknown inet address length: " + string(length))
		}
	}
}

func WriteInetAddr(inetAddr net.IP, dest io.Writer) error {
	if inetAddr == nil {
		return errors.New("cannot write nil [inetaddr]")
	}
	var length byte
	if inetAddr.To4() != nil {
		length = net.IPv4len
	} else {
		length = net.IPv6len
	}
	if err := WriteByte(length, dest); err != nil {
		return fmt.Errorf("cannot write [inetaddr] length: %w", err)
	}
	if length == net.IPv4len {
		if n, err := dest.Write(inetAddr.To4()); err != nil {
			return fmt.Errorf("cannot write [inetaddr] IPv4 content: %w", err)
		} else if n < net.IPv4len {
			return errors.New("not enough capacity to write [inetaddr] IPv4 content")
		}
	} else {
		if n, err := dest.Write(inetAddr.To16()); err != nil {
			return fmt.Errorf("cannot write [inetaddr] IPv6 content: %w", err)
		} else if n < net.IPv6len {
			return errors.New("not enough capacity to write [inetaddr] IPv content")
		}
	}
	return nil
}

func LengthOfInetAddr(inetAddr net.IP) (length int, err error) {
	if inetAddr == nil {
		return -1, errors.New("cannot compute nil [inetaddr] length")
	}
	length = LengthOfByte
	if inetAddr.To4() != nil {
		length += net.IPv4len
	} else {
		length += net.IPv6len
	}
	return length, nil
}
