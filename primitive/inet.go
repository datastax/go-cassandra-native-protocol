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

// [inet] (net.IP + port)

// Inet is the [inet] protocol type. It is the combination of a net.IP + port number.
// +k8s:deepcopy-gen=true
type Inet struct {
	Addr net.IP
	Port int32
}

func (i Inet) String() string {
	return fmt.Sprintf("%v:%v", i.Addr, i.Port)
}

func ReadInet(source io.Reader) (*Inet, error) {
	if addr, err := ReadInetAddr(source); err != nil {
		return nil, fmt.Errorf("cannot read [inet] address: %w", err)
	} else if port, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [inet] port number: %w", err)
	} else {
		return &Inet{Addr: addr, Port: port}, nil
	}
}

func WriteInet(inet *Inet, dest io.Writer) error {
	if inet == nil {
		return errors.New("cannot write nil [inet]")
	}
	if err := WriteInetAddr(inet.Addr, dest); err != nil {
		return fmt.Errorf("cannot write [inet] address: %w", err)
	} else if err := WriteInt(inet.Port, dest); err != nil {
		return fmt.Errorf("cannot write [inet] port number: %w", err)
	}
	return nil
}

func LengthOfInet(inet *Inet) (length int, err error) {
	if inet == nil {
		return -1, errors.New("cannot compute nil [inet] length")
	}
	length, err = LengthOfInetAddr(inet.Addr)
	if err != nil {
		return -1, err
	}
	return length + LengthOfInt, nil
}
