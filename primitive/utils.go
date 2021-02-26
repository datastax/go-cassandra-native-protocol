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

import "net"

func CloneByteSlice(o []byte) []byte {
	if o == nil {
		return nil
	}

	newSlice := make([]byte, len(o))
	copy(newSlice, o)
	return newSlice
}

func CloneStringSlice(o []string) []string {
	if o == nil {
		return nil
	}

	newSlice := make([]string, len(o))
	copy(newSlice, o)
	return newSlice
}

func CloneInet(o *Inet) *Inet {
	var newAddress *Inet
	if o != nil {
		var newAddr net.IP
		if o.Addr != nil {
			newAddr = make(net.IP, len(o.Addr))
			copy(newAddr, o.Addr)
		} else {
			newAddr = nil
		}
		newAddress = &Inet{
			Addr: newAddr,
			Port: o.Port,
		}
	} else {
		newAddress = nil
	}
	return newAddress
}

func CloneOptions(o map[string]string) map[string]string {
	var newMap map[string]string
	if o != nil {
		newMap = make(map[string]string)
		for k, v := range o {
			newMap[k] = v
		}
	} else {
		newMap = nil
	}
	return newMap
}

func CloneSupportedOptions(o map[string][]string) map[string][]string {
	var newMap map[string][]string
	if o != nil {
		newMap = make(map[string][]string)
		for k, v := range o {
			newMap[k] = CloneStringSlice(v)
		}
	} else {
		newMap = nil
	}
	return newMap
}