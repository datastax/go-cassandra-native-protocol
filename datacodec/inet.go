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

package datacodec

import (
	"errors"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"net"
)

// Inet is a codec for the CQL inet type. Its preferred Go type is net.IP but it can encode from and decode to
// []byte as well.
var Inet Codec = &inetCodec{}

type inetCodec struct{}

func (c *inetCodec) DataType() datatype.DataType {
	return datatype.Inet
}

func (c *inetCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	var val net.IP
	if val, err = convertToIP(source); err == nil && val != nil {
		dest, err = writeInet(val)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *inetCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	var val net.IP
	if val, wasNull, err = readInet(source); err == nil {
		err = convertFromIP(val, wasNull, dest)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func convertToIP(source interface{}) (val net.IP, err error) {
	switch s := source.(type) {
	case net.IP:
		val = s
		val4 := val.To4()
		if val4 != nil {
			val = val4
		}
	case *net.IP:
		if s != nil {
			val = *s
			val4 := val.To4()
			if val4 != nil {
				val = val4
			}
		}
	case []byte:
		val = s
		val4 := val.To4()
		if val4 != nil {
			val = val4
		}
	case *[]byte:
		if s != nil {
			val = *s
			val4 := val.To4()
			if val4 != nil {
				val = val4
			}
		}
	case string:
		val = compactV4(net.ParseIP(s))
		if val == nil {
			err = errCannotParseString(s, errors.New("net.ParseIP(text) failed"))
		}
	case *string:
		if s != nil {
			val = compactV4(net.ParseIP(*s))
			if val == nil {
				err = errCannotParseString(*s, errors.New("net.ParseIP(text) failed"))
			}
		}
	case nil:
	default:
		err = ErrConversionNotSupported
	}
	if err != nil {
		err = errSourceConversionFailed(source, val, err)
	}
	return
}

func convertFromIP(val net.IP, wasNull bool, dest interface{}) (err error) {
	switch d := dest.(type) {
	case *interface{}:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = val
		}
	case *net.IP:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = compactV4(val)
		}
	case *[]byte:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = nil
		} else {
			*d = compactV4(val)
		}
	case *string:
		if d == nil {
			err = ErrNilDestination
		} else if wasNull {
			*d = ""
		} else {
			*d = val.String()
		}
	default:
		err = errDestinationInvalid(dest)
	}
	if err != nil {
		err = errDestinationConversionFailed(val, dest, err)
	}
	return
}

func writeInet(val net.IP) (dest []byte, err error) {
	length := len(val)
	if length == 0 {
		dest = nil
	} else if length == net.IPv4len || length == net.IPv6len {
		dest = compactV4(val)
	} else {
		err = errWrongFixedLengths(net.IPv4len, net.IPv6len, length)
	}
	if err != nil {
		err = errCannotWrite(val, err)
	}
	return
}

// The below functions are roughly equivalent to primitive.ReadInetAddr and primitive.WriteInetAddr.
// They favor the compact form (4-byte slice) for IPv4 addresses.

func readInet(source []byte) (val net.IP, wasNull bool, err error) {
	length := len(source)
	if length == 0 {
		wasNull = true
	} else if length == net.IPv4len {
		val = net.IPv4(source[0], source[1], source[2], source[3]).To4()
	} else if length == net.IPv6len {
		val = source
	} else {
		err = errWrongFixedLengths(net.IPv4len, net.IPv6len, length)
	}
	if err != nil {
		err = errCannotRead(val, err)
	}
	return
}

func compactV4(val net.IP) net.IP {
	if val != nil {
		val4 := val.To4()
		if val4 != nil {
			return val4
		}
	}
	return val
}
