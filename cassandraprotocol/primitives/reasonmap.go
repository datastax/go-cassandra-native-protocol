package primitives

import (
	"errors"
	"fmt"
	"net"
)

// <reasonmap> is a map of endpoint to failure reason codes, available from Protocol Version 5 onwards.
// The map is encoded starting with an [int] n followed by n pairs of <endpoint><failurecode> where
// <endpoint> is an [inetaddr] and <failurecode> is a [short].
// Note that [inetaddr] is used as a map key, and is rendered as string since net.IP is not a valid map key type.

func ReadReasonMap(source []byte) (reasonMap map[string]uint16, remaining []byte, err error) {
	var reasonMapLength int32
	reasonMapLength, source, err = ReadInt(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read reason map length: %w", err)
	}
	reasonMap = make(map[string]uint16, reasonMapLength)
	for i := 0; i < int(reasonMapLength); i++ {
		var addr net.IP
		var code uint16
		if addr, source, err = ReadInetAddr(source); err != nil {
			return nil, source, fmt.Errorf("cannot read reason map key: %w", err)
		} else if code, source, err = ReadShort(source); err != nil {
			return nil, source, fmt.Errorf("cannot read reason map value: %w", err)
		}
		reasonMap[addr.String()] = code
	}
	return reasonMap, source, err
}

func WriteReasonMap(reasonMap map[string]uint16, dest []byte) (remaining []byte, err error) {
	dest, err = WriteInt(int32(len(reasonMap)), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write reason map length: %w", err)
	}
	for addrStr, code := range reasonMap {
		if addr := net.ParseIP(addrStr); addr == nil {
			return dest, errors.New("cannot parse reason map key as net.IP: " + addrStr)
		} else {
			if dest, err = WriteInetAddr(addr, dest); err != nil {
				return dest, fmt.Errorf("cannot write reason map key: %w", err)
			}
			if dest, err = WriteShort(code, dest); err != nil {
				return dest, fmt.Errorf("cannot write reason map value: %w", err)
			}
		}
	}
	return dest, err
}

func LengthOfReasonMap(reasonMap map[string]uint16) (int, error) {
	length := LengthOfInt
	for addrStr := range reasonMap {
		if addr := net.ParseIP(addrStr); addr == nil {
			return -1, errors.New("cannot parse IP: " + addrStr)
		} else {
			inetAddrLength, err := LengthOfInetAddr(addr)
			if err != nil {
				return -1, fmt.Errorf("cannot compute length reason map key: %w", err)
			}
			length += inetAddrLength + LengthOfShort
		}
	}
	return length, nil
}
