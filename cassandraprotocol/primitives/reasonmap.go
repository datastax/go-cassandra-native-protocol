package primitives

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"io"
	"net"
)

// <reasonmap> is a map of endpoint to failure reason codes, available from Protocol Version 5 onwards.
// The map is encoded starting with an [int] n followed by n pairs of <endpoint><failurecode> where
// <endpoint> is an [inetaddr] and <failurecode> is a [short].
// Note that [inetaddr] is used as a map key, and is rendered as string since net.IP is not a valid map key type.

func ReadReasonMap(source io.Reader) (map[string]cassandraprotocol.ReasonMapFailureCode, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read reason map length: %w", err)
	} else {
		reasonMap := make(map[string]uint16, length)
		for i := 0; i < int(length); i++ {
			if addr, err := ReadInetAddr(source); err != nil {
				return nil, fmt.Errorf("cannot read reason map key: %w", err)
			} else if code, err := ReadShort(source); err != nil {
				return nil, fmt.Errorf("cannot read reason map value: %w", err)
			} else {
				reasonMap[addr.String()] = code
			}
		}
		return reasonMap, err
	}
}

func WriteReasonMap(reasonMap map[string]cassandraprotocol.ReasonMapFailureCode, dest io.Writer) error {
	if err := WriteInt(int32(len(reasonMap)), dest); err != nil {
		return fmt.Errorf("cannot write reason map length: %w", err)
	}
	for addrStr, code := range reasonMap {
		if addr := net.ParseIP(addrStr); addr == nil {
			return errors.New("cannot parse reason map key as net.IP: " + addrStr)
		} else if err := WriteInetAddr(addr, dest); err != nil {
			return fmt.Errorf("cannot write reason map key: %w", err)
		} else if err = WriteShort(code, dest); err != nil {
			return fmt.Errorf("cannot write reason map value: %w", err)
		}
	}
	return nil
}

func LengthOfReasonMap(reasonMap map[string]cassandraprotocol.ReasonMapFailureCode) (int, error) {
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
