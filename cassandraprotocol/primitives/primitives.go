package primitives

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"net"
)

const (
	SizeOfByte  = 1
	SizeOfShort = 2
	SizeOfInt   = 4
	SizeOfLong  = 8
	SizeOfUuid  = 16
)

// Functions to read and write CQL protocol primitive structures (as defined in section 3 of the protocol
// specification) to and from byte slices.

// [byte] ([byte] is not defined in protocol specs but is used by other primitives)

func ReadByte(source []byte) (decoded uint8, remaining []byte, err error) {
	length := len(source)
	if length < SizeOfByte {
		return 0, source, errors.New("not enough bytes to read [byte]")
	}
	return source[0], source[SizeOfByte:], nil
}

func WriteByte(b uint8, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfByte {
		return dest, errors.New("not enough capacity to write [byte]")
	}
	dest[0] = b
	return dest[SizeOfByte:], nil
}

// [short]

func ReadShort(source []byte) (decoded uint16, remaining []byte, error error) {
	length := len(source)
	if length < SizeOfShort {
		return 0, source, errors.New("not enough bytes to read [short]")
	}
	return binary.BigEndian.Uint16(source[:SizeOfShort]), source[SizeOfShort:], nil
}

func WriteShort(i uint16, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfShort {
		return dest, errors.New("not enough capacity to write [short]")
	}
	binary.BigEndian.PutUint16(dest, i)
	return dest[SizeOfShort:], nil
}

// [int]

func ReadInt(source []byte) (decoded int32, remaining []byte, err error) {
	length := len(source)
	if length < SizeOfInt {
		return 0, source, errors.New("not enough bytes to read [int]")
	}
	return int32(binary.BigEndian.Uint32(source[:SizeOfInt])), source[SizeOfInt:], nil
}

func WriteInt(i int32, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfInt {
		return dest, errors.New("not enough capacity to write [int]")
	}
	binary.BigEndian.PutUint32(dest, uint32(i))
	return dest[SizeOfInt:], nil
}

// [long]

func ReadLong(source []byte) (decoded int64, remaining []byte, err error) {
	length := len(source)
	if length < SizeOfLong {
		return 0, source, errors.New("not enough bytes to read [long]")
	}
	return int64(binary.BigEndian.Uint64(source[:SizeOfLong])), source[SizeOfLong:], nil
}

func WriteLong(l int64, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfLong {
		return dest, errors.New("not enough capacity to write [long]")
	}
	binary.BigEndian.PutUint64(dest, uint64(l))
	return dest[SizeOfLong:], nil
}

// [string]

func ReadString(source []byte) (decoded string, remaining []byte, err error) {
	var strLen uint16
	strLen, source, err = ReadShort(source)
	if err != nil {
		return "", source, fmt.Errorf("cannot read [string] length: %w", err)
	}
	length := len(source)
	if length < int(strLen) {
		return "", source, errors.New("not enough bytes to read [string] content")
	}
	str := string(source[:strLen])
	return str, source[strLen:], nil
}

func WriteString(s string, dest []byte) (remaining []byte, err error) {
	length := len(s)
	dest, err = WriteShort(uint16(length), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [string] length: %w", err)
	}
	if cap(dest) < length {
		return dest, errors.New("not enough capacity to write [string] content")
	}
	copy(dest, s)
	return dest[length:], nil
}

func SizeOfString(s string) int {
	return SizeOfShort + len(s)
}

// [long string]

func ReadLongString(source []byte) (decoded string, remaining []byte, err error) {
	var strLen int32
	strLen, source, err = ReadInt(source)
	if err != nil {
		return "", source, fmt.Errorf("cannot read [long string] length: %w", err)
	}
	length := len(source)
	if length < int(strLen) {
		return "", source, errors.New("not enough bytes to read [long string] content")
	}
	decoded = string(source[:strLen])
	return decoded, source[strLen:], nil
}

func WriteLongString(s string, dest []byte) (remaining []byte, err error) {
	length := len(s)
	dest, err = WriteInt(int32(length), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [long string] length: %w", err)
	}
	if cap(dest) < length {
		return dest, errors.New("not enough capacity to write [long string] content")
	}
	copy(dest, s)
	return dest[length:], nil
}

func SizeOfLongString(s string) int {
	return SizeOfInt + len(s)
}

// [string list]

func ReadStringList(source []byte) (decoded []string, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [string list] length: %w", err)
	}
	decoded = make([]string, length)
	for i := uint16(0); i < length; i++ {
		var str string
		str, source, err = ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [string list] element: %w", err)
		}
		decoded[i] = str
	}
	return decoded, source, nil
}

func WriteStringList(list []string, dest []byte) (remaining []byte, err error) {
	length := len(list)
	dest, err = WriteShort(uint16(length), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [string list] length: %w", err)
	}
	for _, s := range list {
		dest, err = WriteString(s, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [string list] element: %w", err)
		}
	}
	return dest, nil
}

func SizeOfStringList(list []string) int {
	length := SizeOfShort
	for _, s := range list {
		length += SizeOfString(s)
	}
	return length
}

// [bytes]

func ReadBytes(source []byte) (decoded []byte, remaining []byte, err error) {
	var length int32
	length, source, err = ReadInt(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [bytes] length: %w", err)
	}
	if length < 0 {
		return nil, source, nil
	}
	if len(source) < int(length) {
		return nil, source, errors.New("not enough bytes to read [bytes] content")
	}
	return source[:length], source[length:], nil

}

func WriteBytes(b []byte, dest []byte) (remaining []byte, err error) {
	if b == nil {
		dest, err = WriteInt(-1, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write null [bytes]: %w", err)
		}
		return dest, nil
	} else {
		length := len(b)
		dest, err = WriteInt(int32(length), dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [bytes] length: %w", err)
		}
		if cap(dest) < length {
			return dest, errors.New("not enough capacity to write [bytes] content")
		}
		copy(dest, b)
		return dest[length:], nil
	}
}

func SizeOfBytes(b []byte) int {
	return SizeOfInt + len(b)
}

// [short bytes]

func ReadShortBytes(source []byte) (decoded []byte, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [short bytes] length: %w", err)
	}
	if len(source) < int(length) {
		return nil, source, errors.New("not enough bytes to read [short bytes] content")
	}
	return source[:length], source[length:], nil
}

func WriteShortBytes(b []byte, dest []byte) (remaining []byte, err error) {
	length := len(b)
	dest, err = WriteShort(uint16(length), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [short bytes] length: %w", err)
	}
	if cap(dest) < length {
		return dest, errors.New("not enough capacity to write [short bytes] content")
	}
	copy(dest, b)
	return dest[length:], nil
}

func SizeOfShortBytes(b []byte) int {
	return SizeOfShort + len(b)
}

// [uuid]

func ReadUuid(source []byte) (decoded *cassandraprotocol.UUID, remaining []byte, err error) {
	if len(source) < SizeOfUuid {
		return nil, source, errors.New("not enough bytes to read [uuid] content")
	}
	copy(decoded[:], source[:SizeOfUuid])
	return decoded, source[SizeOfUuid:], nil
}

func WriteUuid(uuid *cassandraprotocol.UUID, dest []byte) (remaining []byte, err error) {
	if uuid == nil {
		return dest, errors.New("cannot write nil as [uuid]")
	}
	if len(dest) < SizeOfUuid {
		return dest, errors.New("not enough capacity to write [uuid] content")
	}
	copy(dest, uuid[:])
	return dest[SizeOfUuid:], nil
}

// [inet]

func ReadInet(source []byte) (inet *cassandraprotocol.Inet, remaining []byte, err error) {
	length, source, err := ReadByte(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [inet] length: %w", err)
	}
	var addr net.IP
	if length == net.IPv4len {
		if len(source) < net.IPv4len {
			return nil, source, errors.New("not enough bytes to read [inet] IPv4 content")
		}
		addr = net.IPv4(source[0], source[1], source[2], source[3])
	} else if length == net.IPv6len {
		if len(source) < net.IPv6len {
			return nil, source, errors.New("not enough bytes to read [inet] IPv6 content")
		}
		addr = source[:net.IPv6len]
	} else {
		return nil, source, errors.New("unknown inet address length: " + string(length))
	}
	var port int32
	port, source, err = ReadInt(source[4:])
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [inet] port number: %w", err)
	}
	return &cassandraprotocol.Inet{addr, port}, source, nil
}

func WriteInet(inet *cassandraprotocol.Inet, dest []byte) (remaining []byte, err error) {
	if inet == nil || inet.Addr == nil {
		return dest, errors.New("cannot write nil as [inet]")
	}
	var length byte
	if inet.Addr.To4() != nil {
		length = net.IPv4len
	} else {
		length = net.IPv6len
	}
	dest, err = WriteByte(length, dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [inet] length: %w", err)
	}
	if length == net.IPv4len {
		if cap(dest) < net.IPv4len {
			return dest, errors.New("not enough capacity to write [inet] IPv4 content")
		}
		copy(dest, inet.Addr.To4())
		dest = dest[net.IPv4len:]
	} else if length == net.IPv6len {
		if cap(dest) < net.IPv6len {
			return dest, errors.New("not enough capacity to write [inet] IPv6 content")
		}
		copy(dest, inet.Addr.To16())
		dest = dest[net.IPv6len:]
	} else {
		return dest, errors.New("wrong [inet] length: " + string(length))
	}
	dest, err = WriteInt(inet.Port, dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [inet] port number: %w", err)
	}
	return dest, nil
}

func SizeOfInet(inet *cassandraprotocol.Inet) (length int) {
	if inet.Addr.To4() != nil {
		length = net.IPv4len
	} else {
		length = net.IPv6len
	}
	return length + SizeOfInt
}

// [string map]

func ReadStringMap(source []byte) (decoded map[string]string, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [string map] length: %w", err)
	}
	stringMap := make(map[string]string, length)
	for i := uint16(0); i < length; i++ {
		var key string
		var value string
		key, source, err = ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [string map] key: %w", err)
		}
		value, source, err = ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [string map] value: %w", err)
		}
		stringMap[key] = value
	}
	return stringMap, source, nil
}

func WriteStringMap(m map[string]string, dest []byte) (remaining []byte, err error) {
	dest, err = WriteShort(uint16(len(m)), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [string map] length: %w", err)
	}
	for key, value := range m {
		dest, err = WriteString(key, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [string map] key: %w", err)
		}
		dest, err = WriteString(value, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [string map] value: %w", err)
		}
	}
	return dest, nil
}

func SizeOfStringMap(m map[string]string) int {
	length := SizeOfShort
	for key, value := range m {
		length += SizeOfString(key) + SizeOfString(value)
	}
	return length
}

// [string multimap]

func ReadStringMultiMap(source []byte) (decoded map[string][]string, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [string multimap] length: %w", err)
	}
	stringMap := make(map[string][]string, length)
	for i := uint16(0); i < length; i++ {
		var key string
		var value []string
		key, source, err = ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [string multimap] key: %w", err)
		}
		value, source, err = ReadStringList(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [string multimap] value: %w", err)
		}
		stringMap[key] = value
	}
	return stringMap, source, nil
}

func WriteStringMultiMap(m map[string][]string, dest []byte) (remaining []byte, err error) {
	dest, err = WriteShort(uint16(len(m)), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [string multimap] length: %w", err)
	}
	for key, value := range m {
		dest, err = WriteString(key, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [string multimap] key: %w", err)
		}
		dest, err = WriteStringList(value, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [string multimap] value: %w", err)
		}
	}
	return dest, nil
}

func SizeOfStringMultiMap(m map[string][]string) int {
	length := SizeOfShort
	for key, value := range m {
		length += SizeOfString(key) + SizeOfStringList(value)
	}
	return length
}

// [bytes map]

func ReadBytesMap(source []byte) (decoded map[string][]byte, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [bytes map] length: %w", err)
	}
	bytesMap := make(map[string][]byte, length)
	for i := uint16(0); i < length; i++ {
		var key string
		var value []byte
		key, source, err = ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [bytes map] key: %w", err)
		}
		value, source, err = ReadBytes(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read [bytes map] value: %w", err)
		}
		bytesMap[key] = value
	}
	return bytesMap, source, nil
}

func WriteBytesMap(m map[string][]byte, dest []byte) (remaining []byte, err error) {
	dest, err = WriteShort(uint16(len(m)), dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [bytes map] length: %w", err)
	}
	for key, value := range m {
		dest, err = WriteString(key, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [bytes map] key: %w", err)
		}
		dest, err = WriteBytes(value, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write [bytes map] value: %w", err)
		}
	}
	return dest, nil
}

func SizeOfBytesMap(m map[string][]byte) int {
	length := SizeOfShort
	for key, value := range m {
		length += SizeOfString(key) + SizeOfBytes(value)
	}
	return length
}
