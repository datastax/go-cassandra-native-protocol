package primitives

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"net"
)

const (
	LengthOfByte  = 1
	LengthOfShort = 2
	LengthOfInt   = 4
	LengthOfLong  = 8
	LengthOfUuid  = 16
)

// Functions to read and write CQL protocol primitive structures (as defined in section 3 of the protocol
// specification) to and from byte slices.

// [byte] ([byte] is not defined in protocol specs but is used by other primitives)

func ReadByte(source []byte) (decoded uint8, remaining []byte, err error) {
	length := len(source)
	if length < LengthOfByte {
		return 0, source, errors.New("not enough bytes to read [byte]")
	}
	return source[0], source[LengthOfByte:], nil
}

func WriteByte(b uint8, dest []byte) (remaining []byte, err error) {
	if cap(dest) < LengthOfByte {
		return dest, errors.New("not enough capacity to write [byte]")
	}
	dest[0] = b
	return dest[LengthOfByte:], nil
}

// [short]

func ReadShort(source []byte) (decoded uint16, remaining []byte, error error) {
	if len(source) < LengthOfShort {
		return 0, source, errors.New("not enough bytes to read [short]")
	}
	return binary.BigEndian.Uint16(source[:LengthOfShort]), source[LengthOfShort:], nil
}

func WriteShort(i uint16, dest []byte) (remaining []byte, err error) {
	if cap(dest) < LengthOfShort {
		return dest, errors.New("not enough capacity to write [short]")
	}
	binary.BigEndian.PutUint16(dest, i)
	return dest[LengthOfShort:], nil
}

// [int]

func ReadInt(source []byte) (decoded int32, remaining []byte, err error) {
	if len(source) < LengthOfInt {
		return 0, source, errors.New("not enough bytes to read [int]")
	}
	return int32(binary.BigEndian.Uint32(source[:LengthOfInt])), source[LengthOfInt:], nil
}

func WriteInt(i int32, dest []byte) (remaining []byte, err error) {
	if cap(dest) < LengthOfInt {
		return dest, errors.New("not enough capacity to write [int]")
	}
	binary.BigEndian.PutUint32(dest, uint32(i))
	return dest[LengthOfInt:], nil
}

// [long]

func ReadLong(source []byte) (decoded int64, remaining []byte, err error) {
	if len(source) < LengthOfLong {
		return 0, source, errors.New("not enough bytes to read [long]")
	}
	return int64(binary.BigEndian.Uint64(source[:LengthOfLong])), source[LengthOfLong:], nil
}

func WriteLong(l int64, dest []byte) (remaining []byte, err error) {
	if cap(dest) < LengthOfLong {
		return dest, errors.New("not enough capacity to write [long]")
	}
	binary.BigEndian.PutUint64(dest, uint64(l))
	return dest[LengthOfLong:], nil
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

func LengthOfString(s string) int {
	return LengthOfShort + len(s)
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

func LengthOfLongString(s string) int {
	return LengthOfInt + len(s)
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

func LengthOfStringList(list []string) int {
	length := LengthOfShort
	for _, s := range list {
		length += LengthOfString(s)
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

func LengthOfBytes(b []byte) int {
	return LengthOfInt + len(b)
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

func LengthOfShortBytes(b []byte) int {
	return LengthOfShort + len(b)
}

// [uuid]

func ReadUuid(source []byte) (decoded *cassandraprotocol.UUID, remaining []byte, err error) {
	if len(source) < LengthOfUuid {
		return nil, source, errors.New("not enough bytes to read [uuid] content")
	}
	decoded = new(cassandraprotocol.UUID)
	copy(decoded[:], source[:LengthOfUuid])
	return decoded, source[LengthOfUuid:], nil
}

func WriteUuid(uuid *cassandraprotocol.UUID, dest []byte) (remaining []byte, err error) {
	if uuid == nil {
		return dest, errors.New("cannot write nil [uuid]")
	}
	if len(dest) < LengthOfUuid {
		return dest, errors.New("not enough capacity to write [uuid] content")
	}
	copy(dest, uuid[:])
	return dest[LengthOfUuid:], nil
}

// [inetaddr]

func ReadInetAddr(source []byte) (inetAddr net.IP, remaining []byte, err error) {
	length, source, err := ReadByte(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [inetaddr] length: %w", err)
	}
	var addr net.IP
	if length == net.IPv4len {
		if len(source) < net.IPv4len {
			return nil, source, errors.New("not enough bytes to read [inetaddr] IPv4 content")
		}
		addr = net.IPv4(source[0], source[1], source[2], source[3])
	} else if length == net.IPv6len {
		if len(source) < net.IPv6len {
			return nil, source, errors.New("not enough bytes to read [inetaddr] IPv6 content")
		}
		addr = source[:net.IPv6len]
	} else {
		return nil, source, errors.New("unknown inet address length: " + string(length))
	}
	return addr, source[length:], nil
}

func WriteInetAddr(inetAddr net.IP, dest []byte) (remaining []byte, err error) {
	if inetAddr == nil {
		return dest, errors.New("cannot write nil [inetaddr]")
	}
	var length byte
	if inetAddr.To4() != nil {
		length = net.IPv4len
	} else {
		length = net.IPv6len
	}
	dest, err = WriteByte(length, dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [inetaddr] length: %w", err)
	}
	if length == net.IPv4len {
		if cap(dest) < net.IPv4len {
			return dest, errors.New("not enough capacity to write [inetaddr] IPv4 content")
		}
		copy(dest, inetAddr.To4())
		dest = dest[net.IPv4len:]
	} else {
		if cap(dest) < net.IPv6len {
			return dest, errors.New("not enough capacity to write [inetaddr] IPv6 content")
		}
		copy(dest, inetAddr.To16())
		dest = dest[net.IPv6len:]
	}
	return dest, nil
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

// [inet] (net.IP + port) see cassandraprotocol.Inet

func ReadInet(source []byte) (inet *cassandraprotocol.Inet, remaining []byte, err error) {
	var addr net.IP
	addr, source, err = ReadInetAddr(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [inet] address: %w", err)
	}
	var port int32
	port, source, err = ReadInt(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [inet] port number: %w", err)
	}
	return &cassandraprotocol.Inet{Addr: addr, Port: port}, source, nil
}

func WriteInet(inet *cassandraprotocol.Inet, dest []byte) (remaining []byte, err error) {
	if inet == nil {
		return dest, errors.New("cannot write nil [inet]")
	}
	dest, err = WriteInetAddr(inet.Addr, dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [inet] address: %w", err)
	}
	dest, err = WriteInt(inet.Port, dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write [inet] port number: %w", err)
	}
	return dest, nil
}

func LengthOfInet(inet *cassandraprotocol.Inet) (length int, err error) {
	if inet == nil {
		return -1, errors.New("cannot compute nil [inet] length")
	}
	length, err = LengthOfInetAddr(inet.Addr)
	if err != nil {
		return -1, err
	}
	return length + LengthOfInt, nil
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

func LengthOfStringMap(m map[string]string) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfString(value)
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

func LengthOfStringMultiMap(m map[string][]string) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfStringList(value)
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

func LengthOfBytesMap(m map[string][]byte) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfBytes(value)
	}
	return length
}
