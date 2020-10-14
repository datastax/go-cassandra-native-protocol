package cassandraprotocol

import (
	"encoding/binary"
	"errors"
	"net"
)

const (
	SizeOfByte  = 1
	SizeOfShort = 2
	SizeOfInt   = 4
	SizeOfLong  = 8
	SizeOfUuid  = 16
)

type UUID [16]byte

// Functions to read and write CQL protocol primitive structures (as defined in section 3 of the protocol
// specification) to and from byte slices.

// [byte] ([byte] is not defined in protocol specs but is used by other primitives)

func ReadByte(source []byte) (decoded uint8, remaining []byte, err error) {
	length := len(source)
	if length < SizeOfByte {
		return 0, source, errors.New("not enough bytes to read a protocol [byte]")
	}
	return source[0], source[SizeOfByte:], nil
}

func WriteByte(b uint8, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfByte {
		return dest, errors.New("not enough capacity to write a protocol [byte]")
	}
	dest[0] = b
	return dest[SizeOfByte:], nil
}

// [short]

func ReadShort(source []byte) (decoded uint16, remaining []byte, error error) {
	length := len(source)
	if length < SizeOfShort {
		return 0, source, errors.New("not enough bytes to read a protocol [short]")
	}
	return binary.BigEndian.Uint16(source[:SizeOfShort]), source[SizeOfShort:], nil
}

func WriteShort(i uint16, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfShort {
		return dest, errors.New("not enough capacity to write a protocol [short]")
	}
	binary.BigEndian.PutUint16(dest, i)
	return dest[SizeOfShort:], nil
}

// [int]

func ReadInt(source []byte) (decoded int32, remaining []byte, err error) {
	length := len(source)
	if length < SizeOfInt {
		return 0, source, errors.New("not enough bytes to read a protocol [int]")
	}
	return int32(binary.BigEndian.Uint32(source[:SizeOfInt])), source[SizeOfInt:], nil
}

func WriteInt(i int32, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfInt {
		return dest, errors.New("not enough capacity to write a protocol [int]")
	}
	binary.BigEndian.PutUint32(dest, uint32(i))
	return dest[SizeOfInt:], nil
}

// [long]

func ReadLong(source []byte) (decoded int64, remaining []byte, err error) {
	length := len(source)
	if length < SizeOfLong {
		return 0, source, errors.New("not enough bytes to read a protocol [long]")
	}
	return int64(binary.BigEndian.Uint64(source[:SizeOfLong])), source[SizeOfLong:], nil
}

func WriteLong(l int64, dest []byte) (remaining []byte, err error) {
	if cap(dest) < SizeOfLong {
		return dest, errors.New("not enough capacity to write a protocol [long]")
	}
	binary.BigEndian.PutUint64(dest, uint64(l))
	return dest[SizeOfLong:], nil
}

// [string]

func ReadString(source []byte) (decoded string, remaining []byte, err error) {
	var strLen uint16
	strLen, source, err = ReadShort(source)
	if err != nil {
		return "", source, err
	}
	length := len(source)
	if length < int(strLen) {
		return "", source, errors.New("not enough bytes to read a protocol [string]")
	}
	str := string(source[:strLen])
	return str, source[strLen:], nil
}

func WriteString(s string, dest []byte) (remaining []byte, err error) {
	l := len(s)
	dest, err = WriteShort(uint16(l), dest)
	if err != nil {
		return dest, err
	}
	if cap(dest) < l {
		return dest, errors.New("not enough capacity to write a protocol [string]")
	}
	copy(dest, s)
	return dest[l:], nil
}

func SizeOfString(s string) int {
	return SizeOfShort + len(s)
}

// [long string]

func ReadLongString(source []byte) (decoded string, remaining []byte, err error) {
	var strLen int32
	strLen, source, err = ReadInt(source)
	if err != nil {
		return "", source, err
	}
	length := len(source)
	if length < int(strLen) {
		return "", source, errors.New("not enough bytes to read a protocol [long string]")
	}
	str := string(source[:strLen])
	return str, source[strLen:], nil
}

func WriteLongString(s string, dest []byte) (remaining []byte, err error) {
	l := len(s)
	dest, err = WriteInt(int32(l), dest)
	if err != nil {
		return dest, err
	}
	if cap(dest) < l {
		return dest, errors.New("not enough capacity to write a protocol [long string]")
	}
	copy(dest, s)
	return dest[l:], nil
}

func SizeOfLongString(s string) int {
	return SizeOfInt + len(s)
}

// [string list]

func ReadStringList(source []byte) (decoded []string, remaining []byte, err error) {
	var size uint16
	size, source, err = ReadShort(source)
	if err != nil {
		return nil, source, err
	}
	list := make([]string, size)
	for i := uint16(0); i < size; i++ {
		var str string
		str, source, err = ReadString(source)
		if err != nil {
			return nil, source, err
		}
		list[i] = str
	}
	return list, source, nil
}

func WriteStringList(list []string, dest []byte) (remaining []byte, err error) {
	l := len(list)
	dest, err = WriteShort(uint16(l), dest)
	if err != nil {
		return dest, err
	}
	for _, s := range list {
		dest, err = WriteString(s, dest)
		if err != nil {
			return dest, err
		}
	}
	return dest, nil
}

func SizeOfStringList(list []string) int {
	size := SizeOfShort
	for _, s := range list {
		size += SizeOfString(s)
	}
	return size
}

// [bytes]

func ReadBytes(source []byte) (decoded []byte, remaining []byte, err error) {
	var sliceLen int32
	sliceLen, source, err = ReadInt(source)
	if err != nil {
		return nil, source, err
	}
	length := len(source)
	if length < int(sliceLen) {
		return nil, source, errors.New("not enough bytes to read a protocol [bytes]")
	}
	return source[:sliceLen], source[sliceLen:], nil

}

func WriteBytes(b []byte, dest []byte) (remaining []byte, err error) {
	l := len(b)
	dest, err = WriteInt(int32(l), dest)
	if err != nil {
		return dest, err
	}
	copy(dest, b)
	return dest[l:], nil
}

func SizeOfBytes(b []byte) int {
	return SizeOfInt + len(b)
}

// [short bytes]

func ReadShortBytes(source []byte) (decoded []byte, remaining []byte, err error) {
	var sliceLen uint16
	sliceLen, source, err = ReadShort(source)
	if err != nil {
		return nil, source, err
	}
	length := len(source)
	if length < int(sliceLen) {
		return nil, source, errors.New("not enough bytes to read a protocol [short bytes]")
	}
	slice := source[:sliceLen]
	return slice, source[sliceLen:], nil
}

func WriteShortBytes(b []byte, dest []byte) (remaining []byte, err error) {
	l := len(b)
	dest, err = WriteShort(uint16(l), dest)
	if err != nil {
		return dest, err
	}
	copy(dest, b)
	return dest[l:], nil
}

func SizeOfShortBytes(b []byte) int {
	return SizeOfShort + len(b)
}

// [uuid]

func ReadUuid(source []byte) (decoded *UUID, remaining []byte, err error) {
	if len(source) < SizeOfUuid {
		return nil, source, errors.New("not enough bytes to read a protocol [uuid]")
	}
	var uuid UUID
	copy(uuid[:], source[:SizeOfUuid])
	return &uuid, source[SizeOfUuid:], nil
}

func WriteUuid(uuid *UUID, dest []byte) (remaining []byte, err error) {
	if len(dest) < SizeOfUuid {
		return dest, errors.New("not enough bytes to write a protocol [uuid]")
	}
	copy(dest, uuid[:])
	return dest[SizeOfUuid:], nil
}

// [inet]

func ReadInet(source []byte) (addr net.IP, port int32, remaining []byte, err error) {
	if len(source) == 0 {
		return nil, -1, source, errors.New("not enough bytes to read a protocol [inet]")
	}
	size := source[0]
	source = source[1:]
	if size == 4 {
		if len(source) < 4+4 {
			return nil, -1, source, errors.New("not enough bytes to read a protocol [inet]")
		}
		ip := net.IPv4(source[0], source[1], source[2], source[3])
		port, source, _ := ReadInt(source[4:])
		return ip, port, source, nil
	} else if size == 16 {
		if len(source) < 16+4 {
			return nil, -1, source, errors.New("not enough bytes to read a protocol [inet]")
		}
		ip := net.IP(source[:16])
		port, source, _ := ReadInt(source[16:])
		return ip, port, source, nil
	} else {
		return nil, -1, source, errors.New("unknown inet address size: " + string(size))
	}
}

// [string map]

func ReadStringMap(source []byte) (decoded map[string]string, remaining []byte, err error) {
	var size uint16
	size, source, err = ReadShort(source)
	if err != nil {
		return nil, source, err
	}
	stringMap := make(map[string]string, size)
	for i := uint16(0); i < size; i++ {
		var key string
		var value string
		key, source, err = ReadString(source)
		if err != nil {
			return nil, source, err
		}
		value, source, err = ReadString(source)
		if err != nil {
			return nil, source, err
		}
		stringMap[key] = value
	}
	return stringMap, source, nil
}

func WriteStringMap(m map[string]string, dest []byte) (remaining []byte, err error) {
	dest, err = WriteShort(uint16(len(m)), dest)
	if err != nil {
		return dest, err
	}
	for key, value := range m {
		dest, err = WriteString(key, dest)
		if err != nil {
			return dest, err
		}
		dest, err = WriteString(value, dest)
		if err != nil {
			return dest, err
		}
	}
	return dest, nil
}

func SizeOfStringMap(m map[string]string) int {
	size := SizeOfShort
	for key, value := range m {
		size += SizeOfString(key) + SizeOfString(value)
	}
	return size
}

// [string multimap]

func ReadStringMultiMap(source []byte) (decoded map[string][]string, remaining []byte, err error) {
	var size uint16
	size, source, err = ReadShort(source)
	if err != nil {
		return nil, source, err
	}
	stringMap := make(map[string][]string, size)
	for i := uint16(0); i < size; i++ {
		var key string
		var value []string
		key, source, err = ReadString(source)
		if err != nil {
			return nil, source, err
		}
		value, source, err = ReadStringList(source)
		if err != nil {
			return nil, source, err
		}
		stringMap[key] = value
	}
	return stringMap, source, nil
}

func WriteStringMultiMap(m map[string][]string, dest []byte) (remaining []byte, err error) {
	dest, err = WriteShort(uint16(len(m)), dest)
	if err != nil {
		return dest, err
	}
	for key, value := range m {
		dest, err = WriteString(key, dest)
		if err != nil {
			return dest, err
		}
		dest, err = WriteStringList(value, dest)
		if err != nil {
			return dest, err
		}
	}
	return dest, nil
}

func SizeOfStringMultiMap(m map[string][]string) int {
	size := SizeOfShort
	for key, value := range m {
		size += SizeOfString(key) + SizeOfStringList(value)
	}
	return size
}

// [bytes map]

func ReadBytesMap(source []byte) (decoded map[string][]byte, remaining []byte, err error) {
	var size uint16
	size, source, err = ReadShort(source)
	if err != nil {
		return nil, source, err
	}
	bytesMap := make(map[string][]byte, size)
	for i := uint16(0); i < size; i++ {
		var key string
		var value []byte
		key, source, err = ReadString(source)
		if err != nil {
			return nil, source, err
		}
		value, source, err = ReadBytes(source)
		if err != nil {
			return nil, source, err
		}
		bytesMap[key] = value
	}
	return bytesMap, source, nil
}

func WriteBytesMap(m map[string][]byte, dest []byte) (remaining []byte, err error) {
	dest, err = WriteShort(uint16(len(m)), dest)
	if err != nil {
		return dest, err
	}
	for key, value := range m {
		dest, err = WriteString(key, dest)
		if err != nil {
			return dest, err
		}
		dest, err = WriteBytes(value, dest)
		if err != nil {
			return dest, err
		}
	}
	return dest, nil
}

func SizeOfBytesMap(m map[string][]byte) int {
	size := SizeOfShort
	for key, value := range m {
		size += SizeOfString(key) + SizeOfBytes(value)
	}
	return size
}
