package primitives

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
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

func ReadByte(source io.Reader) (decoded uint8, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [byte]: %w", err)
	}
	return decoded, err
}

func WriteByte(b uint8, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, b); err != nil {
		return fmt.Errorf("cannot write [byte]: %w", err)
	}
	return nil
}

// [short]

func ReadShort(source io.Reader) (decoded uint16, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [short]: %w", err)
	}
	return decoded, err
}

func WriteShort(i uint16, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, i); err != nil {
		return fmt.Errorf("cannot write [short]: %w", err)
	}
	return nil
}

// [int]

func ReadInt(source io.Reader) (decoded int32, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [int]: %w", err)
	}
	return decoded, err
}

func WriteInt(i int32, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, i); err != nil {
		return fmt.Errorf("cannot write [int]: %w", err)
	}
	return nil
}

// [long]

func ReadLong(source io.Reader) (decoded int64, err error) {
	if err = binary.Read(source, binary.BigEndian, &decoded); err != nil {
		err = fmt.Errorf("cannot read [long]: %w", err)
	}
	return decoded, err
}

func WriteLong(l int64, dest io.Writer) error {
	if err := binary.Write(dest, binary.BigEndian, l); err != nil {
		return fmt.Errorf("cannot write [long]: %w", err)
	}
	return nil
}

// [string]

func ReadString(source io.Reader) (string, error) {
	if length, err := ReadShort(source); err != nil {
		return "", fmt.Errorf("cannot read [string] length: %w", err)
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return "", fmt.Errorf("cannot read [string] content: %w", err)
		} else if read != int(length) {
			return "", errors.New("not enough bytes to read [string] content")
		}
		return string(decoded), nil
	}
}

func WriteString(s string, dest io.Writer) error {
	length := len(s)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write [string] length: %w", err)
	} else if n, err := dest.Write([]byte(s)); err != nil {
		return fmt.Errorf("cannot write [string] length: %w", err)
	} else if n < length {
		return errors.New("not enough capacity to write [string] content")
	}
	return nil
}

func LengthOfString(s string) int {
	return LengthOfShort + len(s)
}

// [long string]

func ReadLongString(source io.Reader) (string, error) {
	if length, err := ReadInt(source); err != nil {
		return "", fmt.Errorf("cannot read [long string] length: %w", err)
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return "", fmt.Errorf("cannot read [long string] content: %w", err)
		} else if read != int(length) {
			return "", errors.New("not enough bytes to read [long string] content")
		}
		return string(decoded), nil
	}
}

func WriteLongString(s string, dest io.Writer) error {
	length := len(s)
	if err := WriteInt(int32(length), dest); err != nil {
		return fmt.Errorf("cannot write [long string] length: %w", err)
	} else if n, err := dest.Write([]byte(s)); err != nil {
		return fmt.Errorf("cannot write [long string] length: %w", err)
	} else if n < length {
		return errors.New("not enough capacity to write [long string] content")
	}
	return nil
}

func LengthOfLongString(s string) int {
	return LengthOfInt + len(s)
}

// [string list]

func ReadStringList(source io.Reader) (decoded []string, err error) {
	var length uint16
	length, err = ReadShort(source)
	if err != nil {
		return nil, fmt.Errorf("cannot read [string list] length: %w", err)
	}
	decoded = make([]string, length)
	for i := uint16(0); i < length; i++ {
		var str string
		str, err = ReadString(source)
		if err != nil {
			return nil, fmt.Errorf("cannot read [string list] element %d: %w", i, err)
		}
		decoded[i] = str
	}
	return decoded, nil
}

func WriteStringList(list []string, dest io.Writer) error {
	length := len(list)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write [string list] length: %w", err)
	}
	for i, s := range list {
		if err := WriteString(s, dest); err != nil {
			return fmt.Errorf("cannot write [string list] element %d: %w", i, err)
		}
	}
	return nil
}

func LengthOfStringList(list []string) int {
	length := LengthOfShort
	for _, s := range list {
		length += LengthOfString(s)
	}
	return length
}

// [bytes]

func ReadBytes(source io.Reader) ([]byte, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [bytes] length: %w", err)
	} else if length < 0 {
		return nil, nil
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return nil, fmt.Errorf("cannot read [bytes] content: %w", err)
		} else if read != int(length) {
			return nil, errors.New("not enough bytes to read [bytes] content")
		}
		return decoded, nil
	}
}

func WriteBytes(b []byte, dest io.Writer) error {
	if b == nil {
		if err := WriteInt(-1, dest); err != nil {
			return fmt.Errorf("cannot write null [bytes]: %w", err)
		}
	} else {
		length := len(b)
		if err := WriteInt(int32(length), dest); err != nil {
			return fmt.Errorf("cannot write [bytes] length: %w", err)
		} else if n, err := dest.Write(b); err != nil {
			return fmt.Errorf("cannot write [bytes] content: %w", err)
		} else if n < length {
			return errors.New("not enough capacity to write [bytes] content")
		}
	}
	return nil
}

func LengthOfBytes(b []byte) int {
	return LengthOfInt + len(b)
}

// [short bytes]

func ReadShortBytes(source io.Reader) ([]byte, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read [short bytes] length: %w", err)
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return nil, fmt.Errorf("cannot read [short bytes] content: %w", err)
		} else if read != int(length) {
			return nil, errors.New("not enough bytes to read [short bytes] content")
		}
		return decoded, nil
	}
}

func WriteShortBytes(b []byte, dest io.Writer) error {
	length := len(b)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write [short bytes] length: %w", err)
	} else if n, err := dest.Write(b); err != nil {
		return fmt.Errorf("cannot write [short bytes] content: %w", err)
	} else if n < length {
		return errors.New("not enough capacity to write [short bytes] content")
	}
	return nil
}

func LengthOfShortBytes(b []byte) int {
	return LengthOfShort + len(b)
}

// [uuid]

func ReadUuid(source io.Reader) (*cassandraprotocol.UUID, error) {
	decoded := new(cassandraprotocol.UUID)
	if read, err := source.Read(decoded[:]); err != nil {
		return nil, fmt.Errorf("cannot read [uuid] content: %w", err)
	} else if read != LengthOfUuid {
		return nil, errors.New("not enough bytes to read [uuid] content")
	}
	return decoded, nil
}

func WriteUuid(uuid *cassandraprotocol.UUID, dest io.Writer) error {
	if uuid == nil {
		return errors.New("cannot write nil [uuid]")
	} else if n, err := dest.Write(uuid[:]); err != nil {
		return fmt.Errorf("cannot write [uuid] content: %w", err)
	} else if n < LengthOfUuid {
		return errors.New("not enough capacity to write [uuid] content")
	}
	return nil
}

// [inetaddr]

func ReadInetAddr(source io.Reader) (net.IP, error) {
	if length, err := ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read [inetaddr] length: %w", err)
	} else {
		if length == net.IPv4len {
			decoded := make([]byte, net.IPv4len)
			if read, err := source.Read(decoded); err != nil {
				return nil, errors.New("cannot read [inetaddr] IPv4 content")
			} else if read < net.IPv4len {
				return nil, errors.New("not enough bytes to read [inetaddr] IPv4 content")
			}
			return net.IPv4(decoded[0], decoded[1], decoded[2], decoded[3]), nil
		} else if length == net.IPv6len {
			decoded := make([]byte, net.IPv6len)
			if read, err := source.Read(decoded); err != nil {
				return nil, errors.New("cannot read [inetaddr] IPv content")
			} else if read < net.IPv6len {
				return nil, errors.New("not enough bytes to read [inetaddr] IPv6 content")
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

// [inet] (net.IP + port) see cassandraprotocol.Inet

func ReadInet(source io.Reader) (*cassandraprotocol.Inet, error) {
	if addr, err := ReadInetAddr(source); err != nil {
		return nil, fmt.Errorf("cannot read [inet] address: %w", err)
	} else if port, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [inet] port number: %w", err)
	} else {
		return &cassandraprotocol.Inet{Addr: addr, Port: port}, nil
	}
}

func WriteInet(inet *cassandraprotocol.Inet, dest io.Writer) error {
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

func ReadStringMap(source io.Reader) (map[string]string, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read [string map] length: %w", err)
	} else {
		decoded := make(map[string]string, length)
		for i := uint16(0); i < length; i++ {
			if key, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read [string map] entry %d key: %w", i, err)
			} else if value, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read [string map] entry %d value: %w", i, err)
			} else {
				decoded[key] = value
			}
		}
		return decoded, nil
	}
}

func WriteStringMap(m map[string]string, dest io.Writer) error {
	if err := WriteShort(uint16(len(m)), dest); err != nil {
		return fmt.Errorf("cannot write [string map] length: %w", err)
	}
	for key, value := range m {
		if err := WriteString(key, dest); err != nil {
			return fmt.Errorf("cannot write [string map] entry '%v' key: %w", key, err)
		}
		if err := WriteString(value, dest); err != nil {
			return fmt.Errorf("cannot write [string map] entry '%v' value: %w", key, err)
		}
	}
	return nil
}

func LengthOfStringMap(m map[string]string) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfString(value)
	}
	return length
}

// [string multimap]

func ReadStringMultiMap(source io.Reader) (decoded map[string][]string, err error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read [string multimap] length: %w", err)
	} else {
		decoded := make(map[string][]string, length)
		for i := uint16(0); i < length; i++ {
			if key, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read [string multimap] entry %d key: %w", i, err)
			} else if value, err := ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read [string multimap] entry %d value: %w", i, err)
			} else {
				decoded[key] = value
			}
		}
		return decoded, nil
	}
}

func WriteStringMultiMap(m map[string][]string, dest io.Writer) error {
	if err := WriteShort(uint16(len(m)), dest); err != nil {
		return fmt.Errorf("cannot write [string multimap] length: %w", err)
	}
	for key, value := range m {
		if err := WriteString(key, dest); err != nil {
			return fmt.Errorf("cannot write [string multimap] entry '%v' key: %w", key, err)
		}
		if err := WriteStringList(value, dest); err != nil {
			return fmt.Errorf("cannot write [string multimap] entry '%v' value: %w", key, err)
		}
	}
	return nil
}

func LengthOfStringMultiMap(m map[string][]string) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfStringList(value)
	}
	return length
}

// [bytes map]

func ReadBytesMap(source io.Reader) (map[string][]byte, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read [bytes map] length: %w", err)
	} else {
		decoded := make(map[string][]byte, length)
		for i := uint16(0); i < length; i++ {
			if key, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read [bytes map] entry %d key: %w", i, err)
			} else if value, err := ReadBytes(source); err != nil {
				return nil, fmt.Errorf("cannot read [bytes map] entry %d value: %w", i, err)
			} else {
				decoded[key] = value
			}
		}
		return decoded, nil
	}
}

func WriteBytesMap(m map[string][]byte, dest io.Writer) error {
	if err := WriteShort(uint16(len(m)), dest); err != nil {
		return fmt.Errorf("cannot write [bytes map] length: %w", err)
	}
	for key, value := range m {
		if err := WriteString(key, dest); err != nil {
			return fmt.Errorf("cannot write [bytes map] entry '%v' key: %w", key, err)
		}
		if err := WriteBytes(value, dest); err != nil {
			return fmt.Errorf("cannot write [bytes map] entry '%v' value: %w", value, err)
		}
	}
	return nil
}

func LengthOfBytesMap(m map[string][]byte) int {
	length := LengthOfShort
	for key, value := range m {
		length += LengthOfString(key) + LengthOfBytes(value)
	}
	return length
}
