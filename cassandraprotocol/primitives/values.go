package primitives

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
)

// [value]

func ReadValue(source []byte) (decoded *cassandraprotocol.Value, remaining []byte, err error) {
	var contents []byte
	contents, source, err = ReadBytes(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [value] length: %w", err)
	}
	if contents == nil {
		return &cassandraprotocol.Value{
			Type:     cassandraprotocol.ValueTypeNull,
			Contents: nil,
		}, source, nil
	} else {
		return &cassandraprotocol.Value{
			Type:     cassandraprotocol.ValueTypeRegular,
			Contents: contents,
		}, source, nil
	}
}

func WriteValue(value *cassandraprotocol.Value, dest []byte) (remaining []byte, err error) {
	switch value.Type {
	case cassandraprotocol.ValueTypeNull:
		fallthrough
	case cassandraprotocol.ValueTypeUnset:
		dest, err = WriteInt(value.Type, dest)
	case cassandraprotocol.ValueTypeRegular:
		dest, err = WriteBytes(value.Contents, dest)
	default:
		return dest, fmt.Errorf("unknown value type: %v", value.Type)
	}
	if err != nil {
		return dest, fmt.Errorf("cannot write [value] content: %w", err)
	}
	return dest, nil
}

func SizeOfValue(value *cassandraprotocol.Value) (int, error) {
	switch value.Type {
	case cassandraprotocol.ValueTypeNull:
		fallthrough
	case cassandraprotocol.ValueTypeUnset:
		return SizeOfInt, nil
	case cassandraprotocol.ValueTypeRegular:
		return SizeOfBytes(value.Contents), nil
	default:
		return -1, fmt.Errorf("unknown value type: %v", value.Type)
	}
}

// positional [value]s

func ReadPositionalValues(source []byte) (decoded []*cassandraprotocol.Value, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read positional [value]s length: %w", err)
	}
	decoded = make([]*cassandraprotocol.Value, length)
	for i := uint16(0); i < length; i++ {
		var value *cassandraprotocol.Value
		value, source, err = ReadValue(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read positional [value]s element: %w", err)
		}
		decoded[i] = value
	}
	return decoded, source, nil
}

func WritePositionalValues(values []*cassandraprotocol.Value, dest []byte) (remaining []byte, err error) {
	length := len(values)
	remaining, err = WriteShort(uint16(length), dest)
	if err != nil {
		return remaining, fmt.Errorf("cannot write positional [value]s length: %w", err)
	}
	for _, value := range values {
		remaining, err = WriteValue(value, dest)
		if err != nil {
			return remaining, fmt.Errorf("cannot write positional [value] content: %w", err)
		}
	}
	return remaining, nil
}

func SizeOfPositionalValues(values []*cassandraprotocol.Value) (size int, err error) {
	size += SizeOfShort
	for _, value := range values {
		var valueSize int
		valueSize, err = SizeOfValue(value)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of positional [value]s: %w", err)
		}
		size += valueSize
	}
	return size, nil
}

// named [value]s

func ReadNamedValues(source []byte) (decoded map[string]*cassandraprotocol.Value, remaining []byte, err error) {
	var length uint16
	length, source, err = ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read named [value]s length: %w", err)
	}
	decoded = make(map[string]*cassandraprotocol.Value, length)
	for i := uint16(0); i < length; i++ {
		var name string
		var value *cassandraprotocol.Value
		name, source, err = ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read named [value]s name: %w", err)
		}
		value, source, err = ReadValue(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read named [value]s content: %w", err)
		}
		decoded[name] = value
	}
	return decoded, source, nil
}

func WriteNamedValues(values map[string]*cassandraprotocol.Value, dest []byte) (remaining []byte, err error) {
	length := len(values)
	remaining, err = WriteShort(uint16(length), dest)
	if err != nil {
		return remaining, fmt.Errorf("cannot write named [value]s length: %w", err)
	}
	for name, value := range values {
		remaining, err = WriteString(name, dest)
		if err != nil {
			return remaining, fmt.Errorf("cannot write named [value] name: %w", err)
		}
		remaining, err = WriteValue(value, dest)
		if err != nil {
			return remaining, fmt.Errorf("cannot write named [value] content: %w", err)
		}
	}
	return remaining, nil
}

func SizeOfNamedValues(values map[string]*cassandraprotocol.Value) (size int, err error) {
	size += SizeOfShort
	for name, value := range values {
		var nameSize = SizeOfString(name)
		var valueSize int
		valueSize, err = SizeOfValue(value)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of named [value]s: %w", err)
		}
		size += nameSize
		size += valueSize
	}
	return size, nil
}
