package primitives

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

// [value]

func ReadValue(source io.Reader) (*cassandraprotocol.Value, error) {
	if contents, err := ReadBytes(source); err != nil {
		return nil, fmt.Errorf("cannot read [value] length: %w", err)
	} else if contents == nil {
		return &cassandraprotocol.Value{
			Type:     cassandraprotocol.ValueTypeNull,
			Contents: nil,
		}, nil
	} else {
		return &cassandraprotocol.Value{
			Type:     cassandraprotocol.ValueTypeRegular,
			Contents: contents,
		}, nil
	}
}

func WriteValue(value *cassandraprotocol.Value, dest io.Writer) error {
	switch value.Type {
	case cassandraprotocol.ValueTypeNull:
		fallthrough
	case cassandraprotocol.ValueTypeUnset:
		return WriteInt(value.Type, dest)
	case cassandraprotocol.ValueTypeRegular:
		return WriteBytes(value.Contents, dest)
	default:
		return fmt.Errorf("unknown value type: %v", value.Type)
	}
}

func LengthOfValue(value *cassandraprotocol.Value) (int, error) {
	switch value.Type {
	case cassandraprotocol.ValueTypeNull:
		fallthrough
	case cassandraprotocol.ValueTypeUnset:
		return LengthOfInt, nil
	case cassandraprotocol.ValueTypeRegular:
		return LengthOfBytes(value.Contents), nil
	default:
		return -1, fmt.Errorf("unknown value type: %v", value.Type)
	}
}

// positional [value]s

func ReadPositionalValues(source io.Reader) ([]*cassandraprotocol.Value, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read positional [value]s length: %w", err)
	} else {
		decoded := make([]*cassandraprotocol.Value, length)
		for i := uint16(0); i < length; i++ {
			if value, err := ReadValue(source); err != nil {
				return nil, fmt.Errorf("cannot read positional [value]s element: %w", err)
			} else {
				decoded[i] = value
			}
		}
		return decoded, nil
	}
}

func WritePositionalValues(values []*cassandraprotocol.Value, dest io.Writer) error {
	length := len(values)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write positional [value]s length: %w", err)
	}
	for _, value := range values {
		if err := WriteValue(value, dest); err != nil {
			return fmt.Errorf("cannot write positional [value] content: %w", err)
		}
	}
	return nil
}

func LengthOfPositionalValues(values []*cassandraprotocol.Value) (length int, err error) {
	length += LengthOfShort
	for _, value := range values {
		var valueLength int
		valueLength, err = LengthOfValue(value)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of positional [value]s: %w", err)
		}
		length += valueLength
	}
	return length, nil
}

// named [value]s

func ReadNamedValues(source io.Reader) (map[string]*cassandraprotocol.Value, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read named [value]s length: %w", err)
	} else {
		decoded := make(map[string]*cassandraprotocol.Value, length)
		for i := uint16(0); i < length; i++ {
			if name, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read named [value]s name: %w", err)
			} else if value, err := ReadValue(source); err != nil {
				return nil, fmt.Errorf("cannot read named [value]s content: %w", err)
			} else {
				decoded[name] = value
			}
		}
		return decoded, nil
	}
}

func WriteNamedValues(values map[string]*cassandraprotocol.Value, dest io.Writer) error {
	length := len(values)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write named [value]s length: %w", err)
	}
	for name, value := range values {
		if err := WriteString(name, dest); err != nil {
			return fmt.Errorf("cannot write named [value] name: %w", err)
		}
		if err := WriteValue(value, dest); err != nil {
			return fmt.Errorf("cannot write named [value] content: %w", err)
		}
	}
	return nil
}

func LengthOfNamedValues(values map[string]*cassandraprotocol.Value) (length int, err error) {
	length += LengthOfShort
	for name, value := range values {
		var nameLength = LengthOfString(name)
		var valueLength int
		valueLength, err = LengthOfValue(value)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of named [value]s: %w", err)
		}
		length += nameLength
		length += valueLength
	}
	return length, nil
}
