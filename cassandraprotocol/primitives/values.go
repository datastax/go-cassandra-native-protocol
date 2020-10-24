package primitives

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

// [value]

func ReadValue(source io.Reader) (*cassandraprotocol.Value, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [value] length: %w", err)
	} else if length == cassandraprotocol.ValueTypeNull {
		return cassandraprotocol.NewNullValue(), nil
	} else if length == cassandraprotocol.ValueTypeUnset {
		return cassandraprotocol.NewUnsetValue(), nil
	} else if length < 0 {
		return nil, fmt.Errorf("invalid [value] length: %v", length)
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return nil, fmt.Errorf("cannot read [value] content: %w", err)
		} else if read != int(length) {
			return nil, errors.New("not enough bytes to read [value] content")
		}
		return cassandraprotocol.NewValue(decoded), nil
	}
}

func WriteValue(value *cassandraprotocol.Value, dest io.Writer) error {
	if value == nil {
		return errors.New("cannot write a nil [value]")
	}
	switch value.Type {
	case cassandraprotocol.ValueTypeNull:
		return WriteInt(cassandraprotocol.ValueTypeNull, dest)
	case cassandraprotocol.ValueTypeUnset:
		return WriteInt(cassandraprotocol.ValueTypeUnset, dest)
	case cassandraprotocol.ValueTypeRegular:
		if value.Contents == nil {
			return WriteInt(cassandraprotocol.ValueTypeNull, dest)
		} else {
			length := len(value.Contents)
			if err := WriteInt(int32(length), dest); err != nil {
				return fmt.Errorf("cannot write [value] length: %w", err)
			} else if n, err := dest.Write(value.Contents); err != nil {
				return fmt.Errorf("cannot write [value] content: %w", err)
			} else if n < length {
				return errors.New("not enough capacity to write [value] content")
			}
			return nil
		}
	default:
		return fmt.Errorf("unknown [value] type: %v", value.Type)
	}
}

func LengthOfValue(value *cassandraprotocol.Value) (int, error) {
	if value == nil {
		return -1, errors.New("cannot compute length of a nil [value]")
	}
	switch value.Type {
	case cassandraprotocol.ValueTypeNull:
		return LengthOfInt, nil
	case cassandraprotocol.ValueTypeUnset:
		return LengthOfInt, nil
	case cassandraprotocol.ValueTypeRegular:
		return LengthOfInt + len(value.Contents), nil
	default:
		return -1, fmt.Errorf("unknown [value] type: %v", value.Type)
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
				return nil, fmt.Errorf("cannot read positional [value]s element %d content: %w", i, err)
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
	for i, value := range values {
		if err := WriteValue(value, dest); err != nil {
			return fmt.Errorf("cannot write positional [value]s element %d content: %w", i, err)
		}
	}
	return nil
}

func LengthOfPositionalValues(values []*cassandraprotocol.Value) (length int, err error) {
	length += LengthOfShort
	for i, value := range values {
		var valueLength int
		valueLength, err = LengthOfValue(value)
		if err != nil {
			return -1, fmt.Errorf("cannot compute length of positional [value] %d: %w", i, err)
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
				return nil, fmt.Errorf("cannot read named [value]s entry %d name: %w", i, err)
			} else if value, err := ReadValue(source); err != nil {
				return nil, fmt.Errorf("cannot read named [value]s entry %d content: %w", i, err)
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
			return fmt.Errorf("cannot write named [value]s entry '%v' name: %w", name, err)
		}
		if err := WriteValue(value, dest); err != nil {
			return fmt.Errorf("cannot write named [value]s entry '%v' content: %w", name, err)
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
