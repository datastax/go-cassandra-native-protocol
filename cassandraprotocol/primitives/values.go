package primitives

import (
	"errors"
	"fmt"
	"io"
)

type ValueType = int32

const (
	ValueTypeRegular = ValueType(0)
	ValueTypeNull    = ValueType(-1)
	ValueTypeUnset   = ValueType(-2)
)

// Models the [value] protocol primitive structure
type Value struct {
	Type     ValueType
	Contents []byte
}

func NewValue(contents []byte) *Value {
	if contents == nil {
		return &Value{Type: ValueTypeNull}
	} else {
		return &Value{Type: ValueTypeRegular, Contents: contents}
	}
}

func NewNullValue() *Value {
	return NewValue(nil)
}

func NewUnsetValue() *Value {
	return &Value{Type: ValueTypeUnset}
}

// [value]

func ReadValue(source io.Reader, version ProtocolVersion) (*Value, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [value] length: %w", err)
	} else if length == ValueTypeNull {
		return NewNullValue(), nil
	} else if length == ValueTypeUnset {
		if version < ProtocolVersion4 {
			return nil, fmt.Errorf("cannot use unset value in protocol version: %v", version)
		}
		return NewUnsetValue(), nil
	} else if length < 0 {
		return nil, fmt.Errorf("invalid [value] length: %v", length)
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return nil, fmt.Errorf("cannot read [value] content: %w", err)
		} else if read != int(length) {
			return nil, errors.New("not enough bytes to read [value] content")
		}
		return NewValue(decoded), nil
	}
}

func WriteValue(value *Value, dest io.Writer, version ProtocolVersion) error {
	if value == nil {
		return errors.New("cannot write a nil [value]")
	}
	switch value.Type {
	case ValueTypeNull:
		return WriteInt(ValueTypeNull, dest)
	case ValueTypeUnset:
		if version < ProtocolVersion4 {
			return fmt.Errorf("cannot use unset value in protocol version: %v", version)
		}
		return WriteInt(ValueTypeUnset, dest)
	case ValueTypeRegular:
		if value.Contents == nil {
			return WriteInt(ValueTypeNull, dest)
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

func LengthOfValue(value *Value) (int, error) {
	if value == nil {
		return -1, errors.New("cannot compute length of a nil [value]")
	}
	switch value.Type {
	case ValueTypeNull:
		return LengthOfInt, nil
	case ValueTypeUnset:
		return LengthOfInt, nil
	case ValueTypeRegular:
		return LengthOfInt + len(value.Contents), nil
	default:
		return -1, fmt.Errorf("unknown [value] type: %v", value.Type)
	}
}

// positional [value]s

func ReadPositionalValues(source io.Reader, version ProtocolVersion) ([]*Value, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read positional [value]s length: %w", err)
	} else {
		decoded := make([]*Value, length)
		for i := uint16(0); i < length; i++ {
			if value, err := ReadValue(source, version); err != nil {
				return nil, fmt.Errorf("cannot read positional [value]s element %d content: %w", i, err)
			} else {
				decoded[i] = value
			}
		}
		return decoded, nil
	}
}

func WritePositionalValues(values []*Value, dest io.Writer, version ProtocolVersion) error {
	length := len(values)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write positional [value]s length: %w", err)
	}
	for i, value := range values {
		if err := WriteValue(value, dest, version); err != nil {
			return fmt.Errorf("cannot write positional [value]s element %d content: %w", i, err)
		}
	}
	return nil
}

func LengthOfPositionalValues(values []*Value) (length int, err error) {
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

func ReadNamedValues(source io.Reader, version ProtocolVersion) (map[string]*Value, error) {
	if length, err := ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read named [value]s length: %w", err)
	} else {
		decoded := make(map[string]*Value, length)
		for i := uint16(0); i < length; i++ {
			if name, err := ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read named [value]s entry %d name: %w", i, err)
			} else if value, err := ReadValue(source, version); err != nil {
				return nil, fmt.Errorf("cannot read named [value]s entry %d content: %w", i, err)
			} else {
				decoded[name] = value
			}
		}
		return decoded, nil
	}
}

func WriteNamedValues(values map[string]*Value, dest io.Writer, version ProtocolVersion) error {
	length := len(values)
	if err := WriteShort(uint16(length), dest); err != nil {
		return fmt.Errorf("cannot write named [value]s length: %w", err)
	}
	for name, value := range values {
		if err := WriteString(name, dest); err != nil {
			return fmt.Errorf("cannot write named [value]s entry '%v' name: %w", name, err)
		}
		if err := WriteValue(value, dest, version); err != nil {
			return fmt.Errorf("cannot write named [value]s entry '%v' content: %w", name, err)
		}
	}
	return nil
}

func LengthOfNamedValues(values map[string]*Value) (length int, err error) {
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
