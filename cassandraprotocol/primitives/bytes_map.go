package primitives

import (
	"fmt"
	"io"
)

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
