package primitives

import (
	"fmt"
	"io"
)

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
