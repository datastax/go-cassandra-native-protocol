package primitives

import (
	"fmt"
	"io"
)

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
