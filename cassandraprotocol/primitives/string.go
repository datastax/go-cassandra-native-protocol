package primitives

import (
	"errors"
	"fmt"
	"io"
)

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
