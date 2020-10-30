package primitive

import (
	"errors"
	"fmt"
	"io"
)

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
