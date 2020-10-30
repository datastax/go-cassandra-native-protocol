package primitive

import (
	"errors"
	"fmt"
	"io"
)

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
