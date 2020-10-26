package primitives

import (
	"errors"
	"fmt"
	"io"
)

// [bytes]

func ReadBytes(source io.Reader) ([]byte, error) {
	if length, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [bytes] length: %w", err)
	} else if length < 0 {
		return nil, nil
	} else {
		decoded := make([]byte, length)
		if read, err := source.Read(decoded); err != nil {
			return nil, fmt.Errorf("cannot read [bytes] content: %w", err)
		} else if read != int(length) {
			return nil, errors.New("not enough bytes to read [bytes] content")
		}
		return decoded, nil
	}
}

func WriteBytes(b []byte, dest io.Writer) error {
	if b == nil {
		if err := WriteInt(-1, dest); err != nil {
			return fmt.Errorf("cannot write null [bytes]: %w", err)
		}
	} else {
		length := len(b)
		if err := WriteInt(int32(length), dest); err != nil {
			return fmt.Errorf("cannot write [bytes] length: %w", err)
		} else if n, err := dest.Write(b); err != nil {
			return fmt.Errorf("cannot write [bytes] content: %w", err)
		} else if n < length {
			return errors.New("not enough capacity to write [bytes] content")
		}
	}
	return nil
}

func LengthOfBytes(b []byte) int {
	return LengthOfInt + len(b)
}
