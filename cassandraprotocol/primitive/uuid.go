package primitive

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// [uuid]

const LengthOfUuid = 16

type UUID [16]byte

func (u UUID) String() string {
	return hex.EncodeToString(u[:])
}

func ReadUuid(source io.Reader) (*UUID, error) {
	decoded := new(UUID)
	if read, err := source.Read(decoded[:]); err != nil {
		return nil, fmt.Errorf("cannot read [uuid] content: %w", err)
	} else if read != LengthOfUuid {
		return nil, errors.New("not enough bytes to read [uuid] content")
	}
	return decoded, nil
}

func WriteUuid(uuid *UUID, dest io.Writer) error {
	if uuid == nil {
		return errors.New("cannot write nil [uuid]")
	} else if n, err := dest.Write(uuid[:]); err != nil {
		return fmt.Errorf("cannot write [uuid] content: %w", err)
	} else if n < LengthOfUuid {
		return errors.New("not enough capacity to write [uuid] content")
	}
	return nil
}
