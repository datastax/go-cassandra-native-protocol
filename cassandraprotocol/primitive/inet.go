package primitive

import (
	"errors"
	"fmt"
	"io"
	"net"
)

// [inet] (net.IP + port)

type Inet struct {
	Addr net.IP
	Port int32
}

func (i Inet) String() string {
	return fmt.Sprintf("%v:%v", i.Addr, i.Port)
}

func ReadInet(source io.Reader) (*Inet, error) {
	if addr, err := ReadInetAddr(source); err != nil {
		return nil, fmt.Errorf("cannot read [inet] address: %w", err)
	} else if port, err := ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read [inet] port number: %w", err)
	} else {
		return &Inet{Addr: addr, Port: port}, nil
	}
}

func WriteInet(inet *Inet, dest io.Writer) error {
	if inet == nil {
		return errors.New("cannot write nil [inet]")
	}
	if err := WriteInetAddr(inet.Addr, dest); err != nil {
		return fmt.Errorf("cannot write [inet] address: %w", err)
	} else if err := WriteInt(inet.Port, dest); err != nil {
		return fmt.Errorf("cannot write [inet] port number: %w", err)
	}
	return nil
}

func LengthOfInet(inet *Inet) (length int, err error) {
	if inet == nil {
		return -1, errors.New("cannot compute nil [inet] length")
	}
	length, err = LengthOfInetAddr(inet.Addr)
	if err != nil {
		return -1, err
	}
	return length + LengthOfInt, nil
}
