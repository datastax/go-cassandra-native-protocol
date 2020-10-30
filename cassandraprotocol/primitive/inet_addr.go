package primitive

import (
	"errors"
	"fmt"
	"io"
	"net"
)

// [inetaddr] is modeled by net.IP

func ReadInetAddr(source io.Reader) (net.IP, error) {
	if length, err := ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read [inetaddr] length: %w", err)
	} else {
		if length == net.IPv4len {
			decoded := make([]byte, net.IPv4len)
			if read, err := source.Read(decoded); err != nil {
				return nil, errors.New("cannot read [inetaddr] IPv4 content")
			} else if read < net.IPv4len {
				return nil, errors.New("not enough bytes to read [inetaddr] IPv4 content")
			}
			return net.IPv4(decoded[0], decoded[1], decoded[2], decoded[3]), nil
		} else if length == net.IPv6len {
			decoded := make([]byte, net.IPv6len)
			if read, err := source.Read(decoded); err != nil {
				return nil, errors.New("cannot read [inetaddr] IPv content")
			} else if read < net.IPv6len {
				return nil, errors.New("not enough bytes to read [inetaddr] IPv6 content")
			}
			return decoded, nil
		} else {
			return nil, errors.New("unknown inet address length: " + string(length))
		}
	}
}

func WriteInetAddr(inetAddr net.IP, dest io.Writer) error {
	if inetAddr == nil {
		return errors.New("cannot write nil [inetaddr]")
	}
	var length byte
	if inetAddr.To4() != nil {
		length = net.IPv4len
	} else {
		length = net.IPv6len
	}
	if err := WriteByte(length, dest); err != nil {
		return fmt.Errorf("cannot write [inetaddr] length: %w", err)
	}
	if length == net.IPv4len {
		if n, err := dest.Write(inetAddr.To4()); err != nil {
			return fmt.Errorf("cannot write [inetaddr] IPv4 content: %w", err)
		} else if n < net.IPv4len {
			return errors.New("not enough capacity to write [inetaddr] IPv4 content")
		}
	} else {
		if n, err := dest.Write(inetAddr.To16()); err != nil {
			return fmt.Errorf("cannot write [inetaddr] IPv6 content: %w", err)
		} else if n < net.IPv6len {
			return errors.New("not enough capacity to write [inetaddr] IPv content")
		}
	}
	return nil
}

func LengthOfInetAddr(inetAddr net.IP) (length int, err error) {
	if inetAddr == nil {
		return -1, errors.New("cannot compute nil [inetaddr] length")
	}
	length = LengthOfByte
	if inetAddr.To4() != nil {
		length += net.IPv4len
	} else {
		length += net.IPv6len
	}
	return length, nil
}
