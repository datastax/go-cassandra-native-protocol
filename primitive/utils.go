package primitive

import "net"

func CloneByteSlice(o []byte) []byte {
	if o == nil {
		return nil
	}

	newSlice := make([]byte, len(o))
	copy(newSlice, o)
	return newSlice
}

func CloneStringSlice(o []string) []string {
	if o == nil {
		return nil
	}

	newSlice := make([]string, len(o))
	copy(newSlice, o)
	return newSlice
}

func CloneInet(o *Inet) *Inet {
	var newAddress *Inet
	if o != nil {
		var newAddr net.IP
		if o.Addr != nil {
			newAddr = make(net.IP, len(o.Addr))
			copy(newAddr, o.Addr)
		} else {
			newAddr = nil
		}
		newAddress = &Inet{
			Addr: newAddr,
			Port: o.Port,
		}
	} else {
		newAddress = nil
	}
	return newAddress
}

func CloneOptions(o map[string]string) map[string]string {
	var newMap map[string]string
	if o != nil {
		newMap = make(map[string]string)
		for k, v := range o {
			newMap[k] = v
		}
	} else {
		newMap = nil
	}
	return newMap
}

func CloneSupportedOptions(o map[string][]string) map[string][]string {
	var newMap map[string][]string
	if o != nil {
		newMap = make(map[string][]string)
		for k, v := range o {
			newMap[k] = CloneStringSlice(v)
		}
	} else {
		newMap = nil
	}
	return newMap
}