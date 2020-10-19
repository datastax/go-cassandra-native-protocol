package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
)

type setType struct {
	elementType DataType
}

func (t *setType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeSet
}

func (t *setType) String() string {
	return fmt.Sprintf("set<%v>", t.elementType)
}

type setTypeCodec struct{}

func (c *setTypeCodec) Encode(t DataType, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	setType, ok := t.(*setType)
	if !ok {
		return dest, errors.New(fmt.Sprintf("expected setType struct, got %T", t))
	} else if dest, err = WriteDataType(setType.elementType, dest, version); err != nil {
		return dest, fmt.Errorf("cannot write set element type: %w", err)
	}
	return dest, nil
}

func (c *setTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	setType, ok := t.(*setType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected setType struct, got %T", t))
	} else if elementLength, err := LengthOfDataType(setType.elementType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of set element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func (c *setTypeCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (decoded DataType, remaining []byte, err error) {
	setType := &setType{}
	if setType.elementType, source, err = ReadDataType(source, version); err != nil {
		return nil, source, fmt.Errorf("cannot read set element type: %w", err)
	}
	return setType, source, nil
}
