package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
)

type listType struct {
	elementType DataType
}

func (t *listType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeList
}

func (t *listType) String() string {
	return fmt.Sprintf("list<%v>", t.elementType)
}

type listTypeCodec struct{}

func (c *listTypeCodec) Encode(t DataType, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	listType, ok := t.(*listType)
	if !ok {
		return dest, errors.New(fmt.Sprintf("expected listType struct, got %T", t))
	}
	if dest, err = WriteDataType(listType.elementType, dest, version); err != nil {
		return dest, fmt.Errorf("cannot write list element type: %w", err)
	}
	return dest, nil
}

func (c *listTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	listType, ok := t.(*listType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected listType struct, got %T", t))
	}
	if elementLength, err := LengthOfDataType(listType.elementType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of list element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func (c *listTypeCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (decoded DataType, remaining []byte, err error) {
	listType := &listType{}
	if listType.elementType, source, err = ReadDataType(source, version); err != nil {
		return nil, source, fmt.Errorf("cannot read list element type: %w", err)
	}
	return listType, source, nil
}
