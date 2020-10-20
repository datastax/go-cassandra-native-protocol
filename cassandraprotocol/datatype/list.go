package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type listType struct {
	elementType DataType
}

func NewListType(elementType DataType) DataType {
	return &listType{elementType: elementType}
}

func (t *listType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeList
}

func (t *listType) String() string {
	return fmt.Sprintf("list<%v>", t.elementType)
}

func (t *listType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type listTypeCodec struct{}

func (c *listTypeCodec) Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if listType, ok := t.(*listType); !ok {
		return errors.New(fmt.Sprintf("expected listType struct, got %T", t))
	} else if err = WriteDataType(listType.elementType, dest, version); err != nil {
		return fmt.Errorf("cannot write list element type: %w", err)
	}
	return nil
}

func (c *listTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	if listType, ok := t.(*listType); !ok {
		return -1, errors.New(fmt.Sprintf("expected listType struct, got %T", t))
	} else if elementLength, err := LengthOfDataType(listType.elementType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of list element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func (c *listTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	listType := &listType{}
	if listType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read list element type: %w", err)
	}
	return listType, nil
}
