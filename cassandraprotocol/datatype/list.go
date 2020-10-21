package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type ListType interface {
	DataType
	GetElementType() DataType
}

type listType struct {
	elementType DataType
}

func (t *listType) GetElementType() DataType {
	return t.elementType
}

func NewListType(elementType DataType) ListType {
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

func (c *listTypeCodec) encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if listType, ok := t.(ListType); !ok {
		return errors.New(fmt.Sprintf("expected ListType, got %T", t))
	} else if err = WriteDataType(listType.GetElementType(), dest, version); err != nil {
		return fmt.Errorf("cannot write list element type: %w", err)
	}
	return nil
}

func (c *listTypeCodec) encodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	if listType, ok := t.(ListType); !ok {
		return -1, errors.New(fmt.Sprintf("expected ListType, got %T", t))
	} else if elementLength, err := LengthOfDataType(listType.GetElementType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of list element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func (c *listTypeCodec) decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	listType := &listType{}
	if listType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read list element type: %w", err)
	}
	return listType, nil
}
