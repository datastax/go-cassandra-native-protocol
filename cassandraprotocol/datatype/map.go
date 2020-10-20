package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type mapType struct {
	keyType   DataType
	valueType DataType
}

func (t *mapType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeMap
}

func (t *mapType) String() string {
	return fmt.Sprintf("map<%v,%v>", t.keyType, t.valueType)
}

func (t *mapType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func NewMapType(keyType DataType, valueType DataType) DataType {
	return &mapType{keyType: keyType, valueType: valueType}
}

type mapTypeCodec struct{}

func (c *mapTypeCodec) Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	mapType, ok := t.(*mapType)
	if !ok {
		return errors.New(fmt.Sprintf("expected mapType struct, got %T", t))
	} else if err = WriteDataType(mapType.keyType, dest, version); err != nil {
		return fmt.Errorf("cannot write map key type: %w", err)
	} else if err = WriteDataType(mapType.valueType, dest, version); err != nil {
		return fmt.Errorf("cannot write map value type: %w", err)
	}
	return nil
}

func (c *mapTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	mapType, ok := t.(*mapType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected mapType struct, got %T", t))
	}
	if keyLength, err := LengthOfDataType(mapType.keyType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of map key type: %w", err)
	} else {
		length += keyLength
	}
	if valueLength, err := LengthOfDataType(mapType.valueType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of map value type: %w", err)
	} else {
		length += valueLength
	}
	return length, nil
}

func (c *mapTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	mapType := &mapType{}
	if mapType.keyType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read map key type: %w", err)
	} else if mapType.valueType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read map value type: %w", err)
	}
	return mapType, nil
}
