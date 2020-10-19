package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
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

type mapTypeCodec struct{}

func (c *mapTypeCodec) Encode(t DataType, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	mapType, ok := t.(*mapType)
	if !ok {
		return dest, errors.New(fmt.Sprintf("expected mapType struct, got %T", t))
	}
	if dest, err = WriteDataType(mapType.keyType, dest, version); err != nil {
		return dest, fmt.Errorf("cannot write map key type: %w", err)
	} else if dest, err = WriteDataType(mapType.valueType, dest, version); err != nil {
		return dest, fmt.Errorf("cannot write map value type: %w", err)
	}
	return dest, nil
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

func (c *mapTypeCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (decoded DataType, remaining []byte, err error) {
	mapType := &mapType{}
	if mapType.keyType, source, err = ReadDataType(source, version); err != nil {
		return nil, source, fmt.Errorf("cannot read map key type: %w", err)
	} else if mapType.valueType, source, err = ReadDataType(source, version); err != nil {
		return nil, source, fmt.Errorf("cannot read map value type: %w", err)
	}
	return mapType, source, nil
}
