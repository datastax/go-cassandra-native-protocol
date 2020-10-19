package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type userDefinedType struct {
	keyspace   string
	table      string
	fieldTypes map[string]DataType
}

func (t *userDefinedType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeUdt
}

func (t *userDefinedType) String() string {
	return fmt.Sprintf("%v.%v<%v>", t.keyspace, t.table, t.fieldTypes)
}

type userDefinedTypeCodec struct{}

func (c *userDefinedTypeCodec) Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	userDefinedType, ok := t.(*userDefinedType)
	if !ok {
		return errors.New(fmt.Sprintf("expected userDefinedType struct, got %T", t))
	} else if err = primitives.WriteString(userDefinedType.keyspace, dest); err != nil {
		return fmt.Errorf("cannot write udt keyspace: %w", err)
	} else if err = primitives.WriteString(userDefinedType.table, dest); err != nil {
		return fmt.Errorf("cannot write udt table: %w", err)
	} else if err = primitives.WriteShort(uint16(len(userDefinedType.fieldTypes)), dest); err != nil {
		return fmt.Errorf("cannot write udt field count: %w", err)
	}
	for fieldName, fieldType := range userDefinedType.fieldTypes {
		if err = primitives.WriteString(fieldName, dest); err != nil {
			return fmt.Errorf("cannot write udt field %v name: %w", fieldName, err)
		} else if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write udt field %v: %w", fieldName, err)
		}
	}
	return nil
}

func (c *userDefinedTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	userDefinedType, ok := t.(*userDefinedType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected userDefinedType struct, got %T", t))
	}
	length += primitives.LengthOfString(userDefinedType.keyspace)
	length += primitives.LengthOfString(userDefinedType.table)
	length += primitives.LengthOfShort // field count
	for fieldName, fieldType := range userDefinedType.fieldTypes {
		length += primitives.LengthOfString(fieldName)
		if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of udt field %v: %w", fieldName, err)
		} else {
			length += fieldLength
		}
	}
	return length, nil
}

func (c *userDefinedTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	userDefinedType := &userDefinedType{}
	if userDefinedType.keyspace, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt keyspace: %w", err)
	} else if userDefinedType.table, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt table: %w", err)
	} else if fieldCount, err := primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read udt field count: %w", err)
	} else {
		userDefinedType.fieldTypes = make(map[string]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if fieldName, err := primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d name: %w", i, err)
			} else if userDefinedType.fieldTypes[fieldName], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read udt field %v: %w", fieldName, err)
			}
		}
		return userDefinedType, nil
	}
}
