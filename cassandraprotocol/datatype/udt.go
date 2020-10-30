package datatype

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type UserDefinedType interface {
	DataType
	GetKeyspace() string
	GetName() string
	// names and types are not modeled as a map because iteration order matters
	GetFieldNames() []string
	GetFieldTypes() []DataType
}

type userDefinedType struct {
	keyspace   string
	name       string
	fieldNames []string
	fieldTypes []DataType
}

func (t *userDefinedType) GetKeyspace() string {
	return t.keyspace
}

func (t *userDefinedType) GetName() string {
	return t.name
}

func (t *userDefinedType) GetFieldNames() []string {
	return t.fieldNames
}

func (t *userDefinedType) GetFieldTypes() []DataType {
	return t.fieldTypes
}

func NewUserDefinedType(keyspace string, table string, fieldNames []string, fieldTypes []DataType) UserDefinedType {
	return &userDefinedType{keyspace: keyspace, name: table, fieldNames: fieldNames, fieldTypes: fieldTypes}
}

func (t *userDefinedType) GetDataTypeCode() primitives.DataTypeCode {
	return primitives.DataTypeCodeUdt
}

func (t *userDefinedType) String() string {
	return fmt.Sprintf("%v.%v<%v>", t.keyspace, t.name, t.fieldTypes)
}

func (t *userDefinedType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type userDefinedTypeCodec struct{}

func (c *userDefinedTypeCodec) encode(t DataType, dest io.Writer, version primitives.ProtocolVersion) (err error) {
	userDefinedType, ok := t.(UserDefinedType)
	if !ok {
		return errors.New(fmt.Sprintf("expected UserDefinedType, got %T", t))
	} else if err = primitives.WriteString(userDefinedType.GetKeyspace(), dest); err != nil {
		return fmt.Errorf("cannot write udt keyspace: %w", err)
	} else if err = primitives.WriteString(userDefinedType.GetName(), dest); err != nil {
		return fmt.Errorf("cannot write udt name: %w", err)
	} else if err = primitives.WriteShort(uint16(len(userDefinedType.GetFieldTypes())), dest); err != nil {
		return fmt.Errorf("cannot write udt field count: %w", err)
	}
	if len(userDefinedType.GetFieldNames()) != len(userDefinedType.GetFieldTypes()) {
		return fmt.Errorf("invalid user-defined type: length of field names is not equal to length of field types")
	}
	for i, fieldName := range userDefinedType.GetFieldNames() {
		fieldType := userDefinedType.GetFieldTypes()[i]
		if err = primitives.WriteString(fieldName, dest); err != nil {
			return fmt.Errorf("cannot write udt field %v name: %w", fieldName, err)
		} else if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write udt field %v: %w", fieldName, err)
		}
	}
	return nil
}

func (c *userDefinedTypeCodec) encodedLength(t DataType, version primitives.ProtocolVersion) (length int, err error) {
	userDefinedType, ok := t.(UserDefinedType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected UserDefinedType, got %T", t))
	}
	length += primitives.LengthOfString(userDefinedType.GetKeyspace())
	length += primitives.LengthOfString(userDefinedType.GetName())
	length += primitives.LengthOfShort // field count
	if len(userDefinedType.GetFieldNames()) != len(userDefinedType.GetFieldTypes()) {
		return -1, fmt.Errorf("invalid user-defined type: length of field names is not equal to length of field types")
	}
	for i, fieldName := range userDefinedType.GetFieldNames() {
		fieldType := userDefinedType.GetFieldTypes()[i]
		length += primitives.LengthOfString(fieldName)
		if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of udt field %v: %w", fieldName, err)
		} else {
			length += fieldLength
		}
	}
	return length, nil
}

func (c *userDefinedTypeCodec) decode(source io.Reader, version primitives.ProtocolVersion) (decoded DataType, err error) {
	userDefinedType := &userDefinedType{}
	if userDefinedType.keyspace, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt keyspace: %w", err)
	} else if userDefinedType.name, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt name: %w", err)
	} else if fieldCount, err := primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read udt field count: %w", err)
	} else {
		userDefinedType.fieldNames = make([]string, fieldCount)
		userDefinedType.fieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if userDefinedType.fieldNames[i], err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d name: %w", i, err)
			} else if userDefinedType.fieldTypes[i], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d: %w", i, err)
			}
		}
		return userDefinedType, nil
	}
}
