package datatype

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

var (
	Ascii     PrimitiveType = &primitiveType{code: primitives.DataTypeCodeAscii}
	Bigint    PrimitiveType = &primitiveType{code: primitives.DataTypeCodeBigint}
	Blob      PrimitiveType = &primitiveType{code: primitives.DataTypeCodeBlob}
	Boolean   PrimitiveType = &primitiveType{code: primitives.DataTypeCodeBoolean}
	Counter   PrimitiveType = &primitiveType{code: primitives.DataTypeCodeCounter}
	Decimal   PrimitiveType = &primitiveType{code: primitives.DataTypeCodeDecimal}
	Double    PrimitiveType = &primitiveType{code: primitives.DataTypeCodeDouble}
	Float     PrimitiveType = &primitiveType{code: primitives.DataTypeCodeFloat}
	Int       PrimitiveType = &primitiveType{code: primitives.DataTypeCodeInt}
	Timestamp PrimitiveType = &primitiveType{code: primitives.DataTypeCodeTimestamp}
	Uuid      PrimitiveType = &primitiveType{code: primitives.DataTypeCodeUuid}
	Varchar   PrimitiveType = &primitiveType{code: primitives.DataTypeCodeVarchar}
	Varint    PrimitiveType = &primitiveType{code: primitives.DataTypeCodeVarint}
	Timeuuid  PrimitiveType = &primitiveType{code: primitives.DataTypeCodeTimeuuid}
	Inet      PrimitiveType = &primitiveType{code: primitives.DataTypeCodeInet}
	Date      PrimitiveType = &primitiveType{code: primitives.DataTypeCodeDate}
	Time      PrimitiveType = &primitiveType{code: primitives.DataTypeCodeTime}
	Smallint  PrimitiveType = &primitiveType{code: primitives.DataTypeCodeSmallint}
	Tinyint   PrimitiveType = &primitiveType{code: primitives.DataTypeCodeTinyint}
	Duration  PrimitiveType = &primitiveType{code: primitives.DataTypeCodeDuration}
)

type PrimitiveType interface {
	DataType
}

type primitiveType struct {
	code primitives.DataTypeCode
}

func (t *primitiveType) GetDataTypeCode() primitives.DataTypeCode {
	return t.code
}

func (t *primitiveType) String() string {
	switch t.GetDataTypeCode() {
	case primitives.DataTypeCodeAscii:
		return "ascii"
	case primitives.DataTypeCodeBigint:
		return "bigint"
	case primitives.DataTypeCodeBlob:
		return "blob"
	case primitives.DataTypeCodeBoolean:
		return "boolean"
	case primitives.DataTypeCodeCounter:
		return "counter"
	case primitives.DataTypeCodeDecimal:
		return "decimal"
	case primitives.DataTypeCodeDouble:
		return "double"
	case primitives.DataTypeCodeFloat:
		return "float"
	case primitives.DataTypeCodeInt:
		return "int"
	case primitives.DataTypeCodeTimestamp:
		return "timestamp"
	case primitives.DataTypeCodeUuid:
		return "uuid"
	case primitives.DataTypeCodeVarchar:
		return "varchar"
	case primitives.DataTypeCodeVarint:
		return "varint"
	case primitives.DataTypeCodeTimeuuid:
		return "timeuuid"
	case primitives.DataTypeCodeInet:
		return "inet"
	case primitives.DataTypeCodeDate:
		return "date"
	case primitives.DataTypeCodeTime:
		return "tune"
	case primitives.DataTypeCodeSmallint:
		return "smallint"
	case primitives.DataTypeCodeTinyint:
		return "tinyint"
	case primitives.DataTypeCodeDuration:
		return "duration"
	}
	return "?"
}

func (t *primitiveType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type primitiveTypeCodec struct {
	primitiveType PrimitiveType
}

func (c *primitiveTypeCodec) encode(t DataType, _ io.Writer, version primitives.ProtocolVersion) (err error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := primitives.CheckPrimitiveDataTypeCode(t.GetDataTypeCode()); err != nil {
		return err
	}
	if version < primitives.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == primitives.DataTypeCodeDuration {
		return fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return nil
}

func (c *primitiveTypeCodec) encodedLength(t DataType, _ primitives.ProtocolVersion) (int, error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := primitives.CheckPrimitiveDataTypeCode(t.GetDataTypeCode()); err != nil {
		return -1, err
	}
	return 0, nil
}

func (c *primitiveTypeCodec) decode(_ io.Reader, version primitives.ProtocolVersion) (t DataType, err error) {
	if version < primitives.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == primitives.DataTypeCodeDuration {
		return nil, fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return c.primitiveType, nil
}
