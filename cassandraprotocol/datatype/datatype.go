package datatype

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type DataType interface {
	GetDataTypeCode() primitives.DataTypeCode
}

type encoder interface {
	encode(t DataType, dest io.Writer, version primitives.ProtocolVersion) (err error)
	encodedLength(t DataType, version primitives.ProtocolVersion) (length int, err error)
}

type decoder interface {
	decode(source io.Reader, version primitives.ProtocolVersion) (t DataType, err error)
}

type codec interface {
	encoder
	decoder
}

func WriteDataType(t DataType, dest io.Writer, version primitives.ProtocolVersion) (err error) {
	if err = primitives.WriteShort(t.GetDataTypeCode(), dest); err != nil {
		return fmt.Errorf("cannot write data type code %v: %w", t.GetDataTypeCode(), err)
	} else if codec, err := findCodec(t.GetDataTypeCode()); err != nil {
		return err
	} else if err = codec.encode(t, dest, version); err != nil {
		return fmt.Errorf("cannot write data type %v: %w", t, err)
	} else {
		return nil
	}
}

func LengthOfDataType(t DataType, version primitives.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfShort // type code
	if codec, err := findCodec(t.GetDataTypeCode()); err != nil {
		return -1, err
	} else if dataTypeLength, err := codec.encodedLength(t, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of data type %v: %w", t, err)
	} else {
		return length + dataTypeLength, nil
	}
}

func ReadDataType(source io.Reader, version primitives.ProtocolVersion) (decoded DataType, err error) {
	var typeCode primitives.DataTypeCode
	if typeCode, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read data type code: %w", err)
	} else if codec, err := findCodec(typeCode); err != nil {
		return nil, err
	} else if decoded, err = codec.decode(source, version); err != nil {
		return nil, fmt.Errorf("cannot read data type code %v: %w", typeCode, err)
	} else {
		return decoded, nil
	}
}

var codecs = map[primitives.DataTypeCode]codec{
	primitives.DataTypeCodeAscii:     &primitiveTypeCodec{Ascii},
	primitives.DataTypeCodeBigint:    &primitiveTypeCodec{Bigint},
	primitives.DataTypeCodeBlob:      &primitiveTypeCodec{Blob},
	primitives.DataTypeCodeBoolean:   &primitiveTypeCodec{Boolean},
	primitives.DataTypeCodeCounter:   &primitiveTypeCodec{Counter},
	primitives.DataTypeCodeDecimal:   &primitiveTypeCodec{Decimal},
	primitives.DataTypeCodeDouble:    &primitiveTypeCodec{Double},
	primitives.DataTypeCodeFloat:     &primitiveTypeCodec{Float},
	primitives.DataTypeCodeInt:       &primitiveTypeCodec{Int},
	primitives.DataTypeCodeTimestamp: &primitiveTypeCodec{Timestamp},
	primitives.DataTypeCodeUuid:      &primitiveTypeCodec{Uuid},
	primitives.DataTypeCodeVarchar:   &primitiveTypeCodec{Varchar},
	primitives.DataTypeCodeVarint:    &primitiveTypeCodec{Varint},
	primitives.DataTypeCodeTimeuuid:  &primitiveTypeCodec{Timeuuid},
	primitives.DataTypeCodeInet:      &primitiveTypeCodec{Inet},
	primitives.DataTypeCodeDate:      &primitiveTypeCodec{Date},
	primitives.DataTypeCodeTime:      &primitiveTypeCodec{Time},
	primitives.DataTypeCodeSmallint:  &primitiveTypeCodec{Smallint},
	primitives.DataTypeCodeTinyint:   &primitiveTypeCodec{Tinyint},
	primitives.DataTypeCodeDuration:  &primitiveTypeCodec{Duration},
	primitives.DataTypeCodeList:      &listTypeCodec{},
	primitives.DataTypeCodeSet:       &setTypeCodec{},
	primitives.DataTypeCodeMap:       &mapTypeCodec{},
	primitives.DataTypeCodeTuple:     &tupleTypeCodec{},
	primitives.DataTypeCodeUdt:       &userDefinedTypeCodec{},
	primitives.DataTypeCodeCustom:    &customTypeCodec{},
}

func findCodec(code primitives.DataTypeCode) (codec, error) {
	codec, ok := codecs[code]
	if !ok {
		return nil, fmt.Errorf("cannot find codec for data type code %v", code)
	}
	return codec, nil
}
