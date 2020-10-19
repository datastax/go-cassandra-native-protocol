package datatype

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type DataType interface {
	GetDataTypeCode() cassandraprotocol.DataTypeCode
}

type Encoder interface {
	Encode(t DataType, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error)
	EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error)
}

type Decoder interface {
	Decode(source []byte, version cassandraprotocol.ProtocolVersion) (t DataType, remaining []byte, err error)
}

type Codec interface {
	Encoder
	Decoder
}

func WriteDataType(t DataType, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	if dest, err = primitives.WriteShort(t.GetDataTypeCode(), dest); err != nil {
		return dest, fmt.Errorf("cannot write data type code %v: %w", t.GetDataTypeCode(), err)
	} else if codec, found := codecs[t.GetDataTypeCode()]; !found {
		return dest, fmt.Errorf("cannot find codec for data type %v", t)
	} else if dest, err = codec.Encode(t, dest, version); err != nil {
		return dest, fmt.Errorf("cannot write data type %v: %w", t, err)
	} else {
		return dest, nil
	}
}

func LengthOfDataType(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfShort // type code
	if codec, found := codecs[t.GetDataTypeCode()]; !found {
		return -1, fmt.Errorf("cannot find codec for data type %v", t)
	} else if dataTypeLength, err := codec.EncodedLength(t, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of data type %v: %w", t, err)
	} else {
		return length + dataTypeLength, nil
	}
}

func ReadDataType(source []byte, version cassandraprotocol.ProtocolVersion) (decoded DataType, remaining []byte, err error) {
	var typeCode cassandraprotocol.DataTypeCode
	if typeCode, source, err = primitives.ReadShort(source); err != nil {
		return nil, source, fmt.Errorf("cannot read data type code: %w", err)
	} else if codec, found := codecs[typeCode]; !found {
		return nil, source, fmt.Errorf("cannot find codec for type code %v", typeCode)
	} else if decoded, source, err = codec.Decode(source, version); err != nil {
		return nil, source, fmt.Errorf("cannot read data type code %v: %w", typeCode, err)
	} else {
		return decoded, source, nil
	}
}

var primitiveTypes = map[cassandraprotocol.DataTypeCode]*primitiveType{
	cassandraprotocol.DataTypeCodeAscii:     {cassandraprotocol.DataTypeCodeAscii},
	cassandraprotocol.DataTypeCodeBigint:    {cassandraprotocol.DataTypeCodeBigint},
	cassandraprotocol.DataTypeCodeBlob:      {cassandraprotocol.DataTypeCodeBlob},
	cassandraprotocol.DataTypeCodeBoolean:   {cassandraprotocol.DataTypeCodeBoolean},
	cassandraprotocol.DataTypeCodeCounter:   {cassandraprotocol.DataTypeCodeCounter},
	cassandraprotocol.DataTypeCodeDecimal:   {cassandraprotocol.DataTypeCodeDecimal},
	cassandraprotocol.DataTypeCodeDouble:    {cassandraprotocol.DataTypeCodeDouble},
	cassandraprotocol.DataTypeCodeFloat:     {cassandraprotocol.DataTypeCodeFloat},
	cassandraprotocol.DataTypeCodeInt:       {cassandraprotocol.DataTypeCodeInt},
	cassandraprotocol.DataTypeCodeTimestamp: {cassandraprotocol.DataTypeCodeTimestamp},
	cassandraprotocol.DataTypeCodeUuid:      {cassandraprotocol.DataTypeCodeUuid},
	cassandraprotocol.DataTypeCodeVarchar:   {cassandraprotocol.DataTypeCodeVarchar},
	cassandraprotocol.DataTypeCodeVarint:    {cassandraprotocol.DataTypeCodeVarint},
	cassandraprotocol.DataTypeCodeTimeuuid:  {cassandraprotocol.DataTypeCodeTimeuuid},
	cassandraprotocol.DataTypeCodeInet:      {cassandraprotocol.DataTypeCodeInet},
	cassandraprotocol.DataTypeCodeDate:      {cassandraprotocol.DataTypeCodeDate},
	cassandraprotocol.DataTypeCodeTime:      {cassandraprotocol.DataTypeCodeTime},
	cassandraprotocol.DataTypeCodeSmallint:  {cassandraprotocol.DataTypeCodeSmallint},
	cassandraprotocol.DataTypeCodeTinyint:   {cassandraprotocol.DataTypeCodeTinyint},
	cassandraprotocol.DataTypeCodeDuration:  {cassandraprotocol.DataTypeCodeDuration},
}

var codecs = map[cassandraprotocol.DataTypeCode]Codec{
	cassandraprotocol.DataTypeCodeAscii:     &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeAscii]},
	cassandraprotocol.DataTypeCodeBigint:    &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeBigint]},
	cassandraprotocol.DataTypeCodeBlob:      &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeBlob]},
	cassandraprotocol.DataTypeCodeBoolean:   &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeBoolean]},
	cassandraprotocol.DataTypeCodeCounter:   &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeCounter]},
	cassandraprotocol.DataTypeCodeDecimal:   &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeDecimal]},
	cassandraprotocol.DataTypeCodeDouble:    &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeDouble]},
	cassandraprotocol.DataTypeCodeFloat:     &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeFloat]},
	cassandraprotocol.DataTypeCodeInt:       &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeInt]},
	cassandraprotocol.DataTypeCodeTimestamp: &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeTimestamp]},
	cassandraprotocol.DataTypeCodeUuid:      &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeUuid]},
	cassandraprotocol.DataTypeCodeVarchar:   &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeVarchar]},
	cassandraprotocol.DataTypeCodeVarint:    &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeVarint]},
	cassandraprotocol.DataTypeCodeTimeuuid:  &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeTimeuuid]},
	cassandraprotocol.DataTypeCodeInet:      &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeInet]},
	cassandraprotocol.DataTypeCodeDate:      &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeDate]},
	cassandraprotocol.DataTypeCodeTime:      &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeTime]},
	cassandraprotocol.DataTypeCodeSmallint:  &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeSmallint]},
	cassandraprotocol.DataTypeCodeTinyint:   &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeTinyint]},
	cassandraprotocol.DataTypeCodeDuration:  &primitiveTypeCodec{primitiveTypes[cassandraprotocol.DataTypeCodeDuration]},
	cassandraprotocol.DataTypeCodeList:      &listTypeCodec{},
	cassandraprotocol.DataTypeCodeSet:       &setTypeCodec{},
	cassandraprotocol.DataTypeCodeMap:       &mapTypeCodec{},
	cassandraprotocol.DataTypeCodeTuple:     &tupleTypeCodec{},
	cassandraprotocol.DataTypeCodeUdt:       &userDefinedTypeCodec{},
	cassandraprotocol.DataTypeCodeCustom:    &customTypeCodec{},
}
