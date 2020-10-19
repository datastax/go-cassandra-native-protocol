package datatype

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type DataType interface {
	GetDataTypeCode() cassandraprotocol.DataTypeCode
}

type Encoder interface {
	Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error)
	EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error)
}

type Decoder interface {
	Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (t DataType, err error)
}

type Codec interface {
	Encoder
	Decoder
}

func WriteDataType(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if err = primitives.WriteShort(t.GetDataTypeCode(), dest); err != nil {
		return fmt.Errorf("cannot write data type code %v: %w", t.GetDataTypeCode(), err)
	} else if codec, found := codecs[t.GetDataTypeCode()]; !found {
		return fmt.Errorf("cannot find codec for data type %v", t)
	} else if err = codec.Encode(t, dest, version); err != nil {
		return fmt.Errorf("cannot write data type %v: %w", t, err)
	} else {
		return nil
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

func ReadDataType(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	var typeCode cassandraprotocol.DataTypeCode
	if typeCode, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read data type code: %w", err)
	} else if codec, found := codecs[typeCode]; !found {
		return nil, fmt.Errorf("cannot find codec for type code %v", typeCode)
	} else if decoded, err = codec.Decode(source, version); err != nil {
		return nil, fmt.Errorf("cannot read data type code %v: %w", typeCode, err)
	} else {
		return decoded, nil
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
