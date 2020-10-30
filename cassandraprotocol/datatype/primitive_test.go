package datatype

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrimitiveType(t *testing.T) {
	tests := []struct {
		name     string
		input    PrimitiveType
		expected primitive.DataTypeCode
	}{
		{"Ascii", Ascii, primitive.DataTypeCodeAscii},
		{"Bigint", Bigint, primitive.DataTypeCodeBigint},
		{"Blob", Blob, primitive.DataTypeCodeBlob},
		{"Boolean", Boolean, primitive.DataTypeCodeBoolean},
		{"Counter", Counter, primitive.DataTypeCodeCounter},
		{"Decimal", Decimal, primitive.DataTypeCodeDecimal},
		{"Double", Double, primitive.DataTypeCodeDouble},
		{"Float", Float, primitive.DataTypeCodeFloat},
		{"Int", Int, primitive.DataTypeCodeInt},
		{"Timestamp", Timestamp, primitive.DataTypeCodeTimestamp},
		{"Uuid", Uuid, primitive.DataTypeCodeUuid},
		{"Varchar", Varchar, primitive.DataTypeCodeVarchar},
		{"Varint", Varint, primitive.DataTypeCodeVarint},
		{"Timeuuid", Timeuuid, primitive.DataTypeCodeTimeuuid},
		{"Inet", Inet, primitive.DataTypeCodeInet},
		{"Date", Date, primitive.DataTypeCodeDate},
		{"Time", Time, primitive.DataTypeCodeTime},
		{"Smallint", Smallint, primitive.DataTypeCodeSmallint},
		{"Tinyint", Tinyint, primitive.DataTypeCodeTinyint},
		{"Duration", Duration, primitive.DataTypeCodeDuration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input.GetDataTypeCode()
			assert.Equal(t, test.expected, actual)
		})
	}
}
