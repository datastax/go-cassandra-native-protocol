package datatype

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrimitiveType(t *testing.T) {
	tests := []struct {
		name     string
		input    PrimitiveType
		expected primitives.DataTypeCode
	}{
		{"Ascii", Ascii, primitives.DataTypeCodeAscii},
		{"Bigint", Bigint, primitives.DataTypeCodeBigint},
		{"Blob", Blob, primitives.DataTypeCodeBlob},
		{"Boolean", Boolean, primitives.DataTypeCodeBoolean},
		{"Counter", Counter, primitives.DataTypeCodeCounter},
		{"Decimal", Decimal, primitives.DataTypeCodeDecimal},
		{"Double", Double, primitives.DataTypeCodeDouble},
		{"Float", Float, primitives.DataTypeCodeFloat},
		{"Int", Int, primitives.DataTypeCodeInt},
		{"Timestamp", Timestamp, primitives.DataTypeCodeTimestamp},
		{"Uuid", Uuid, primitives.DataTypeCodeUuid},
		{"Varchar", Varchar, primitives.DataTypeCodeVarchar},
		{"Varint", Varint, primitives.DataTypeCodeVarint},
		{"Timeuuid", Timeuuid, primitives.DataTypeCodeTimeuuid},
		{"Inet", Inet, primitives.DataTypeCodeInet},
		{"Date", Date, primitives.DataTypeCodeDate},
		{"Time", Time, primitives.DataTypeCodeTime},
		{"Smallint", Smallint, primitives.DataTypeCodeSmallint},
		{"Tinyint", Tinyint, primitives.DataTypeCodeTinyint},
		{"Duration", Duration, primitives.DataTypeCodeDuration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input.GetDataTypeCode()
			assert.Equal(t, test.expected, actual)
		})
	}
}
