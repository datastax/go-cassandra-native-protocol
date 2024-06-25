// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// ColumnMetadata represents a column in a PreparedResult message.
// +k8s:deepcopy-gen=true
type ColumnMetadata struct {
	Keyspace string
	Table    string
	Name     string
	Index    int32
	Type     datatype.DataType
}

// VariablesMetadata is used in PreparedResult to indicate metadata about the prepared statement's bound variables.
// +k8s:deepcopy-gen=true
type VariablesMetadata struct {
	// The indices of variables belonging to the table's partition key, if any. Valid from protocol version 4 onwards;
	// will be nil for protocol versions lesser than 4.
	PkIndices []uint16
	Columns   []*ColumnMetadata
}

func (rm *VariablesMetadata) Flags() (flag primitive.VariablesFlag) {
	if len(rm.Columns) > 0 && haveSameTable(rm.Columns) {
		flag |= primitive.VariablesFlagGlobalTablesSpec
	}
	return flag
}

// RowsMetadata is used in RowsResult to indicate metadata about the result set present in the result response;
// and in PreparedResult, to indicate metadata about the result set that the prepared statement will produce once
// executed.
// +k8s:deepcopy-gen=true
type RowsMetadata struct {
	// Must be always present, even when Columns is nil. If Columns is non-nil, the value of ColumnCount must match
	// len(Columns), otherwise an error is returned when encoding.
	ColumnCount int32
	// PagingState is a [bytes] value. If provided, this means that this page of results is not the last page..
	PagingState []byte
	// Valid for protocol version 5 and DSE protocol version 2 only.
	NewResultMetadataId []byte
	// Valid for DSE protocol versions only.
	ContinuousPageNumber int32
	// Valid for DSE protocol versions only.
	LastContinuousPage bool
	// If nil, the NO_METADATA flag is set. In a PreparedResult, will be non-nil if the statement is a SELECT.
	Columns []*ColumnMetadata
}

func (rm *RowsMetadata) Flags() (flag primitive.RowsFlag) {
	if len(rm.Columns) == 0 {
		flag |= primitive.RowsFlagNoMetadata
	} else if haveSameTable(rm.Columns) {
		flag |= primitive.RowsFlagGlobalTablesSpec
	}
	if rm.PagingState != nil {
		flag |= primitive.RowsFlagHasMorePages
	}
	if rm.NewResultMetadataId != nil {
		flag |= primitive.RowsFlagMetadataChanged
	}
	if rm.ContinuousPageNumber > 0 {
		flag |= primitive.RowsFlagDseContinuousPaging
		if rm.LastContinuousPage {
			flag |= primitive.RowsFlagDseLastContinuousPage
		}
	}
	return flag
}

func encodeVariablesMetadata(metadata *VariablesMetadata, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if metadata == nil {
		metadata = &VariablesMetadata{}
	}
	flags := metadata.Flags()
	if err = primitive.WriteInt(int32(flags), dest); err != nil {
		return fmt.Errorf("cannot write RESULT Prepared variables metadata flags: %w", err)
	}
	if err = primitive.WriteInt(int32(len(metadata.Columns)), dest); err != nil {
		return fmt.Errorf("cannot write RESULT Prepared variables metadata column count: %w", err)
	}
	if version >= primitive.ProtocolVersion4 {
		if err = primitive.WriteInt(int32(len(metadata.PkIndices)), dest); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared variables metadata pk indices length: %w", err)
		}
		for i, idx := range metadata.PkIndices {
			if err = primitive.WriteShort(idx, dest); err != nil {
				return fmt.Errorf("cannot write RESULT Prepared variables metadata pk indices element %d: %w", i, err)
			}
		}
	}
	if len(metadata.Columns) > 0 {
		globalTableSpec := flags.Contains(primitive.VariablesFlagGlobalTablesSpec)
		if err = encodeColumnsMetadata(globalTableSpec, metadata.Columns, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared variables metadata column cols: %w", err)
		}
	}
	return nil
}

func lengthOfVariablesMetadata(metadata *VariablesMetadata, version primitive.ProtocolVersion) (length int, err error) {
	if metadata == nil {
		metadata = &VariablesMetadata{}
	}
	length += primitive.LengthOfInt // flags
	length += primitive.LengthOfInt // column count
	if version >= primitive.ProtocolVersion4 {
		length += primitive.LengthOfInt // pk count
		length += primitive.LengthOfShort * len(metadata.PkIndices)
	}
	if len(metadata.Columns) > 0 {
		globalTableSpec := metadata.Flags()&primitive.VariablesFlagGlobalTablesSpec > 0
		var lcs int
		if lcs, err = lengthOfColumnsMetadata(globalTableSpec, metadata.Columns, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT Prepared variables metadata column cols: %w", err)
		}
		length += lcs
	}
	return length, nil
}

func decodeVariablesMetadata(source io.Reader, version primitive.ProtocolVersion) (metadata *VariablesMetadata, err error) {
	metadata = &VariablesMetadata{}
	var f int32
	if f, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata flags: %w", err)
	}
	var flags = primitive.VariablesFlag(f)
	var columnCount int32
	if columnCount, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata column count: %w", err)
	}
	if version >= primitive.ProtocolVersion4 {
		var pkCount int32
		if pkCount, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata pk indices length: %w", err)
		}
		if pkCount > 0 {
			metadata.PkIndices = make([]uint16, pkCount)
			for i := 0; i < int(pkCount); i++ {
				if metadata.PkIndices[i], err = primitive.ReadShort(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata pk index element %d: %w", i, err)
				}
			}
		}
	}
	if columnCount > 0 {
		globalTableSpec := flags.Contains(primitive.VariablesFlagGlobalTablesSpec)
		if metadata.Columns, err = decodeColumnsMetadata(globalTableSpec, columnCount, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata column cols: %w", err)
		}
	}
	return metadata, nil
}

func encodeRowsMetadata(metadata *RowsMetadata, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if metadata == nil {
		metadata = &RowsMetadata{}
	}
	flags := metadata.Flags()
	if err = primitive.WriteInt(int32(flags), dest); err != nil {
		return fmt.Errorf("cannot write RESULT Rows metadata flags: %w", err)
	}
	columnSpecsLength := len(metadata.Columns)
	if columnSpecsLength > 0 && int(metadata.ColumnCount) != columnSpecsLength {
		return fmt.Errorf(
			"invalid RESULT Rows metadata: metadata.ColumnCount %d != len(metadata.ColumnSpecs) %d",
			metadata.ColumnCount,
			columnSpecsLength,
		)
	}
	if err = primitive.WriteInt(metadata.ColumnCount, dest); err != nil {
		return fmt.Errorf("cannot write RESULT Rows metadata column count: %w", err)
	}
	if flags.Contains(primitive.RowsFlagHasMorePages) {
		if err = primitive.WriteBytes(metadata.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata paging state: %w", err)
		}
	}
	if flags.Contains(primitive.RowsFlagMetadataChanged) {
		if err = primitive.WriteShortBytes(metadata.NewResultMetadataId, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata new result metadata id: %w", err)
		}
	}
	if flags.Contains(primitive.RowsFlagDseContinuousPaging) {
		if err = primitive.WriteInt(metadata.ContinuousPageNumber, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata continuous page number: %w", err)
		}
	}
	if flags&primitive.RowsFlagNoMetadata == 0 && columnSpecsLength > 0 {
		globalTableSpec := flags.Contains(primitive.RowsFlagGlobalTablesSpec)
		if err = encodeColumnsMetadata(globalTableSpec, metadata.Columns, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata column specs: %w", err)
		}
	}
	return nil
}

func lengthOfRowsMetadata(metadata *RowsMetadata, version primitive.ProtocolVersion) (length int, err error) {
	if metadata == nil {
		metadata = &RowsMetadata{}
	}
	length += primitive.LengthOfInt // flags
	length += primitive.LengthOfInt // column count
	flags := metadata.Flags()
	if flags.Contains(primitive.RowsFlagHasMorePages) {
		length += primitive.LengthOfBytes(metadata.PagingState)
	}
	if flags.Contains(primitive.RowsFlagMetadataChanged) {
		length += primitive.LengthOfShortBytes(metadata.NewResultMetadataId)
	}
	if flags.Contains(primitive.RowsFlagDseContinuousPaging) {
		length += primitive.LengthOfInt // continuous page number
	}
	if flags&primitive.RowsFlagNoMetadata == 0 && len(metadata.Columns) > 0 {
		globalTableSpec := flags.Contains(primitive.RowsFlagGlobalTablesSpec)
		var lengthOfCols int
		if lengthOfCols, err = lengthOfColumnsMetadata(globalTableSpec, metadata.Columns, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT Rows metadata column cols: %w", err)
		}
		length += lengthOfCols
	}
	return length, nil
}

func decodeRowsMetadata(source io.Reader, version primitive.ProtocolVersion) (metadata *RowsMetadata, err error) {
	metadata = &RowsMetadata{}
	var f int32
	if f, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Rows metadata flags: %w", err)
	}
	var flags = primitive.RowsFlag(f)
	if metadata.ColumnCount, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Rows metadata column count: %w", err)
	}
	if flags.Contains(primitive.RowsFlagHasMorePages) {
		if metadata.PagingState, err = primitive.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata paging state: %w", err)
		}
	}
	if flags.Contains(primitive.RowsFlagMetadataChanged) {
		if metadata.NewResultMetadataId, err = primitive.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata new result metadata id: %w", err)
		}
	}
	if flags.Contains(primitive.RowsFlagDseContinuousPaging) {
		if metadata.ContinuousPageNumber, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata continuous paging number: %w", err)
		}
		metadata.LastContinuousPage = flags.Contains(primitive.RowsFlagDseLastContinuousPage)
	}
	if flags&primitive.RowsFlagNoMetadata == 0 {
		globalTableSpec := flags.Contains(primitive.RowsFlagGlobalTablesSpec)
		if metadata.Columns, err = decodeColumnsMetadata(globalTableSpec, metadata.ColumnCount, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata column cols: %w", err)
		}
	}
	return metadata, nil
}

func encodeColumnsMetadata(globalTableSpec bool, cols []*ColumnMetadata, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if globalTableSpec {
		firstCol := cols[0]
		if err = primitive.WriteString(firstCol.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write column col global keyspace: %w", err)
		}
		if err = primitive.WriteString(firstCol.Table, dest); err != nil {
			return fmt.Errorf("cannot write column col global table: %w", err)
		}
	}
	for i, col := range cols {
		if !globalTableSpec {
			if err = primitive.WriteString(col.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write column col %d keyspace: %w", i, err)
			}
			if err = primitive.WriteString(col.Table, dest); err != nil {
				return fmt.Errorf("cannot write column col %d table: %w", i, err)
			}
		}
		if err = primitive.WriteString(col.Name, dest); err != nil {
			return fmt.Errorf("cannot write column col %d name: %w", i, err)
		}
		if err = datatype.WriteDataType(col.Type, dest, version); err != nil {
			return fmt.Errorf("cannot write column col %d type: %w", i, err)
		}
	}
	return nil
}

func lengthOfColumnsMetadata(globalTableSpec bool, cols []*ColumnMetadata, version primitive.ProtocolVersion) (length int, err error) {
	if globalTableSpec {
		firstCol := cols[0]
		length += primitive.LengthOfString(firstCol.Keyspace)
		length += primitive.LengthOfString(firstCol.Table)
	}
	for i, col := range cols {
		if !globalTableSpec {
			length += primitive.LengthOfString(col.Keyspace)
			length += primitive.LengthOfString(col.Table)
		}
		length += primitive.LengthOfString(col.Name)
		if lengthOfDataType, err := datatype.LengthOfDataType(col.Type, version); err != nil {
			return -1, fmt.Errorf("cannot compute length column col %d type: %w", i, err)
		} else {
			length += lengthOfDataType
		}
	}
	return
}

func decodeColumnsMetadata(globalTableSpec bool, columnCount int32, source io.Reader, version primitive.ProtocolVersion) (cols []*ColumnMetadata, err error) {
	var globalKsName string
	var globalTableName string
	if globalTableSpec {
		if globalKsName, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read column col global keyspace: %w", err)
		}
		if globalTableName, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read column col global table: %w", err)
		}
	}
	cols = make([]*ColumnMetadata, columnCount)
	for i := 0; i < int(columnCount); i++ {
		cols[i] = &ColumnMetadata{}
		if globalTableSpec {
			cols[i].Keyspace = globalKsName
		} else {
			if cols[i].Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read column col %d keyspace: %w", i, err)
			}
		}
		if globalTableSpec {
			cols[i].Table = globalTableName
		} else {
			if cols[i].Table, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read column col %d table: %w", i, err)
			}
		}
		if cols[i].Name, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read column col %d name: %w", i, err)
		}
		if cols[i].Type, err = datatype.ReadDataType(source, version); err != nil {
			return nil, fmt.Errorf("cannot read column col %d type: %w", i, err)
		}
	}
	return cols, nil
}

func haveSameTable(cols []*ColumnMetadata) bool {
	if cols == nil || len(cols) == 0 {
		return false
	}
	first := true
	var ksName string
	var tableName string
	for _, col := range cols {
		if first {
			first = false
			ksName = col.Keyspace
			tableName = col.Table
		} else if col.Keyspace != ksName || col.Table != tableName {
			return false
		}
	}
	return true
}
