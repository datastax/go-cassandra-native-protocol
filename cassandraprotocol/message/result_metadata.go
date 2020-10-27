package message

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type VariablesMetadata struct {
	// The indices of variables belonging to the table's partition key, if any. Valid from protocol v4 onwards.
	PkIndices   []uint16
	ColumnSpecs []*ColumnMetadata
}

type VariablesMetadataCustomizer func(metadata *VariablesMetadata)

func NewVariablesMetadata(customizers ...VariablesMetadataCustomizer) *VariablesMetadata {
	metadata := &VariablesMetadata{}
	for _, customizer := range customizers {
		customizer(metadata)
	}
	return metadata
}

func WithPartitionKeyIndices(indices ...uint16) func(metadata *VariablesMetadata) {
	return func(metadata *VariablesMetadata) {
		metadata.PkIndices = indices
	}
}

func WithResultColumns(specs ...*ColumnMetadata) func(metadata *VariablesMetadata) {
	return func(metadata *VariablesMetadata) {
		metadata.ColumnSpecs = specs
	}
}

func (rm *VariablesMetadata) Flags() (flag cassandraprotocol.VariablesFlag) {
	if len(rm.ColumnSpecs) > 0 && haveSameTable(rm.ColumnSpecs) {
		flag |= cassandraprotocol.VariablesFlagGlobalTablesSpec
	}
	return flag
}

type RowsMetadata struct {
	// always present, even when ColumnSpecs is nil
	ColumnCount int32
	PagingState []byte
	// Valid for protocol version 5 and DSE protocol version 2 only.
	NewResultMetadataId []byte
	// Valid for DSE protocol versions only.
	ContinuousPageNo int32
	// Valid for DSE protocol versions only.
	LastContinuousPage bool
	// If nil, the NO_METADATA flag is set. Should never be nil in a Prepared result.
	ColumnSpecs []*ColumnMetadata
}

type RowsMetadataCustomizer func(metadata *RowsMetadata)

func NewRowsMetadata(customizers ...RowsMetadataCustomizer) *RowsMetadata {
	metadata := &RowsMetadata{}
	for _, customizer := range customizers {
		customizer(metadata)
	}
	return metadata
}

func WithColumns(specs ...*ColumnMetadata) func(metadata *RowsMetadata) {
	return func(metadata *RowsMetadata) {
		metadata.ColumnSpecs = specs
		metadata.ColumnCount = int32(len(specs))

	}
}

// Sets the column count but does not create column metadata. This activates the NO_METADATA flag.
// Only valid for Rows result, not for Prepared ones.
func NoColumnMetadata(columnCount int32) func(metadata *RowsMetadata) {
	return func(metadata *RowsMetadata) {
		metadata.ColumnCount = columnCount
	}
}

func WithResultPagingState(pagingState []byte) func(metadata *RowsMetadata) {
	return func(metadata *RowsMetadata) {
		metadata.PagingState = pagingState
	}
}

// v5+
func WithNewResultMetadataId(newResultMetadataId []byte) func(metadata *RowsMetadata) {
	return func(metadata *RowsMetadata) {
		metadata.NewResultMetadataId = newResultMetadataId
	}
}

func (rm *RowsMetadata) Flags() (flag cassandraprotocol.RowsFlag) {
	if len(rm.ColumnSpecs) == 0 {
		flag |= cassandraprotocol.RowsFlagNoMetadata
	} else if haveSameTable(rm.ColumnSpecs) {
		flag |= cassandraprotocol.RowsFlagGlobalTablesSpec
	}
	if rm.PagingState != nil {
		flag |= cassandraprotocol.RowsFlagHasMorePages
	}
	if rm.NewResultMetadataId != nil {
		flag |= cassandraprotocol.RowsFlagMetadataChanged
	}
	if rm.ContinuousPageNo > 0 {
		flag |= cassandraprotocol.RowsFlagDseContinuousPaging
		if rm.LastContinuousPage {
			flag |= cassandraprotocol.RowsFlagDseLastContinuousPage
		}
	}
	return flag
}

type ColumnMetadata struct {
	Keyspace string
	Table    string
	Name     string
	Index    int32
	Type     datatype.DataType
}

func encodeVariablesMetadata(metadata *VariablesMetadata, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	flags := metadata.Flags()
	if err = primitives.WriteInt(int32(flags), dest); err != nil {
		return fmt.Errorf("cannot write RESULT Prepared variables metadata flags: %w", err)
	}
	if err = primitives.WriteInt(int32(len(metadata.ColumnSpecs)), dest); err != nil {
		return fmt.Errorf("cannot write RESULT Prepared variables metadata column count: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion4 {
		if err = primitives.WriteInt(int32(len(metadata.PkIndices)), dest); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared variables metadata pk indices length: %w", err)
		}
		for i, idx := range metadata.PkIndices {
			if err = primitives.WriteShort(idx, dest); err != nil {
				return fmt.Errorf("cannot write RESULT Prepared variables metadata pk indices element %d: %w", i, err)
			}
		}
	}
	if len(metadata.ColumnSpecs) > 0 {
		globalTableSpec := flags&cassandraprotocol.VariablesFlagGlobalTablesSpec > 0
		if err = encodeColumnSpecs(globalTableSpec, metadata.ColumnSpecs, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared variables metadata column specs: %w", err)
		}
	}
	return nil
}

func lengthOfVariablesMetadata(metadata *VariablesMetadata, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfInt // flags
	length += primitives.LengthOfInt // column count
	if version >= cassandraprotocol.ProtocolVersion4 {
		length += primitives.LengthOfInt // pk count
		length += primitives.LengthOfShort * len(metadata.PkIndices)
	}
	if len(metadata.ColumnSpecs) > 0 {
		globalTableSpec := metadata.Flags()&cassandraprotocol.VariablesFlagGlobalTablesSpec > 0
		var lcs int
		if lcs, err = lengthOfColumnSpecs(globalTableSpec, metadata.ColumnSpecs, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT Prepared variables metadata column specs: %w", err)
		}
		length += lcs
	}
	return length, nil
}

func decodeVariablesMetadata(source io.Reader, version cassandraprotocol.ProtocolVersion) (metadata *VariablesMetadata, err error) {
	metadata = &VariablesMetadata{}
	var f int32
	if f, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata flags: %w", err)
	}
	var flags = cassandraprotocol.VariablesFlag(f)
	var columnCount int32
	if columnCount, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata column count: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion4 {
		var pkCount int32
		if pkCount, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata pk indices length: %w", err)
		}
		if pkCount > 0 {
			metadata.PkIndices = make([]uint16, pkCount)
			for i := 0; i < int(pkCount); i++ {
				if metadata.PkIndices[i], err = primitives.ReadShort(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata pk index element %d: %w", i, err)
				}
			}
		}
	}
	if columnCount > 0 {
		metadata.ColumnSpecs = make([]*ColumnMetadata, columnCount)
		globalTableSpec := flags&cassandraprotocol.VariablesFlagGlobalTablesSpec > 0
		if metadata.ColumnSpecs, err = decodeColumnSpecs(globalTableSpec, columnCount, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata column specs: %w", err)
		}
	}
	return metadata, nil
}

func encodeRowsMetadata(metadata *RowsMetadata, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	flags := metadata.Flags()
	if err = primitives.WriteInt(int32(flags), dest); err != nil {
		return fmt.Errorf("cannot write RESULT Rows metadata flags: %w", err)
	}
	columnSpecsLength := len(metadata.ColumnSpecs)
	if columnSpecsLength > 0 && int(metadata.ColumnCount) != columnSpecsLength {
		return fmt.Errorf(
			"invalid RESULT Rows metadata: metadata.ColumnCount %d != len(metadata.ColumnSpecs) %d",
			metadata.ColumnCount,
			columnSpecsLength,
		)
	}
	if err = primitives.WriteInt(metadata.ColumnCount, dest); err != nil {
		return fmt.Errorf("cannot write RESULT Rows metadata column count: %w", err)
	}
	if flags&cassandraprotocol.RowsFlagHasMorePages > 0 {
		if err = primitives.WriteBytes(metadata.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata paging state: %w", err)
		}
	}
	if flags&cassandraprotocol.RowsFlagMetadataChanged > 0 {
		if err = primitives.WriteShortBytes(metadata.NewResultMetadataId, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata new result metadata id: %w", err)
		}
	}
	if flags&cassandraprotocol.RowsFlagDseContinuousPaging > 0 {
		if err = primitives.WriteInt(metadata.ContinuousPageNo, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata continuous page number: %w", err)
		}
	}
	if flags&cassandraprotocol.RowsFlagNoMetadata == 0 && columnSpecsLength > 0 {
		globalTableSpec := flags&cassandraprotocol.RowsFlagGlobalTablesSpec > 0
		if err = encodeColumnSpecs(globalTableSpec, metadata.ColumnSpecs, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata column specs: %w", err)
		}
	}
	return nil
}

func lengthOfRowsMetadata(metadata *RowsMetadata, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfInt // flags
	length += primitives.LengthOfInt // column count
	flags := metadata.Flags()
	if flags&cassandraprotocol.RowsFlagHasMorePages > 0 {
		length += primitives.LengthOfBytes(metadata.PagingState)
	}
	if flags&cassandraprotocol.RowsFlagMetadataChanged > 0 {
		length += primitives.LengthOfShortBytes(metadata.NewResultMetadataId)
	}
	if flags&cassandraprotocol.RowsFlagDseContinuousPaging > 0 {
		length += primitives.LengthOfInt // continuous page number
	}
	if flags&cassandraprotocol.RowsFlagNoMetadata == 0 && len(metadata.ColumnSpecs) > 0 {
		globalTableSpec := flags&cassandraprotocol.RowsFlagGlobalTablesSpec > 0
		var lengthOfSpecs int
		if lengthOfSpecs, err = lengthOfColumnSpecs(globalTableSpec, metadata.ColumnSpecs, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT Rows metadata column specs: %w", err)
		}
		length += lengthOfSpecs
	}
	return length, nil
}

func decodeRowsMetadata(source io.Reader, version cassandraprotocol.ProtocolVersion) (metadata *RowsMetadata, err error) {
	metadata = &RowsMetadata{}
	var f int32
	if f, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Rows metadata flags: %w", err)
	}
	var flags = cassandraprotocol.RowsFlag(f)
	if metadata.ColumnCount, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT Rows metadata column count: %w", err)
	}
	if flags&cassandraprotocol.RowsFlagHasMorePages > 0 {
		if metadata.PagingState, err = primitives.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata paging state: %w", err)
		}
	}
	if flags&cassandraprotocol.RowsFlagMetadataChanged > 0 {
		if metadata.NewResultMetadataId, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata new result metadata id: %w", err)
		}
	}
	if flags&cassandraprotocol.RowsFlagDseContinuousPaging > 0 {
		if metadata.ContinuousPageNo, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata continuous paging number: %w", err)
		}
		metadata.LastContinuousPage = flags&cassandraprotocol.RowsFlagDseLastContinuousPage > 0
	}
	if flags&cassandraprotocol.RowsFlagNoMetadata == 0 {
		metadata.ColumnSpecs = make([]*ColumnMetadata, metadata.ColumnCount)
		globalTableSpec := flags&cassandraprotocol.RowsFlagGlobalTablesSpec > 0
		if metadata.ColumnSpecs, err = decodeColumnSpecs(globalTableSpec, metadata.ColumnCount, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata column specs: %w", err)
		}
	}
	return metadata, nil
}

func encodeColumnSpecs(globalTableSpec bool, specs []*ColumnMetadata, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if globalTableSpec {
		firstSpec := specs[0]
		if err = primitives.WriteString(firstSpec.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write column spec global keyspace: %w", err)
		}
		if err = primitives.WriteString(firstSpec.Table, dest); err != nil {
			return fmt.Errorf("cannot write column spec global table: %w", err)
		}
	}
	for i, spec := range specs {
		if !globalTableSpec {
			if err = primitives.WriteString(spec.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write column spec %d keyspace: %w", i, err)
			}
			if err = primitives.WriteString(spec.Table, dest); err != nil {
				return fmt.Errorf("cannot write column spec %d table: %w", i, err)
			}
		}
		if err = primitives.WriteString(spec.Name, dest); err != nil {
			return fmt.Errorf("cannot write column spec %d name: %w", i, err)
		}
		if err = datatype.WriteDataType(spec.Type, dest, version); err != nil {
			return fmt.Errorf("cannot write column spec %d type: %w", i, err)
		}
	}
	return nil
}

func lengthOfColumnSpecs(globalTableSpec bool, specs []*ColumnMetadata, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	if globalTableSpec {
		firstSpec := specs[0]
		length += primitives.LengthOfString(firstSpec.Keyspace)
		length += primitives.LengthOfString(firstSpec.Table)
	}
	for i, spec := range specs {
		if !globalTableSpec {
			length += primitives.LengthOfString(spec.Keyspace)
			length += primitives.LengthOfString(spec.Table)
		}
		length += primitives.LengthOfString(spec.Name)
		if lengthOfDataType, err := datatype.LengthOfDataType(spec.Type, version); err != nil {
			return -1, fmt.Errorf("cannot compute length column spec %d type: %w", i, err)
		} else {
			length += lengthOfDataType
		}
	}
	return
}

func decodeColumnSpecs(globalTableSpec bool, columnCount int32, source io.Reader, version cassandraprotocol.ProtocolVersion) (specs []*ColumnMetadata, err error) {
	var globalKsName string
	var globalTableName string
	if globalTableSpec {
		if globalKsName, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read column spec global keyspace: %w", err)
		}
		if globalTableName, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read column spec global table: %w", err)
		}
	}
	specs = make([]*ColumnMetadata, columnCount)
	for i := 0; i < int(columnCount); i++ {
		specs[i] = &ColumnMetadata{}
		if globalTableSpec {
			specs[i].Keyspace = globalKsName
		} else {
			if specs[i].Keyspace, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read column spec %d keyspace: %w", i, err)
			}
		}
		if globalTableSpec {
			specs[i].Table = globalTableName
		} else {
			if specs[i].Table, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read column spec %d table: %w", i, err)
			}
		}
		if specs[i].Name, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read column spec %d name: %w", i, err)
		}
		if specs[i].Type, err = datatype.ReadDataType(source, version); err != nil {
			return nil, fmt.Errorf("cannot read column spec %d type: %w", i, err)
		}
	}
	return specs, nil
}

func haveSameTable(specs []*ColumnMetadata) bool {
	if specs == nil || len(specs) == 0 {
		return false
	}
	first := true
	var ksName string
	var tableName string
	for _, spec := range specs {
		if first {
			first = false
			ksName = spec.Keyspace
			tableName = spec.Table
		} else if spec.Keyspace != ksName || spec.Table != tableName {
			return false
		}
	}
	return true
}
