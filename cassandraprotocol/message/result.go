package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Result interface {
	Message
	GetResultType() cassandraprotocol.ResultType
}

// VOID

type Void struct{}

func (m *Void) IsResponse() bool {
	return true
}

func (m *Void) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *Void) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeVoid
}

func (m *Void) String() string {
	return "RESULT VOID"
}

// SET KEYSPACE

type SetKeyspace struct {
	Keyspace string
}

func (m *SetKeyspace) IsResponse() bool {
	return true
}

func (m *SetKeyspace) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *SetKeyspace) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeSetKeyspace
}

func (m *SetKeyspace) String() string {
	return "RESULT SET KEYSPACE " + m.Keyspace
}

// SCHEMA CHANGE

type SchemaChange struct {
	ChangeType cassandraprotocol.SchemaChangeType
	Target     cassandraprotocol.SchemaChangeTarget
	Keyspace   string
	Object     string
	Arguments  []string
}

func (m *SchemaChange) IsResponse() bool {
	return true
}

func (m *SchemaChange) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *SchemaChange) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeSchemaChange
}

func (m *SchemaChange) String() string {
	return fmt.Sprintf("RESULT SCHEMA CHANGE (type=%v target=%v keyspace=%v object=%v args=%v)",
		m.ChangeType,
		m.Target,
		m.Keyspace,
		m.Object,
		m.Arguments)
}

// PREPARED

type Prepared struct {
	PreparedQueryId   []byte
	ResultMetadataId  []byte
	VariablesMetadata *RowsMetadata
	ResultMetadata    *RowsMetadata
}

func (m *Prepared) IsResponse() bool {
	return true
}

func (m *Prepared) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *Prepared) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypePrepared
}

func (m *Prepared) String() string {
	return fmt.Sprintf("RESULT PREPARED (%v)", m.PreparedQueryId)
}

// ROWS

type Rows struct {
	Metadata *RowsMetadata
	Data     [][][]byte
}

func (m *Rows) IsResponse() bool {
	return true
}

func (m *Rows) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *Rows) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeRows
}

func (m *Rows) String() string {
	return fmt.Sprintf("RESULT ROWS (%v rows x %v cols)", len(m.Data), m.Metadata.ColumnCount)
}

type RowsMetadata struct {
	Flags               cassandraprotocol.RowsFlag
	ColumnCount         int32
	PagingState         []byte
	NewResultMetadataId []byte
	PkIndices           []uint16
	// nil if the NO_METADATA flag is present
	ColumnSpecs []*ColumnSpec
}

func NewRowsMetadata(customizers ...RowsMetadataCustomizer) *RowsMetadata {
	metadata := &RowsMetadata{}
	for _, customizer := range customizers {
		customizer(metadata)
	}
	return metadata
}

type RowsMetadataCustomizer func(*RowsMetadata)

// Builds a new instance with NO_METADATA = false; the column count is set to the number
// of column specifications in the provided list.
func WithColumnSpecs(columnSpecs []*ColumnSpec, pagingState []byte, pkIndices []uint16, newResultMetadataId []byte) RowsMetadataCustomizer {
	return func(metadata *RowsMetadata) {
		metadata.ColumnCount = int32(len(columnSpecs))
		metadata.ColumnSpecs = columnSpecs
		metadata.PagingState = pagingState
		metadata.PkIndices = pkIndices
		metadata.NewResultMetadataId = newResultMetadataId
		metadata.Flags = computeFlags(false, columnSpecs, pagingState, newResultMetadataId)
	}
}

// Builds a new instance with NO_METADATA = true.
func WithoutColumnSpecs(columnCount int32, pagingState []byte, pkIndices []uint16, newResultMetadataId []byte) RowsMetadataCustomizer {
	return func(metadata *RowsMetadata) {
		metadata.ColumnCount = columnCount
		metadata.PagingState = pagingState
		metadata.PkIndices = pkIndices
		metadata.NewResultMetadataId = newResultMetadataId
		metadata.Flags = computeFlags(true, nil, pagingState, newResultMetadataId)
	}
}

type ColumnSpec struct {
	KeyspaceName string
	TableName    string
	Name         string
	Index        int32
	Type         datatype.DataType
}

// CODEC

type ResultCodec struct{}

func (c *ResultCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	result, ok := msg.(Result)
	if !ok {
		return errors.New(fmt.Sprintf("expected interface Result, got %T", msg))
	}
	if err = primitives.WriteInt(result.GetResultType(), dest); err != nil {
		return fmt.Errorf("cannot write RESULT type: %w", err)
	}
	switch result.GetResultType() {
	case cassandraprotocol.ResultTypeVoid:
		return nil
	case cassandraprotocol.ResultTypeSetKeyspace:
		sk, ok := result.(*SetKeyspace)
		if !ok {
			return errors.New(fmt.Sprintf("expected SetKeyspace, got %T", result))
		}
		if err = primitives.WriteString(sk.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write RESULT SET KEYSPACE keyspace: %w", err)
		}
	case cassandraprotocol.ResultTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected SchemaChange, got %T", msg))
		}
		switch sce.ChangeType {
		case cassandraprotocol.SchemaChangeTypeCreated:
		case cassandraprotocol.SchemaChangeTypeUpdated:
		case cassandraprotocol.SchemaChangeTypeDropped:
		default:
			return errors.New(fmt.Sprintf("unknown schema change type: %v", sce.Target))
		}
		if err = primitives.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChange.ChangeType: %w", err)
		}
		if err = primitives.WriteString(sce.Target, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChange.Target: %w", err)
		}
		if err = primitives.WriteString(sce.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChange.Keyspace: %w", err)
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChange.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return errors.New(fmt.Sprintf("%s schema changes are not supported in protocol version %d", sce.Target, version))
			}
			if err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChange.Object: %w", err)
			}
			if err = primitives.WriteStringList(sce.Arguments, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChange.Arguments: %w", err)
			}
		default:
			return errors.New(fmt.Sprintf("unknown schema change target: %v", sce.Target))
		}
	case cassandraprotocol.ResultTypePrepared:
		p, ok := msg.(*Prepared)
		if !ok {
			return errors.New(fmt.Sprintf("expected Prepared, got %T", msg))
		}
		if err = primitives.WriteShortBytes(p.PreparedQueryId, dest); err != nil {
			return fmt.Errorf("cannot write RESULT PREPARED prepared query id: %w", err)
		}
		if version >= cassandraprotocol.ProtocolVersion5 {
			if err = primitives.WriteShortBytes(p.ResultMetadataId, dest); err != nil {
				return fmt.Errorf("cannot write RESULT PREPARED result metadata id: %w", err)
			}
		}
		if err = encodeRowsMetadata(p.VariablesMetadata, version >= cassandraprotocol.ProtocolVersion4, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT PREPARED variables metadata: %w", err)
		}
		if err = encodeRowsMetadata(p.ResultMetadata, false, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT PREPARED result metadata: %w", err)
		}
	case cassandraprotocol.ResultTypeRows:
		rows, ok := msg.(*Rows)
		if !ok {
			return errors.New(fmt.Sprintf("expected Rows, got %T", msg))
		}
		if err = encodeRowsMetadata(rows.Metadata, false, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT ROWS metadata: %w", err)
		}
		if err = primitives.WriteInt(int32(len(rows.Data)), dest); err != nil {
			return fmt.Errorf("cannot write RESULT ROWS data length: %w", err)
		}
		for i, row := range rows.Data {
			for j, col := range row {
				if err = primitives.WriteBytes(col, dest); err != nil {
					return fmt.Errorf("cannot write RESULT ROWS data row %d col %d: %w", i, j, err)
				}
			}
		}
	default:
		return errors.New(fmt.Sprintf("unknown RESULT type: %v", result.GetResultType()))
	}
	return nil
}

func (c *ResultCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	result, ok := msg.(Result)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected interface Result, got %T", msg))
	}
	length += primitives.LengthOfInt
	switch result.GetResultType() {
	case cassandraprotocol.ResultTypeVoid:
		return length, nil
	case cassandraprotocol.ResultTypeSetKeyspace:
		sk, ok := result.(*SetKeyspace)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected SetKeyspace, got %T", result))
		}
		length += primitives.LengthOfString(sk.Keyspace)
	case cassandraprotocol.ResultTypeSchemaChange:
		sc, ok := msg.(*SchemaChange)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected SchemaChange, got %T", msg))
		}
		length += primitives.LengthOfString(sc.ChangeType)
		length += primitives.LengthOfString(sc.Target)
		length += primitives.LengthOfString(sc.Keyspace)
		switch sc.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			length += primitives.LengthOfString(sc.Object)
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return -1, errors.New(fmt.Sprintf("%s schema changes are not supported in protocol version %d", sc.Target, version))
			}
			length += primitives.LengthOfString(sc.Object)
			length += primitives.LengthOfStringList(sc.Arguments)
		default:
			return -1, errors.New(fmt.Sprintf("unknown schema change target: %v", sc.Target))
		}
	case cassandraprotocol.ResultTypePrepared:
		p, ok := msg.(*Prepared)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected Prepared, got %T", msg))
		}
		length += primitives.LengthOfShortBytes(p.PreparedQueryId)
		if version >= cassandraprotocol.ProtocolVersion5 {
			length += primitives.LengthOfShortBytes(p.ResultMetadataId)
		}
		var lengthOfVariablesMetadata int
		if lengthOfVariablesMetadata, err = lengthOfRowsMetadata(p.VariablesMetadata, version >= cassandraprotocol.ProtocolVersion4, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT PREPARED variables metadata: %w", err)
		}
		length += lengthOfVariablesMetadata
		var lengthOfResultMetadata int
		if lengthOfResultMetadata, err = lengthOfRowsMetadata(p.ResultMetadata, false, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT PREPARED result metadata: %w", err)
		}
		length += lengthOfResultMetadata
	case cassandraprotocol.ResultTypeRows:
		rows, ok := msg.(*Rows)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected Rows, got %T", msg))
		}
		var lengthOfMetadata int
		if lengthOfMetadata, err = lengthOfRowsMetadata(rows.Metadata, false, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT ROWS metadata: %w", err)
		}
		length += lengthOfMetadata
		length += primitives.LengthOfInt // number of rows
		for _, row := range rows.Data {
			for _, col := range row {
				length += primitives.LengthOfBytes(col)
			}
		}
	default:
		return -1, errors.New(fmt.Sprintf("unknown RESULT type: %v", result.GetResultType()))
	}
	return length, nil
}

func (c *ResultCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	var resultType cassandraprotocol.ResultType
	if resultType, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT type: %w", err)
	}
	switch resultType {
	case cassandraprotocol.ResultTypeVoid:
		return &Void{}, nil
	case cassandraprotocol.ResultTypeSetKeyspace:
		setKeyspace := &SetKeyspace{}
		if setKeyspace.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT SetKeyspace.Keyspace: %w", err)
		}
		return setKeyspace, nil
	case cassandraprotocol.ResultTypeSchemaChange:
		sc := &SchemaChange{}
		if sc.ChangeType, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChange.ChangeType: %w", err)
		}
		if sc.Target, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChange.Target: %w", err)
		}
		if sc.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChange.Keyspace: %w", err)
		}
		switch sc.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if sc.Object, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChange.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return nil, errors.New(fmt.Sprintf("%s schema change are not supported in protocol version %d", sc.Target, version))
			}
			if sc.Object, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChange.Object: %w", err)
			}
			if sc.Arguments, err = primitives.ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChange.Arguments: %w", err)
			}
		default:
			return nil, errors.New(fmt.Sprintf("unknown schema change target: %v", sc.Target))
		}
		return sc, nil
	case cassandraprotocol.ResultTypePrepared:
		p := &Prepared{}
		if p.PreparedQueryId, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT PREPARED prepared query id: %w", err)
		}
		if version >= cassandraprotocol.ProtocolVersion5 {
			if p.ResultMetadataId, err = primitives.ReadShortBytes(source); err != nil {
				return nil, fmt.Errorf("cannot read RESULT PREPARED result metadata id: %w", err)
			}
		}
		if p.VariablesMetadata, err = decodeRowsMetadata(version >= cassandraprotocol.ProtocolVersion4, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT PREPARED variables metadata: %w", err)
		}
		if p.ResultMetadata, err = decodeRowsMetadata(false, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT PREPARED result metadata: %w", err)
		}
		return p, nil
	case cassandraprotocol.ResultTypeRows:
		rows := &Rows{}
		if rows.Metadata, err = decodeRowsMetadata(false, source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT ROWS metadata: %w", err)
		}
		var rowsCount int32
		if rowsCount, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT ROWS data length: %w", err)
		}
		rows.Data = make([][][]byte, rowsCount)
		for i := 0; i < int(rowsCount); i++ {
			rows.Data[i] = make([][]byte, rows.Metadata.ColumnCount)
			for j := 0; j < int(rows.Metadata.ColumnCount); j++ {
				if rows.Data[i][j], err = primitives.ReadBytes(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT ROWS data row %d col %d: %w", i, j, err)
				}
			}
		}
		return rows, nil
	default:
		return nil, errors.New(fmt.Sprintf("unknown RESULT type: %v", resultType))
	}
}

func (c *ResultCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func encodeRowsMetadata(metadata *RowsMetadata, encodePkIndices bool, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if err = primitives.WriteInt(metadata.Flags, dest); err != nil {
		return fmt.Errorf("cannot write RESULT ROWS metadata flags: %w", err)
	}
	if err = primitives.WriteInt(metadata.ColumnCount, dest); err != nil {
		return fmt.Errorf("cannot write RESULT ROWS metadata column count: %w", err)
	}
	if encodePkIndices {
		if metadata.PkIndices == nil {
			if err = primitives.WriteInt(0, dest); err != nil {
				return fmt.Errorf("cannot write RESULT ROWS metadata pk indices length: %w", err)
			}
		} else {
			if err = primitives.WriteInt(int32(len(metadata.PkIndices)), dest); err != nil {
				return fmt.Errorf("cannot write RESULT ROWS metadata pk indices length: %w", err)
			}
			for i, idx := range metadata.PkIndices {
				if err = primitives.WriteShort(idx, dest); err != nil {
					return fmt.Errorf("cannot write RESULT ROWS metadata pk indices element %d: %w", i, err)
				}
			}
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagHasMorePages > 0 {
		if err = primitives.WriteBytes(metadata.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write RESULT ROWS metadata paging state: %w", err)
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagMetadataChanged > 0 {
		if err = primitives.WriteShortBytes(metadata.NewResultMetadataId, dest); err != nil {
			return fmt.Errorf("cannot write RESULT ROWS metadata new result metadata id: %w", err)
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagNoMetadata == 0 && len(metadata.ColumnSpecs) > 0 {
		globalTable := metadata.Flags&cassandraprotocol.RowsFlagGlobalTablesSpec > 0
		if globalTable {
			firstSpec := metadata.ColumnSpecs[0]
			if err = primitives.WriteString(firstSpec.KeyspaceName, dest); err != nil {
				return fmt.Errorf("cannot write RESULT ROWS column spec global keyspace: %w", err)
			}
			if err = primitives.WriteString(firstSpec.TableName, dest); err != nil {
				return fmt.Errorf("cannot write RESULT ROWS column spec global table: %w", err)
			}
		}
		for i, spec := range metadata.ColumnSpecs {
			if !globalTable {
				if err = primitives.WriteString(spec.KeyspaceName, dest); err != nil {
					return fmt.Errorf("cannot write RESULT ROWS column spec %d keyspace: %w", i, err)
				}
				if err = primitives.WriteString(spec.TableName, dest); err != nil {
					return fmt.Errorf("cannot write RESULT ROWS column spec %d table: %w", i, err)
				}
			}
			if err = primitives.WriteString(spec.Name, dest); err != nil {
				return fmt.Errorf("cannot write RESULT ROWS column spec %d name: %w", i, err)
			}
			if err = datatype.WriteDataType(spec.Type, dest, version); err != nil {
				return fmt.Errorf("cannot write RESULT ROWS column spec %d type: %w", i, err)
			}
		}
	}
	return nil
}

func lengthOfRowsMetadata(metadata *RowsMetadata, encodePkIndices bool, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfInt // flags
	length += primitives.LengthOfInt // column count
	if encodePkIndices {
		length += primitives.LengthOfInt // pk count
		if metadata.PkIndices != nil {
			length += primitives.LengthOfShort * len(metadata.PkIndices)
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagHasMorePages > 0 {
		length += primitives.LengthOfBytes(metadata.PagingState)
	}
	if metadata.Flags&cassandraprotocol.RowsFlagMetadataChanged > 0 {
		length += primitives.LengthOfShortBytes(metadata.NewResultMetadataId)
	}
	if metadata.Flags&cassandraprotocol.RowsFlagNoMetadata == 0 && len(metadata.ColumnSpecs) > 0 {
		globalTable := metadata.Flags&cassandraprotocol.RowsFlagGlobalTablesSpec > 0
		if globalTable {
			firstSpec := metadata.ColumnSpecs[0]
			length += primitives.LengthOfString(firstSpec.KeyspaceName)
			length += primitives.LengthOfString(firstSpec.TableName)
		}
		for _, spec := range metadata.ColumnSpecs {
			if !globalTable {
				length += primitives.LengthOfString(spec.KeyspaceName)
				length += primitives.LengthOfString(spec.TableName)
			}
			length += primitives.LengthOfString(spec.Name)
			if lengthOfDataType, err := datatype.LengthOfDataType(spec.Type, version); err != nil {
				return -1, err
			} else {
				length += lengthOfDataType
			}
		}
	}
	return length, nil
}

func decodeRowsMetadata(decodePkIndices bool, source io.Reader, version cassandraprotocol.ProtocolVersion) (metadata *RowsMetadata, err error) {
	metadata = &RowsMetadata{}
	if metadata.Flags, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT ROWS metadata flags: %w", err)
	}
	if metadata.ColumnCount, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT ROWS metadata column count: %w", err)
	}
	if decodePkIndices {
		var pkCount int32
		if pkCount, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT ROWS metadata pk indices length: %w", err)
		}
		if pkCount > 0 {
			metadata.PkIndices = make([]uint16, pkCount)
			for i := 0; i < int(pkCount); i++ {
				if metadata.PkIndices[i], err = primitives.ReadShort(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT ROWS metadata pk index element %d: %w", i, err)
				}
			}
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagHasMorePages > 0 {
		if metadata.PagingState, err = primitives.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT ROWS metadata paging state: %w", err)
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagMetadataChanged > 0 {
		if metadata.NewResultMetadataId, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT ROWS metadata new result metadata id: %w", err)
		}
	}
	if metadata.Flags&cassandraprotocol.RowsFlagNoMetadata == 0 {
		metadata.ColumnSpecs = make([]*ColumnSpec, metadata.ColumnCount)
		globalTableSpec := metadata.Flags&cassandraprotocol.RowsFlagGlobalTablesSpec > 0
		var globalKsName string
		var globalTableName string
		if globalTableSpec {
			if globalKsName, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read RESULT ROWS column spec global keyspace: %w", err)
			}
			if globalTableName, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read RESULT ROWS column spec global table: %w", err)
			}
		}
		for i := 0; i < int(metadata.ColumnCount); i++ {
			metadata.ColumnSpecs[i] = &ColumnSpec{}
			if globalTableSpec {
				metadata.ColumnSpecs[i].KeyspaceName = globalKsName
			} else {
				if metadata.ColumnSpecs[i].KeyspaceName, err = primitives.ReadString(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT ROWS column spec %d keyspace: %w", i, err)
				}
			}
			if globalTableSpec {
				metadata.ColumnSpecs[i].TableName = globalTableName
			} else {
				if metadata.ColumnSpecs[i].TableName, err = primitives.ReadString(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT ROWS column spec %d table: %w", i, err)
				}
			}
			if metadata.ColumnSpecs[i].Name, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read RESULT ROWS column spec %d name: %w", i, err)
			}
			if metadata.ColumnSpecs[i].Type, err = datatype.ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read RESULT ROWS column spec %d type: %w", i, err)
			}
		}
	}
	return metadata, nil
}

func computeFlags(
	noMetadata bool,
	columnSpecs []*ColumnSpec,
	pagingState []byte,
	newResultMetadataId []byte) (flag cassandraprotocol.RowsFlag) {
	if noMetadata {
		flag |= cassandraprotocol.RowsFlagNoMetadata
	} else if haveSameTable(columnSpecs) {
		flag |= cassandraprotocol.RowsFlagGlobalTablesSpec
	}
	if pagingState != nil {
		flag |= cassandraprotocol.RowsFlagHasMorePages
	}
	if newResultMetadataId != nil {
		flag |= cassandraprotocol.RowsFlagMetadataChanged
	}
	return flag
}

func haveSameTable(specs []*ColumnSpec) bool {
	if specs == nil || len(specs) == 0 {
		return false
	}
	first := true
	var ksName string
	var tableName string
	for _, spec := range specs {
		if first {
			first = false
			ksName = spec.KeyspaceName
			tableName = spec.TableName
		} else if spec.KeyspaceName != ksName || spec.TableName != tableName {
			return false
		}
	}
	return true
}
