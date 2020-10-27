package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Result interface {
	Message
	GetResultType() cassandraprotocol.ResultType
}

// VOID

type VoidResult struct{}

func (m *VoidResult) IsResponse() bool {
	return true
}

func (m *VoidResult) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *VoidResult) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeVoid
}

func (m *VoidResult) String() string {
	return "RESULT VOID"
}

// SET KEYSPACE

type SetKeyspaceResult struct {
	Keyspace string
}

func (m *SetKeyspaceResult) IsResponse() bool {
	return true
}

func (m *SetKeyspaceResult) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *SetKeyspaceResult) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeSetKeyspace
}

func (m *SetKeyspaceResult) String() string {
	return "RESULT SET KEYSPACE " + m.Keyspace
}

// SCHEMA CHANGE

// Note: this struct is identical to SchemaChangeEvent
type SchemaChangeResult struct {
	ChangeType cassandraprotocol.SchemaChangeType
	Target     cassandraprotocol.SchemaChangeTarget
	Keyspace   string
	Object     string
	Arguments  []string
}

func (m *SchemaChangeResult) IsResponse() bool {
	return true
}

func (m *SchemaChangeResult) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *SchemaChangeResult) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeSchemaChange
}

func (m *SchemaChangeResult) String() string {
	return fmt.Sprintf("RESULT SCHEMA CHANGE (type=%v target=%v keyspace=%v object=%v args=%v)",
		m.ChangeType,
		m.Target,
		m.Keyspace,
		m.Object,
		m.Arguments)
}

// PREPARED

type PreparedResult struct {
	PreparedQueryId []byte
	// The result set metadata id; valid for protocol version 5, if the prepared statement is a SELECT. Also valid in DSE v2. See Execute.
	ResultMetadataId []byte
	// Reflects the prepared statement's bound variables, if any, or empty (but not nil) if there are no bound variables.
	VariablesMetadata *VariablesMetadata
	// When the prepared statement is a SELECT, reflects the result set columns; empty (but not nil) otherwise.
	ResultMetadata *RowsMetadata
}

type PreparedResultCustomizer func(result *PreparedResult)

func NewPreparedResult(customizers ...PreparedResultCustomizer) *PreparedResult {
	result := &PreparedResult{
		PreparedQueryId:   nil,
		ResultMetadataId:  nil,
		VariablesMetadata: &VariablesMetadata{},
		ResultMetadata:    &RowsMetadata{},
	}
	for _, customizer := range customizers {
		customizer(result)
	}
	return result
}

func WithPreparedQueryId(preparedQueryId []byte) func(result *PreparedResult) {
	return func(result *PreparedResult) {
		result.PreparedQueryId = preparedQueryId
	}
}

func WithResultMetadataId(resultMetadataId []byte) func(result *PreparedResult) {
	return func(result *PreparedResult) {
		result.ResultMetadataId = resultMetadataId
	}
}

func WithVariablesMetadata(variablesMetadata *VariablesMetadata) func(result *PreparedResult) {
	return func(result *PreparedResult) {
		result.VariablesMetadata = variablesMetadata
	}
}

func WithPreparedResultMetadata(rowsMetadata *RowsMetadata) func(result *PreparedResult) {
	return func(result *PreparedResult) {
		result.ResultMetadata = rowsMetadata
	}
}

func (m *PreparedResult) IsResponse() bool {
	return true
}

func (m *PreparedResult) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *PreparedResult) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypePrepared
}

func (m *PreparedResult) String() string {
	return fmt.Sprintf("RESULT PREPARED (%v)", m.PreparedQueryId)
}

// ROWS

type RowsResult struct {
	Metadata *RowsMetadata
	Data     [][][]byte
}

type RowsResultCustomizer func(result *RowsResult)

func NewRowsResult(customizers ...RowsResultCustomizer) *RowsResult {
	result := &RowsResult{
		Metadata: &RowsMetadata{},
	}
	for _, customizer := range customizers {
		customizer(result)
	}
	return result
}

func WithRowsMetadata(rowsMetadata *RowsMetadata) func(result *RowsResult) {
	return func(result *RowsResult) {
		result.Metadata = rowsMetadata
	}
}

func WithRowsData(rows ...[][]byte) func(result *RowsResult) {
	return func(result *RowsResult) {
		result.Data = rows
	}
}

func (m *RowsResult) IsResponse() bool {
	return true
}

func (m *RowsResult) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func (m *RowsResult) GetResultType() cassandraprotocol.ResultType {
	return cassandraprotocol.ResultTypeRows
}

func (m *RowsResult) String() string {
	return fmt.Sprintf("RESULT ROWS (%v rows x %v cols)", len(m.Data), m.Metadata.ColumnCount)
}

// CODEC

type ResultCodec struct{}

func (c *ResultCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	result, ok := msg.(Result)
	if !ok {
		return fmt.Errorf("expected message.Result, got %T", msg)
	}
	if err = cassandraprotocol.CheckResultType(result.GetResultType()); err != nil {
		return err
	} else if err = primitives.WriteInt(result.GetResultType(), dest); err != nil {
		return fmt.Errorf("cannot write RESULT type: %w", err)
	}
	switch result.GetResultType() {
	case cassandraprotocol.ResultTypeVoid:
		return nil
	case cassandraprotocol.ResultTypeSetKeyspace:
		sk, ok := result.(*SetKeyspaceResult)
		if !ok {
			return fmt.Errorf("expected *message.SetKeyspaceResult, got %T", result)
		}
		if sk.Keyspace == "" {
			return errors.New("RESULT SetKeyspace: cannot write empty keyspace")
		} else if err = primitives.WriteString(sk.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write RESULT SET KEYSPACE keyspace: %w", err)
		}
	case cassandraprotocol.ResultTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeResult)
		if !ok {
			return fmt.Errorf("expected *message.SchemaChangeResult, got %T", msg)
		}
		if err = cassandraprotocol.CheckSchemaChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitives.WriteString(sce.ChangeType, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeResult.ChangeType: %w", err)
		}
		if err = cassandraprotocol.CheckSchemaChangeTarget(sce.Target); err != nil {
			return err
		} else if err = primitives.WriteString(sce.Target, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeResult.Target: %w", err)
		}
		if sce.Keyspace == "" {
			return errors.New("RESULT SchemaChange: cannot write empty keyspace")
		} else if err = primitives.WriteString(sce.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeResult.Keyspace: %w", err)
		}
		switch sce.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if sce.Object == "" {
				return errors.New("RESULT SchemaChange: cannot write empty object")
			} else if err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeResult.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return fmt.Errorf("%s schema change targets are not supported in protocol version %d", sce.Target, version)
			}
			if sce.Object == "" {
				return errors.New("RESULT SchemaChange: cannot write empty object")
			} else if err = primitives.WriteString(sce.Object, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeResult.Object: %w", err)
			}
			if err = primitives.WriteStringList(sce.Arguments, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeResult.Arguments: %w", err)
			}
		default:
			return fmt.Errorf("unknown schema change target: %v", sce.Target)
		}
	case cassandraprotocol.ResultTypePrepared:
		p, ok := msg.(*PreparedResult)
		if !ok {
			return fmt.Errorf("expected *message.PreparedResult, got %T", msg)
		}
		if len(p.PreparedQueryId) == 0 {
			return errors.New("cannot write empty RESULT Prepared query id")
		} else if err = primitives.WriteShortBytes(p.PreparedQueryId, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared prepared query id: %w", err)
		}
		if hasResultMetadataId(version) {
			if len(p.ResultMetadataId) == 0 {
				return errors.New("cannot write empty RESULT Prepared result metadata id")
			} else if err = primitives.WriteShortBytes(p.ResultMetadataId, dest); err != nil {
				return fmt.Errorf("cannot write RESULT Prepared result metadata id: %w", err)
			}
		}
		if p.VariablesMetadata == nil {
			return errors.New("cannot write nil RESULT Prepared variables metadata")
		} else if err = encodeVariablesMetadata(p.VariablesMetadata, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared variables metadata: %w", err)
		}
		if p.ResultMetadata == nil {
			return errors.New("cannot write nil RESULT Prepared result metadata")
		} else if err = encodeRowsMetadata(p.ResultMetadata, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared result metadata: %w", err)
		}
	case cassandraprotocol.ResultTypeRows:
		rows, ok := msg.(*RowsResult)
		if !ok {
			return fmt.Errorf("expected *message.RowsResult, got %T", msg)
		}
		if rows.Metadata == nil {
			return errors.New("cannot write nil RESULT Prepared result metadata")
		} else if err = encodeRowsMetadata(rows.Metadata, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata: %w", err)
		}
		if err = primitives.WriteInt(int32(len(rows.Data)), dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows data length: %w", err)
		}
		for i, row := range rows.Data {
			for j, col := range row {
				if err = primitives.WriteBytes(col, dest); err != nil {
					return fmt.Errorf("cannot write RESULT Rows data row %d col %d: %w", i, j, err)
				}
			}
		}
	default:
		return fmt.Errorf("unknown RESULT type: %v", result.GetResultType())
	}
	return nil
}

func (c *ResultCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	result, ok := msg.(Result)
	if !ok {
		return -1, fmt.Errorf("expected interface Result, got %T", msg)
	}
	length += primitives.LengthOfInt
	switch result.GetResultType() {
	case cassandraprotocol.ResultTypeVoid:
		return length, nil
	case cassandraprotocol.ResultTypeSetKeyspace:
		sk, ok := result.(*SetKeyspaceResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.SetKeyspaceResult, got %T", result)
		}
		length += primitives.LengthOfString(sk.Keyspace)
	case cassandraprotocol.ResultTypeSchemaChange:
		sc, ok := msg.(*SchemaChangeResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.SchemaChangeResult, got %T", msg)
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
			length += primitives.LengthOfString(sc.Object)
			length += primitives.LengthOfStringList(sc.Arguments)
		default:
			return -1, fmt.Errorf("unknown schema change target: %v", sc.Target)
		}
	case cassandraprotocol.ResultTypePrepared:
		p, ok := msg.(*PreparedResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.PreparedResult, got %T", msg)
		}
		length += primitives.LengthOfShortBytes(p.PreparedQueryId)
		if hasResultMetadataId(version) {
			length += primitives.LengthOfShortBytes(p.ResultMetadataId)
		}
		if p.VariablesMetadata == nil {
			return -1, errors.New("cannot compute length of nil RESULT Prepared variables metadata")
		} else {
			var lengthOfMetadata int
			if lengthOfMetadata, err = lengthOfVariablesMetadata(p.VariablesMetadata, version); err != nil {
				return -1, fmt.Errorf("cannot compute length of RESULT Prepared variables metadata: %w", err)
			}
			length += lengthOfMetadata
		}
		if p.ResultMetadata == nil {
			return -1, errors.New("cannot compute length of nil RESULT Prepared result metadata")
		} else {
			var lengthOfMetadata int
			if lengthOfMetadata, err = lengthOfRowsMetadata(p.ResultMetadata, version); err != nil {
				return -1, fmt.Errorf("cannot compute length of RESULT Prepared result metadata: %w", err)
			}
			length += lengthOfMetadata
		}
	case cassandraprotocol.ResultTypeRows:
		rows, ok := msg.(*RowsResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.RowsResult, got %T", msg)
		}
		if rows.Metadata == nil {
			return -1, errors.New("cannot compute length of nil RESULT Rows metadata")
		} else {
			var lengthOfMetadata int
			if lengthOfMetadata, err = lengthOfRowsMetadata(rows.Metadata, version); err != nil {
				return -1, fmt.Errorf("cannot compute length of RESULT Rows metadata: %w", err)
			}
			length += lengthOfMetadata
		}
		length += primitives.LengthOfInt // number of rows
		for _, row := range rows.Data {
			for _, col := range row {
				length += primitives.LengthOfBytes(col)
			}
		}
	default:
		return -1, fmt.Errorf("unknown RESULT type: %v", result.GetResultType())
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
		return &VoidResult{}, nil
	case cassandraprotocol.ResultTypeSetKeyspace:
		setKeyspace := &SetKeyspaceResult{}
		if setKeyspace.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT SetKeyspaceResult.Keyspace: %w", err)
		}
		return setKeyspace, nil
	case cassandraprotocol.ResultTypeSchemaChange:
		sc := &SchemaChangeResult{}
		if sc.ChangeType, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeResult.ChangeType: %w", err)
		}
		if sc.Target, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeResult.Target: %w", err)
		}
		if sc.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeResult.Keyspace: %w", err)
		}
		switch sc.Target {
		case cassandraprotocol.SchemaChangeTargetKeyspace:
		case cassandraprotocol.SchemaChangeTargetTable:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetType:
			if sc.Object, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeResult.Object: %w", err)
			}
		case cassandraprotocol.SchemaChangeTargetAggregate:
			fallthrough
		case cassandraprotocol.SchemaChangeTargetFunction:
			if version < cassandraprotocol.ProtocolVersion4 {
				return nil, fmt.Errorf("%s schema change targets are not supported in protocol version %d", sc.Target, version)
			}
			if sc.Object, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeResult.Object: %w", err)
			}
			if sc.Arguments, err = primitives.ReadStringList(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeResult.Arguments: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown schema change target: %v", sc.Target)
		}
		return sc, nil
	case cassandraprotocol.ResultTypePrepared:
		p := &PreparedResult{}
		if p.PreparedQueryId, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared prepared query id: %w", err)
		}
		if hasResultMetadataId(version) {
			if p.ResultMetadataId, err = primitives.ReadShortBytes(source); err != nil {
				return nil, fmt.Errorf("cannot read RESULT Prepared result metadata id: %w", err)
			}
		}
		if p.VariablesMetadata, err = decodeVariablesMetadata(source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared variables metadata: %w", err)
		}
		if p.ResultMetadata, err = decodeRowsMetadata(source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared result metadata: %w", err)
		}
		return p, nil
	case cassandraprotocol.ResultTypeRows:
		rows := &RowsResult{}
		if rows.Metadata, err = decodeRowsMetadata(source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata: %w", err)
		}
		var rowsCount int32
		if rowsCount, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows data length: %w", err)
		}
		rows.Data = make([][][]byte, rowsCount)
		for i := 0; i < int(rowsCount); i++ {
			rows.Data[i] = make([][]byte, rows.Metadata.ColumnCount)
			for j := 0; j < int(rows.Metadata.ColumnCount); j++ {
				if rows.Data[i][j], err = primitives.ReadBytes(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT Rows data row %d col %d: %w", i, j, err)
				}
			}
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("unknown RESULT type: %v", resultType)
	}
}

func (c *ResultCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeResult
}

func hasResultMetadataId(version cassandraprotocol.ProtocolVersion) bool {
	return version >= cassandraprotocol.ProtocolVersion5 &&
		version != cassandraprotocol.ProtocolVersionDse1
}
