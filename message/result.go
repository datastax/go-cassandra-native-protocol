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
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Result interface {
	Message
	GetResultType() primitive.ResultType
}

// VOID

type VoidResult struct{}

func (m *VoidResult) IsResponse() bool {
	return true
}

func (m *VoidResult) GetOpCode() primitive.OpCode {
	return primitive.OpCodeResult
}

func (m *VoidResult) GetResultType() primitive.ResultType {
	return primitive.ResultTypeVoid
}

func (m *VoidResult) Clone() Message {
	return &VoidResult{}
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

func (m *SetKeyspaceResult) Clone() Message {
	return &SetKeyspaceResult{
		Keyspace: m.Keyspace,
	}
}

func (m *SetKeyspaceResult) GetOpCode() primitive.OpCode {
	return primitive.OpCodeResult
}

func (m *SetKeyspaceResult) GetResultType() primitive.ResultType {
	return primitive.ResultTypeSetKeyspace
}

func (m *SetKeyspaceResult) String() string {
	return "RESULT SET KEYSPACE " + m.Keyspace
}

// SCHEMA CHANGE

// Note: this struct is identical to SchemaChangeEvent.
type SchemaChangeResult struct {
	// The schema change type.
	ChangeType primitive.SchemaChangeType
	// The schema change target, that is, the kind of schema object affected by the change. This field has been
	// introduced in protocol version 3.
	Target primitive.SchemaChangeTarget
	// The name of the keyspace affected by the change.
	Keyspace string
	// If the schema object affected by the change is not the keyspace itself, this field contains its name. Otherwise,
	// this field is irrelevant.
	Object string
	// If the schema object affected by the change is a function or an aggregate, this field contains its arguments.
	// Otherwise, this field is irrelevant. Valid from protocol version 4 onwards.
	Arguments []string
}

func (m *SchemaChangeResult) IsResponse() bool {
	return true
}

func (m *SchemaChangeResult) GetOpCode() primitive.OpCode {
	return primitive.OpCodeResult
}

func (m *SchemaChangeResult) Clone() Message {
	return &SchemaChangeResult{
		ChangeType: m.ChangeType,
		Target:     m.Target,
		Keyspace:   m.Keyspace,
		Object:     m.Object,
		Arguments:  primitive.CloneStringSlice(m.Arguments),
	}
}

func (m *SchemaChangeResult) GetResultType() primitive.ResultType {
	return primitive.ResultTypeSchemaChange
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

func (m *PreparedResult) IsResponse() bool {
	return true
}

func (m *PreparedResult) Clone() Message {
	return &PreparedResult{
		PreparedQueryId:   primitive.CloneByteSlice(m.PreparedQueryId),
		ResultMetadataId:  primitive.CloneByteSlice(m.ResultMetadataId),
		VariablesMetadata: cloneVariablesMetadata(m.VariablesMetadata),
		ResultMetadata:    cloneRowsMetadata(m.ResultMetadata),
	}
}

func (m *PreparedResult) GetOpCode() primitive.OpCode {
	return primitive.OpCodeResult
}

func (m *PreparedResult) GetResultType() primitive.ResultType {
	return primitive.ResultTypePrepared
}

func (m *PreparedResult) String() string {
	return fmt.Sprintf("RESULT PREPARED (%v)", m.PreparedQueryId)
}

// ROWS

type Column = []byte

type Row = []Column

type RowSet = []Row

type RowsResult struct {
	Metadata *RowsMetadata
	Data     RowSet
}

func (m *RowsResult) IsResponse() bool {
	return true
}

func (m *RowsResult) GetOpCode() primitive.OpCode {
	return primitive.OpCodeResult
}

func (m *RowsResult) Clone() Message {
	return &RowsResult{
		Metadata: cloneRowsMetadata(m.Metadata),
		Data:     cloneRowSet(m.Data),
	}
}

func (m *RowsResult) GetResultType() primitive.ResultType {
	return primitive.ResultTypeRows
}

func (m *RowsResult) String() string {
	return fmt.Sprintf("RESULT ROWS (%v rows x %v cols)", len(m.Data), m.Metadata.ColumnCount)
}

// CODEC

type resultCodec struct{}

func (c *resultCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	result, ok := msg.(Result)
	if !ok {
		return fmt.Errorf("expected message.Result, got %T", msg)
	}
	if err = primitive.CheckValidResultType(result.GetResultType()); err != nil {
		return err
	} else if err = primitive.WriteInt(int32(result.GetResultType()), dest); err != nil {
		return fmt.Errorf("cannot write RESULT type: %w", err)
	}
	switch result.GetResultType() {
	case primitive.ResultTypeVoid:
		return nil
	case primitive.ResultTypeSetKeyspace:
		sk, ok := result.(*SetKeyspaceResult)
		if !ok {
			return fmt.Errorf("expected *message.SetKeyspaceResult, got %T", result)
		}
		if sk.Keyspace == "" {
			return errors.New("RESULT SetKeyspace: cannot write empty keyspace")
		} else if err = primitive.WriteString(sk.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write RESULT SET KEYSPACE keyspace: %w", err)
		}
	case primitive.ResultTypeSchemaChange:
		sce, ok := msg.(*SchemaChangeResult)
		if !ok {
			return fmt.Errorf("expected *message.SchemaChangeResult, got %T", msg)
		}
		if err = primitive.CheckValidSchemaChangeType(sce.ChangeType); err != nil {
			return err
		} else if err = primitive.WriteString(string(sce.ChangeType), dest); err != nil {
			return fmt.Errorf("cannot write SchemaChangeResult.ChangeType: %w", err)
		}
		if version >= primitive.ProtocolVersion3 {
			if err = primitive.CheckValidSchemaChangeTarget(sce.Target, version); err != nil {
				return err
			} else if err = primitive.WriteString(string(sce.Target), dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeResult.Target: %w", err)
			}
			if sce.Keyspace == "" {
				return errors.New("RESULT SchemaChange: cannot write empty keyspace")
			} else if err = primitive.WriteString(sce.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeResult.Keyspace: %w", err)
			}
			switch sce.Target {
			case primitive.SchemaChangeTargetKeyspace:
			case primitive.SchemaChangeTargetTable:
				fallthrough
			case primitive.SchemaChangeTargetType:
				if sce.Object == "" {
					return errors.New("RESULT SchemaChange: cannot write empty object")
				} else if err = primitive.WriteString(sce.Object, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeResult.Object: %w", err)
				}
			case primitive.SchemaChangeTargetAggregate:
				fallthrough
			case primitive.SchemaChangeTargetFunction:
				if sce.Object == "" {
					return errors.New("RESULT SchemaChange: cannot write empty object")
				} else if err = primitive.WriteString(sce.Object, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeResult.Object: %w", err)
				}
				if err = primitive.WriteStringList(sce.Arguments, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeResult.Arguments: %w", err)
				}
			}
		} else {
			if err = primitive.CheckValidSchemaChangeTarget(sce.Target, version); err != nil {
				return err
			}
			if sce.Keyspace == "" {
				return errors.New("RESULT SchemaChange: cannot write empty keyspace")
			} else if err = primitive.WriteString(sce.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write SchemaChangeEvent.Keyspace: %w", err)
			}
			switch sce.Target {
			case primitive.SchemaChangeTargetKeyspace:
				if sce.Object != "" {
					return errors.New("RESULT SchemaChange: table must be empty for keyspace targets")
				} else if err = primitive.WriteString("", dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
				}
			case primitive.SchemaChangeTargetTable:
				if sce.Object == "" {
					return errors.New("RESULT SchemaChange: cannot write empty table")
				} else if err = primitive.WriteString(sce.Object, dest); err != nil {
					return fmt.Errorf("cannot write SchemaChangeEvent.Object: %w", err)
				}
			}
		}
	case primitive.ResultTypePrepared:
		p, ok := msg.(*PreparedResult)
		if !ok {
			return fmt.Errorf("expected *message.PreparedResult, got %T", msg)
		}
		if len(p.PreparedQueryId) == 0 {
			return errors.New("cannot write empty RESULT Prepared query id")
		} else if err = primitive.WriteShortBytes(p.PreparedQueryId, dest); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared prepared query id: %w", err)
		}
		if hasResultMetadataId(version) {
			if len(p.ResultMetadataId) == 0 {
				return errors.New("cannot write empty RESULT Prepared result metadata id")
			} else if err = primitive.WriteShortBytes(p.ResultMetadataId, dest); err != nil {
				return fmt.Errorf("cannot write RESULT Prepared result metadata id: %w", err)
			}
		}
		if err = encodeVariablesMetadata(p.VariablesMetadata, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared variables metadata: %w", err)
		}
		if err = encodeRowsMetadata(p.ResultMetadata, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Prepared result metadata: %w", err)
		}
	case primitive.ResultTypeRows:
		rows, ok := msg.(*RowsResult)
		if !ok {
			return fmt.Errorf("expected *message.RowsResult, got %T", msg)
		}
		if err = encodeRowsMetadata(rows.Metadata, dest, version); err != nil {
			return fmt.Errorf("cannot write RESULT Rows metadata: %w", err)
		}
		if err = primitive.WriteInt(int32(len(rows.Data)), dest); err != nil {
			return fmt.Errorf("cannot write RESULT Rows data length: %w", err)
		}
		for i, row := range rows.Data {
			for j, col := range row {
				if err = primitive.WriteBytes(col, dest); err != nil {
					return fmt.Errorf("cannot write RESULT Rows data row %d col %d: %w", i, j, err)
				}
			}
		}
	default:
		return fmt.Errorf("unknown RESULT type: %v", result.GetResultType())
	}
	return nil
}

func (c *resultCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (length int, err error) {
	result, ok := msg.(Result)
	if !ok {
		return -1, fmt.Errorf("expected interface Result, got %T", msg)
	}
	length += primitive.LengthOfInt
	switch result.GetResultType() {
	case primitive.ResultTypeVoid:
		return length, nil
	case primitive.ResultTypeSetKeyspace:
		sk, ok := result.(*SetKeyspaceResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.SetKeyspaceResult, got %T", result)
		}
		length += primitive.LengthOfString(sk.Keyspace)
	case primitive.ResultTypeSchemaChange:
		sc, ok := msg.(*SchemaChangeResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.SchemaChangeResult, got %T", msg)
		}
		length += primitive.LengthOfString(string(sc.ChangeType))
		if err = primitive.CheckValidSchemaChangeTarget(sc.Target, version); err != nil {
			return -1, err
		}
		if version >= primitive.ProtocolVersion3 {
			length += primitive.LengthOfString(string(sc.Target))
			length += primitive.LengthOfString(sc.Keyspace)
			switch sc.Target {
			case primitive.SchemaChangeTargetKeyspace:
			case primitive.SchemaChangeTargetTable:
				fallthrough
			case primitive.SchemaChangeTargetType:
				length += primitive.LengthOfString(sc.Object)
			case primitive.SchemaChangeTargetAggregate:
				fallthrough
			case primitive.SchemaChangeTargetFunction:
				length += primitive.LengthOfString(sc.Object)
				length += primitive.LengthOfStringList(sc.Arguments)
			}
		} else {
			length += primitive.LengthOfString(sc.Keyspace)
			length += primitive.LengthOfString(sc.Object)
		}
	case primitive.ResultTypePrepared:
		p, ok := msg.(*PreparedResult)
		if !ok {
			return -1, fmt.Errorf("expected *message.PreparedResult, got %T", msg)
		}
		length += primitive.LengthOfShortBytes(p.PreparedQueryId)
		if hasResultMetadataId(version) {
			length += primitive.LengthOfShortBytes(p.ResultMetadataId)
		}
		if lengthOfMetadata, err := lengthOfVariablesMetadata(p.VariablesMetadata, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT Prepared variables metadata: %w", err)
		} else {
			length += lengthOfMetadata
		}
		if lengthOfMetadata, err := lengthOfRowsMetadata(p.ResultMetadata, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of RESULT Prepared result metadata: %w", err)
		} else {
			length += lengthOfMetadata
		}
	case primitive.ResultTypeRows:
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
		length += primitive.LengthOfInt // number of rows
		for _, row := range rows.Data {
			for _, col := range row {
				length += primitive.LengthOfBytes(col)
			}
		}
	default:
		return -1, fmt.Errorf("unknown RESULT type: %v", result.GetResultType())
	}
	return length, nil
}

func (c *resultCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	var resultType int32
	if resultType, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read RESULT type: %w", err)
	}
	switch primitive.ResultType(resultType) {
	case primitive.ResultTypeVoid:
		return &VoidResult{}, nil
	case primitive.ResultTypeSetKeyspace:
		setKeyspace := &SetKeyspaceResult{}
		if setKeyspace.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT SetKeyspaceResult.Keyspace: %w", err)
		}
		return setKeyspace, nil
	case primitive.ResultTypeSchemaChange:
		sc := &SchemaChangeResult{}
		var changeType string
		if changeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read SchemaChangeResult.ChangeType: %w", err)
		}
		sc.ChangeType = primitive.SchemaChangeType(changeType)
		if version >= primitive.ProtocolVersion3 {
			var target string
			if target, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeResult.Target: %w", err)
			}
			sc.Target = primitive.SchemaChangeTarget(target)
			if err = primitive.CheckValidSchemaChangeTarget(sc.Target, version); err != nil {
				return nil, err
			}
			if sc.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeResult.Keyspace: %w", err)
			}
			switch sc.Target {
			case primitive.SchemaChangeTargetKeyspace:
			case primitive.SchemaChangeTargetTable:
				fallthrough
			case primitive.SchemaChangeTargetType:
				if sc.Object, err = primitive.ReadString(source); err != nil {
					return nil, fmt.Errorf("cannot read SchemaChangeResult.Object: %w", err)
				}
			case primitive.SchemaChangeTargetAggregate:
				fallthrough
			case primitive.SchemaChangeTargetFunction:
				if sc.Object, err = primitive.ReadString(source); err != nil {
					return nil, fmt.Errorf("cannot read SchemaChangeResult.Object: %w", err)
				}
				if sc.Arguments, err = primitive.ReadStringList(source); err != nil {
					return nil, fmt.Errorf("cannot read SchemaChangeResult.Arguments: %w", err)
				}
			default:
				return nil, fmt.Errorf("unknown schema change target: %v", sc.Target)
			}
		} else {
			if sc.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Keyspace: %w", err)
			}
			if sc.Object, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read SchemaChangeEvent.Object: %w", err)
			}
			if sc.Object == "" {
				sc.Target = primitive.SchemaChangeTargetKeyspace
			} else {
				sc.Target = primitive.SchemaChangeTargetTable
			}
		}
		return sc, nil
	case primitive.ResultTypePrepared:
		p := &PreparedResult{}
		if p.PreparedQueryId, err = primitive.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Prepared prepared query id: %w", err)
		}
		if hasResultMetadataId(version) {
			if p.ResultMetadataId, err = primitive.ReadShortBytes(source); err != nil {
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
	case primitive.ResultTypeRows:
		rows := &RowsResult{}
		if rows.Metadata, err = decodeRowsMetadata(source, version); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows metadata: %w", err)
		}
		var rowsCount int32
		if rowsCount, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read RESULT Rows data length: %w", err)
		}
		rows.Data = make(RowSet, rowsCount)
		for i := 0; i < int(rowsCount); i++ {
			rows.Data[i] = make(Row, rows.Metadata.ColumnCount)
			for j := 0; j < int(rows.Metadata.ColumnCount); j++ {
				if rows.Data[i][j], err = primitive.ReadBytes(source); err != nil {
					return nil, fmt.Errorf("cannot read RESULT Rows data row %d col %d: %w", i, j, err)
				}
			}
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("unknown RESULT type: %v", resultType)
	}
}

func (c *resultCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeResult
}

func hasResultMetadataId(version primitive.ProtocolVersion) bool {
	return version >= primitive.ProtocolVersion5 &&
		version != primitive.ProtocolVersionDse1
}

func cloneRowSet(o RowSet) RowSet {
	newRowSet := RowSet{}
	for idx, v := range o {
		newRowSet[idx] = cloneRow(v)
	}

	return newRowSet
}

func cloneRow(o Row) Row {
	newRow := Row{}
	for idx, v := range o {
		newRow[idx] = primitive.CloneByteSlice(v)
	}

	return newRow
}
