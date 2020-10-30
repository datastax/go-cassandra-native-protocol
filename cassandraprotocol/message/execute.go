package message

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Execute struct {
	QueryId []byte
	// the ID of the result set metadata that was sent along with response to PREPARE message.
	// Valid in protocol version 5 and DSE protocol version 2. See PreparedResult.
	ResultMetadataId []byte
	Options          *QueryOptions
}

func (m *Execute) IsResponse() bool {
	return false
}

func (m *Execute) GetOpCode() primitives.OpCode {
	return primitives.OpCodeExecute
}

func (m *Execute) String() string {
	return "EXECUTE " + hex.EncodeToString(m.QueryId)
}

type ExecuteCodec struct{}

func (c *ExecuteCodec) Encode(msg Message, dest io.Writer, version primitives.ProtocolVersion) error {
	execute, ok := msg.(*Execute)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Execute, got %T", msg))
	}
	if len(execute.QueryId) == 0 {
		return errors.New("EXECUTE missing query id")
	} else if err := primitives.WriteShortBytes(execute.QueryId, dest); err != nil {
		return fmt.Errorf("cannot write EXECUTE query id: %w", err)
	}
	if hasResultMetadataId(version) {
		if len(execute.ResultMetadataId) == 0 {
			return errors.New("EXECUTE missing result metadata id")
		} else if err := primitives.WriteShortBytes(execute.ResultMetadataId, dest); err != nil {
			return fmt.Errorf("cannot write EXECUTE result metadata id: %w", err)
		}
	}
	if err := EncodeQueryOptions(execute.Options, dest, version); err != nil {
		return fmt.Errorf("cannot write EXECUTE options: %w", err)
	}
	return nil
}

func (c *ExecuteCodec) EncodedLength(msg Message, version primitives.ProtocolVersion) (size int, err error) {
	execute, ok := msg.(*Execute)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Execute, got %T", msg))
	}
	size += primitives.LengthOfShortBytes(execute.QueryId)
	if hasResultMetadataId(version) {
		size += primitives.LengthOfShortBytes(execute.ResultMetadataId)
	}
	if lengthOfQueryOptions, err := LengthOfQueryOptions(execute.Options, version); err == nil {
		return size + lengthOfQueryOptions, nil
	} else {
		return -1, fmt.Errorf("cannot compute size EXECUTE query options: %w", err)
	}
}

func (c *ExecuteCodec) Decode(source io.Reader, version primitives.ProtocolVersion) (msg Message, err error) {
	var execute = &Execute{
		Options: nil,
	}
	if execute.QueryId, err = primitives.ReadShortBytes(source); err != nil {
		return nil, fmt.Errorf("cannot read EXECUTE query id: %w", err)
	} else if len(execute.QueryId) == 0 {
		return nil, errors.New("EXECUTE missing query id")
	}
	if hasResultMetadataId(version) {
		if execute.ResultMetadataId, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read EXECUTE result metadata id: %w", err)
		} else if len(execute.ResultMetadataId) == 0 {
			return nil, errors.New("EXECUTE missing result metadata id")
		}
	}
	if execute.Options, err = DecodeQueryOptions(source, version); err != nil {
		return nil, fmt.Errorf("cannot read EXECUTE query options: %w", err)
	}
	return execute, nil
}

func (c *ExecuteCodec) GetOpCode() primitives.OpCode {
	return primitives.OpCodeExecute
}
