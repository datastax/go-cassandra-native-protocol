package message

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

// Revise was called CANCEL in DSE protocol version 1 and was renamed to REVISE_REQUEST in version 2.
type Revise struct {
	RevisionType   cassandraprotocol.DseRevisionType
	TargetStreamId int32
	// The number of pages that the client is ready to receive, or zero to indicate no limit.
	//Valid for DSE v2 only when RevisionType is 2 (DseRevisionTypeMoreContinuousPages).
	// See ContinuousPagingOptions.
	NextPages int32
}

func (m *Revise) IsResponse() bool {
	return false
}

func (m *Revise) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeDseRevise
}

func (m *Revise) String() string {
	return fmt.Sprintf("CANCEL operation type: %v, stream id: %v", m.RevisionType, m.TargetStreamId)
}

type ReviseCodec struct{}

func (c *ReviseCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) error {
	revise, ok := msg.(*Revise)
	if !ok {
		return fmt.Errorf("expected *message.Revise, got %T", msg)
	}
	if err := cassandraprotocol.CheckDseProtocolVersion(version); err != nil {
		return err
	} else if err := cassandraprotocol.CheckDseRevisionType(revise.RevisionType); err != nil {
		return err
	} else if err := primitives.WriteInt(revise.RevisionType, dest); err != nil {
		return fmt.Errorf("cannot write REVISE/CANCEL revision type: %w", err)
	}
	if err := primitives.WriteInt(revise.TargetStreamId, dest); err != nil {
		return fmt.Errorf("cannot write REVISE/CANCEL target stream id: %w", err)
	}
	switch revise.RevisionType {
	case cassandraprotocol.DseRevisionTypeMoreContinuousPages:
		if err := primitives.WriteInt(revise.NextPages, dest); err != nil {
			return fmt.Errorf("cannot write REVISE/CANCEL next pages: %w", err)
		}
	}
	return nil
}

func (c *ReviseCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	if err := cassandraprotocol.CheckDseProtocolVersion(version); err != nil {
		return -1, err
	}
	revise, ok := msg.(*Revise)
	if !ok {
		return -1, fmt.Errorf("expected *message.Revise, got %T", msg)
	}
	length += primitives.LengthOfInt // revision type
	length += primitives.LengthOfInt // stream id
	switch revise.RevisionType {
	case cassandraprotocol.DseRevisionTypeMoreContinuousPages:
		length += primitives.LengthOfInt // next pages
	}
	return length, nil
}

func (c *ReviseCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	if err := cassandraprotocol.CheckDseProtocolVersion(version); err != nil {
		return nil, err
	}
	revise := &Revise{}
	if revise.RevisionType, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read REVISE/CANCEL revision type: %w", err)
	} else if err := cassandraprotocol.CheckDseRevisionType(revise.RevisionType); err != nil {
		return nil, err
	} else if revise.TargetStreamId, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read REVISE/CANCEL target stream id: %w", err)
	}
	switch revise.RevisionType {
	case cassandraprotocol.DseRevisionTypeMoreContinuousPages:
		if revise.NextPages, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read REVISE/CANCEL next pages: %w", err)
		}
	}
	return revise, nil
}

func (c *ReviseCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeDseRevise
}
