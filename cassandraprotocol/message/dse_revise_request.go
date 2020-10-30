package message

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"io"
)

// Revise was called CANCEL in DSE protocol version 1 and was renamed to REVISE_REQUEST in version 2.
type Revise struct {
	RevisionType   primitive.DseRevisionType
	TargetStreamId int32
	// The number of pages that the client is ready to receive, or zero to indicate no limit.
	// Valid for DSE v2 only when RevisionType is 2 (DseRevisionTypeMoreContinuousPages).
	// See ContinuousPagingOptions.
	NextPages int32
}

func (m *Revise) IsResponse() bool {
	return false
}

func (m *Revise) GetOpCode() primitive.OpCode {
	return primitive.OpCodeDseRevise
}

func (m *Revise) String() string {
	return fmt.Sprintf("REVISE_REQUEST operation type: %v, stream id: %v", m.RevisionType, m.TargetStreamId)
}

type ReviseCodec struct{}

func (c *ReviseCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) error {
	revise, ok := msg.(*Revise)
	if !ok {
		return fmt.Errorf("expected *message.Revise, got %T", msg)
	}
	if err := primitive.CheckDseProtocolVersion(version); err != nil {
		return err
	} else if err := primitive.CheckDseRevisionType(revise.RevisionType); err != nil {
		return err
	} else if err := primitive.WriteInt(revise.RevisionType, dest); err != nil {
		return fmt.Errorf("cannot write REVISE/CANCEL revision type: %w", err)
	}
	if err := primitive.WriteInt(revise.TargetStreamId, dest); err != nil {
		return fmt.Errorf("cannot write REVISE/CANCEL target stream id: %w", err)
	}
	switch revise.RevisionType {
	case primitive.DseRevisionTypeMoreContinuousPages:
		if err := primitive.WriteInt(revise.NextPages, dest); err != nil {
			return fmt.Errorf("cannot write REVISE/CANCEL next pages: %w", err)
		}
	}
	return nil
}

func (c *ReviseCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (length int, err error) {
	if err := primitive.CheckDseProtocolVersion(version); err != nil {
		return -1, err
	}
	revise, ok := msg.(*Revise)
	if !ok {
		return -1, fmt.Errorf("expected *message.Revise, got %T", msg)
	}
	length += primitive.LengthOfInt // revision type
	length += primitive.LengthOfInt // stream id
	switch revise.RevisionType {
	case primitive.DseRevisionTypeMoreContinuousPages:
		length += primitive.LengthOfInt // next pages
	}
	return length, nil
}

func (c *ReviseCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	if err := primitive.CheckDseProtocolVersion(version); err != nil {
		return nil, err
	}
	revise := &Revise{}
	if revise.RevisionType, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read REVISE/CANCEL revision type: %w", err)
	} else if err := primitive.CheckDseRevisionType(revise.RevisionType); err != nil {
		return nil, err
	} else if revise.TargetStreamId, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read REVISE/CANCEL target stream id: %w", err)
	}
	switch revise.RevisionType {
	case primitive.DseRevisionTypeMoreContinuousPages:
		if revise.NextPages, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read REVISE/CANCEL next pages: %w", err)
		}
	}
	return revise, nil
}

func (c *ReviseCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeDseRevise
}
