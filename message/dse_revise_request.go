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

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Revise was called CANCEL in DSE protocol version 1 and was renamed to REVISE_REQUEST in version 2.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
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

type reviseCodec struct{}

func (c *reviseCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) error {
	revise, ok := msg.(*Revise)
	if !ok {
		return fmt.Errorf("expected *message.Revise, got %T", msg)
	}
	if err := primitive.CheckDseProtocolVersion(version); err != nil {
		return err
	} else if err := primitive.CheckValidDseRevisionType(revise.RevisionType, version); err != nil {
		return err
	} else if err := primitive.WriteInt(int32(revise.RevisionType), dest); err != nil {
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

func (c *reviseCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (length int, err error) {
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

func (c *reviseCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	if err := primitive.CheckDseProtocolVersion(version); err != nil {
		return nil, err
	}
	revise := &Revise{}
	var revisionType int32
	if revisionType, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read REVISE/CANCEL revision type: %w", err)
	}
	revise.RevisionType = primitive.DseRevisionType(revisionType)
	if err := primitive.CheckValidDseRevisionType(revise.RevisionType, version); err != nil {
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

func (c *reviseCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeDseRevise
}
