package frame

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

func (c *Codec) Encode(frame *Frame, dest io.Writer) error {
	version := frame.Header.Version
	if err := cassandraprotocol.CheckProtocolVersion(version); err != nil {
		return err
	}
	if version < cassandraprotocol.ProtocolVersion4 && frame.Body.CustomPayload != nil {
		return fmt.Errorf("custom payloads are not supported in protocol version %v", version)
	}
	if version < cassandraprotocol.ProtocolVersion4 && frame.Body.Warnings != nil {
		return fmt.Errorf("warnings are not supported in protocol version %v", version)
	}
	if c.compressor != nil && frame.IsCompressible() {
		return c.encodeFrameCompressed(frame, dest)
	} else {
		return c.encodeFrameUncompressed(frame, dest)
	}
}

func (c *Codec) findEncoder(frame *Frame) (encoder message.Encoder, err error) {
	opCode := frame.Body.Message.GetOpCode()
	encoder, found := c.messageCodecs[opCode]
	if !found {
		err = errors.New(fmt.Sprintf("unsupported opcode %d", opCode))
	}
	return encoder, err
}

func (c *Codec) encodeFrameUncompressed(frame *Frame, dest io.Writer) error {
	var err error
	var encodedBodyLength int
	if encodedBodyLength, err = c.uncompressedBodyLength(frame); err != nil {
		return fmt.Errorf("cannot compute length of uncompressed message body: %w", err)
	}
	if err = c.encodeHeader(frame, encodedBodyLength, dest); err != nil {
		return fmt.Errorf("cannot encode frame header: %w", err)
	}
	if err = c.encodeBodyUncompressed(frame, dest); err != nil {
		return fmt.Errorf("cannot encode frame body: %w", err)
	}
	return nil
}

func (c *Codec) encodeFrameCompressed(frame *Frame, dest io.Writer) error {
	var err error
	// 1) Encode uncompressed body
	var uncompressedBodyLength int
	if uncompressedBodyLength, err = c.uncompressedBodyLength(frame); err != nil {
		return fmt.Errorf("cannot compute length of uncompressed message body: %w", err)
	}
	uncompressedBody := bytes.NewBuffer(make([]byte, 0, uncompressedBodyLength))
	if err = c.encodeBodyUncompressed(frame, uncompressedBody); err != nil {
		return fmt.Errorf("cannot encode frame body: %w", err)
	}
	// 2) Compress body
	var compressedBody *bytes.Buffer
	if compressedBody, err = c.compressor.Compress(uncompressedBody); err != nil {
		return fmt.Errorf("cannot compress frame body: %w", err)
	}
	compressedBodyLength := compressedBody.Len()
	// 3) Encode header
	if err = c.encodeHeader(frame, compressedBodyLength, dest); err != nil {
		return fmt.Errorf("cannot encode frame header: %w", err)
	}
	// 4) join header and compressed body
	if _, err := compressedBody.WriteTo(dest); err != nil {
		return fmt.Errorf("cannot concat frame body to frame header: %w", err)
	}
	return nil
}

func (c *Codec) encodeHeader(frame *Frame, bodyLength int, dest io.Writer) error {
	versionAndDirection := frame.Header.Version
	if frame.Body.Message.IsResponse() {
		versionAndDirection |= 0b1000_0000
	}
	if err := primitives.WriteByte(versionAndDirection, dest); err != nil {
		return fmt.Errorf("cannot encode header version and direction: %w", err)
	}
	flags := frame.Flags(c.compressor != nil)
	if err := primitives.WriteByte(flags, dest); err != nil {
		return fmt.Errorf("cannot encode header flags: %w", err)
	} else if err = primitives.WriteShort(uint16(frame.Header.StreamId), dest); err != nil {
		return fmt.Errorf("cannot encode header stream id: %w", err)
	} else if err = primitives.WriteByte(frame.Body.Message.GetOpCode(), dest); err != nil {
		return fmt.Errorf("cannot encode header opcode: %w", err)
	} else if err = primitives.WriteInt(int32(bodyLength), dest); err != nil {
		return fmt.Errorf("cannot encode header body length: %w", err)
	}
	return nil
}

func (c *Codec) uncompressedBodyLength(frame *Frame) (length int, err error) {
	if encoder, err := c.findEncoder(frame); err != nil {
		return -1, err
	} else if length, err = encoder.EncodedLength(frame.Body.Message, frame.Header.Version); err != nil {
		return -1, fmt.Errorf("cannot compute message length: %w", err)
	}
	if frame.Body.TracingId != nil {
		length += primitives.LengthOfUuid
	}
	if frame.Body.CustomPayload != nil {
		length += primitives.LengthOfBytesMap(frame.Body.CustomPayload)
	}
	if frame.Body.Warnings != nil {
		length += primitives.LengthOfStringList(frame.Body.Warnings)
	}
	return length, nil
}

func (c *Codec) encodeBodyUncompressed(frame *Frame, dest io.Writer) error {
	var err error
	if frame.Body.Message.IsResponse() && frame.Body.TracingId != nil {
		if err = primitives.WriteUuid(frame.Body.TracingId, dest); err != nil {
			return fmt.Errorf("cannot encode body tracing id: %w", err)
		}
	}
	if frame.Body.CustomPayload != nil {
		if err = primitives.WriteBytesMap(frame.Body.CustomPayload, dest); err != nil {
			return fmt.Errorf("cannot encode body custom payload: %w", err)
		}
	}
	if frame.Body.Warnings != nil {
		if err = primitives.WriteStringList(frame.Body.Warnings, dest); err != nil {
			return fmt.Errorf("cannot encode body warnings: %w", err)
		}
	}
	if encoder, err := c.findEncoder(frame); err != nil {
		return err
	} else if err = encoder.Encode(frame.Body.Message, dest, frame.Header.Version); err != nil {
		return fmt.Errorf("cannot encode body message: %w", err)
	}
	return nil
}
