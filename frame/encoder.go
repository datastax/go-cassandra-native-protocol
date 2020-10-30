package frame

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

// EncodeFrame encodes the entire frame, compressing the body if needed.
func (c *Codec) EncodeFrame(frame *Frame, dest io.Writer) error {
	version := frame.Header.Version
	if err := primitive.CheckProtocolVersion(version); err != nil {
		return err
	}
	if version < primitive.ProtocolVersion4 && frame.Body.CustomPayload != nil {
		return fmt.Errorf("custom payloads are not supported in protocol version %v", version)
	}
	if version < primitive.ProtocolVersion4 && frame.Body.Warnings != nil {
		return fmt.Errorf("warnings are not supported in protocol version %v", version)
	}
	if c.compressor != nil && frame.IsCompressible() {
		return c.encodeFrameCompressed(frame, dest)
	} else {
		return c.encodeFrameUncompressed(frame, dest)
	}
}

// EncodeRawFrame encodes the header and writes the body as raw bytes.
func (c *Codec) EncodeRawFrame(frame *RawFrame, dest io.Writer) error {
	version := frame.RawHeader.Version
	if err := primitive.CheckProtocolVersion(version); err != nil {
		return err
	}
	if err := c.encodeRawHeader(frame.RawHeader, dest); err != nil {
		return fmt.Errorf("cannot encode raw header: %w", err)
	}
	if bytesWritten, err := dest.Write(frame.RawBody); err != nil {
		return fmt.Errorf(
			"cannot write body: %w, body length: %d, bytes written: %d", err, len(frame.RawBody), bytesWritten)
	}
	return nil
}

// ConvertToRawFrame converts a Frame to a RawFrame, encoding the body and compressing it if necessary.
func (c *Codec) ConvertToRawFrame(frame *Frame) (*RawFrame, error) {
	var body *bytes.Buffer
	var err error

	if c.compressor != nil && frame.IsCompressible() {
		body, err = c.encodeBodyCompressed(frame)
	} else {
		body = &bytes.Buffer{}
		err = c.encodeBodyUncompressed(frame, body)
	}

	if err != nil {
		return nil, fmt.Errorf("cannot encode body: %w", err)
	}

	bodyBytes := body.Bytes()
	return &RawFrame{
		RawHeader: c.convertToRawHeader(frame, int32(len(bodyBytes))),
		RawBody:   bodyBytes,
	}, nil
}

func (c *Codec) convertToRawHeader(frame *Frame, bodyLength int32) *RawHeader {
	return &RawHeader{
		IsResponse: frame.Body.Message.IsResponse(),
		Version:    frame.Header.Version,
		Flags:      frame.Flags(c.compressor != nil),
		StreamId:   frame.Header.StreamId,
		OpCode:     frame.Body.Message.GetOpCode(),
		BodyLength: bodyLength,
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
	var compressedBody *bytes.Buffer
	var err error
	if compressedBody, err = c.encodeBodyCompressed(frame); err != nil {
		return fmt.Errorf("cannot encode and compress body: %w", err)
	}
	compressedBodyLength := compressedBody.Len()

	if err := c.encodeHeader(frame, compressedBodyLength, dest); err != nil {
		return fmt.Errorf("cannot encode frame header: %w", err)
	}

	if _, err := compressedBody.WriteTo(dest); err != nil {
		return fmt.Errorf("cannot concat frame body to frame header: %w", err)
	}
	return nil
}

func (c *Codec) encodeBodyCompressed(frame *Frame) (*bytes.Buffer, error) {
	var err error

	var uncompressedBodyLength int
	if uncompressedBodyLength, err = c.uncompressedBodyLength(frame); err != nil {
		return nil, fmt.Errorf("cannot compute length of uncompressed message body: %w", err)
	}
	uncompressedBody := bytes.NewBuffer(make([]byte, 0, uncompressedBodyLength))
	if err = c.encodeBodyUncompressed(frame, uncompressedBody); err != nil {
		return nil, fmt.Errorf("cannot encode frame body: %w", err)
	}

	var compressedBody *bytes.Buffer
	if compressedBody, err = c.compressor.Compress(uncompressedBody); err != nil {
		return nil, fmt.Errorf("cannot compress frame body: %w", err)
	}

	return compressedBody, nil
}

func (c *Codec) encodeHeader(frame *Frame, bodyLength int, dest io.Writer) error {
	versionAndDirection := frame.Header.Version
	if frame.Body.Message.IsResponse() {
		versionAndDirection |= 0b1000_0000
	}
	if err := primitive.WriteByte(versionAndDirection, dest); err != nil {
		return fmt.Errorf("cannot encode header version and direction: %w", err)
	}
	flags := frame.Flags(c.compressor != nil)
	if err := primitive.WriteByte(flags, dest); err != nil {
		return fmt.Errorf("cannot encode header flags: %w", err)
	} else if err = primitive.WriteShort(uint16(frame.Header.StreamId), dest); err != nil {
		return fmt.Errorf("cannot encode header stream id: %w", err)
	} else if err = primitive.WriteByte(frame.Body.Message.GetOpCode(), dest); err != nil {
		return fmt.Errorf("cannot encode header opcode: %w", err)
	} else if err = primitive.WriteInt(int32(bodyLength), dest); err != nil {
		return fmt.Errorf("cannot encode header body length: %w", err)
	}
	return nil
}

func (c *Codec) encodeRawHeader(rawHeader *RawHeader, dest io.Writer) error {
	versionAndDirection := rawHeader.Version
	if rawHeader.IsResponse {
		versionAndDirection |= 0b1000_0000
	}
	if err := primitive.WriteByte(versionAndDirection, dest); err != nil {
		return fmt.Errorf("cannot encode header version and direction: %w", err)
	}
	flags := rawHeader.Flags
	if err := primitive.WriteByte(flags, dest); err != nil {
		return fmt.Errorf("cannot encode header flags: %w", err)
	} else if err = primitive.WriteShort(uint16(rawHeader.StreamId), dest); err != nil {
		return fmt.Errorf("cannot encode header stream id: %w", err)
	} else if err = primitive.WriteByte(rawHeader.OpCode, dest); err != nil {
		return fmt.Errorf("cannot encode header opcode: %w", err)
	} else if err = primitive.WriteInt(rawHeader.BodyLength, dest); err != nil {
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
		length += primitive.LengthOfUuid
	}
	if frame.Body.CustomPayload != nil {
		length += primitive.LengthOfBytesMap(frame.Body.CustomPayload)
	}
	if frame.Body.Warnings != nil {
		length += primitive.LengthOfStringList(frame.Body.Warnings)
	}
	return length, nil
}

func (c *Codec) encodeBodyUncompressed(frame *Frame, dest io.Writer) error {
	var err error
	if frame.Body.Message.IsResponse() && frame.Body.TracingId != nil {
		if err = primitive.WriteUuid(frame.Body.TracingId, dest); err != nil {
			return fmt.Errorf("cannot encode body tracing id: %w", err)
		}
	}
	if frame.Body.CustomPayload != nil {
		if err = primitive.WriteBytesMap(frame.Body.CustomPayload, dest); err != nil {
			return fmt.Errorf("cannot encode body custom payload: %w", err)
		}
	}
	if frame.Body.Warnings != nil {
		if err = primitive.WriteStringList(frame.Body.Warnings, dest); err != nil {
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
