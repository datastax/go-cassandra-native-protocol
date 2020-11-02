package frame

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

func (c *codec) EncodeFrame(frame *Frame, dest io.Writer) error {
	if frame.Header.Flags&primitive.HeaderFlagCompressed == 0 {
		return c.encodeFrameUncompressed(frame, dest)
	} else {
		return c.encodeFrameCompressed(frame, dest)
	}
}

func (c *codec) encodeFrameUncompressed(frame *Frame, dest io.Writer) error {
	if encodedBodyLength, err := c.uncompressedBodyLength(frame.Header, frame.Body); err != nil {
		return fmt.Errorf("cannot compute length of uncompressed message body: %w", err)
	} else {
		frame.Header.BodyLength = int32(encodedBodyLength)
	}
	if err := c.EncodeHeader(frame.Header, dest); err != nil {
		return fmt.Errorf("cannot encode frame header: %w", err)
	} else if err := c.EncodeBody(frame.Header, frame.Body, dest); err != nil {
		return fmt.Errorf("cannot encode frame body: %w", err)
	}
	return nil
}

func (c *codec) encodeFrameCompressed(frame *Frame, dest io.Writer) error {
	compressedBody := bytes.Buffer{}
	if err := c.EncodeBody(frame.Header, frame.Body, &compressedBody); err != nil {
		return fmt.Errorf("cannot encode and compress body: %w", err)
	} else {
		frame.Header.BodyLength = int32(compressedBody.Len())
		if err := c.EncodeHeader(frame.Header, dest); err != nil {
			return fmt.Errorf("cannot encode frame header: %w", err)
		} else if _, err := compressedBody.WriteTo(dest); err != nil {
			return fmt.Errorf("cannot concat frame body to frame header: %w", err)
		}
	}
	return nil
}

// EncodeRawFrame encodes the header and writes the body as raw bytes.
func (c *codec) EncodeRawFrame(frame *RawFrame, dest io.Writer) error {
	if err := primitive.CheckProtocolVersion(frame.Header.Version); err != nil {
		return err
	} else {
		frame.Header.BodyLength = int32(len(frame.Body))
		if err := c.EncodeHeader(frame.Header, dest); err != nil {
			return fmt.Errorf("cannot encode raw header: %w", err)
		} else if _, err := dest.Write(frame.Body); err != nil {
			return fmt.Errorf("cannot write raw body: %w", err)
		}
	}
	return nil
}

func (c *codec) EncodeHeader(header *Header, dest io.Writer) error {
	if err := primitive.CheckProtocolVersion(header.Version); err != nil {
		return err
	}
	versionAndDirection := header.Version
	if header.IsResponse {
		versionAndDirection |= 0b1000_0000
	}
	if err := primitive.WriteByte(versionAndDirection, dest); err != nil {
		return fmt.Errorf("cannot encode header version and direction: %w", err)
	} else if err := primitive.WriteByte(header.Flags, dest); err != nil {
		return fmt.Errorf("cannot encode header flags: %w", err)
	} else if err = primitive.WriteShort(uint16(header.StreamId), dest); err != nil {
		return fmt.Errorf("cannot encode header stream id: %w", err)
	} else if err = primitive.WriteByte(header.OpCode, dest); err != nil {
		return fmt.Errorf("cannot encode header opcode: %w", err)
	} else if err = primitive.WriteInt(header.BodyLength, dest); err != nil {
		return fmt.Errorf("cannot encode header body length: %w", err)
	}
	return nil
}

func (c *codec) EncodeBody(header *Header, body *Body, dest io.Writer) error {
	if header.OpCode != body.Message.GetOpCode() {
		return fmt.Errorf("opcode mismatch between header and body: %d != %d", header.OpCode, body.Message.GetOpCode())
	} else if header.Flags&primitive.HeaderFlagCompressed == 0 {
		return c.encodeBodyUncompressed(header, body, dest)
	} else {
		if c.compressor == nil {
			return errors.New("cannot compress body: no compressor available")
		} else if uncompressedBodyLength, err := c.uncompressedBodyLength(header, body); err != nil {
			return fmt.Errorf("cannot compute length of uncompressed message body: %w", err)
		} else {
			uncompressedBody := bytes.NewBuffer(make([]byte, 0, uncompressedBodyLength))
			if err = c.encodeBodyUncompressed(header, body, uncompressedBody); err != nil {
				return fmt.Errorf("cannot encode body: %w", err)
			} else if err := c.compressor.Compress(uncompressedBody, dest); err != nil {
				return fmt.Errorf("cannot compress body: %w", err)
			}
			return nil
		}
	}
}

func (c *codec) encodeBodyUncompressed(header *Header, body *Body, dest io.Writer) (err error) {
	if header.Flags&primitive.HeaderFlagTracing != 0 && body.Message.IsResponse() {
		if err = primitive.WriteUuid(body.TracingId, dest); err != nil {
			return fmt.Errorf("cannot encode body tracing id: %w", err)
		}
	}
	if header.Flags&primitive.HeaderFlagCustomPayload != 0 {
		if header.Version < primitive.ProtocolVersion4 {
			return fmt.Errorf("custom payloads are not supported in protocol version %v", header.Version)
		} else if err = primitive.WriteBytesMap(body.CustomPayload, dest); err != nil {
			return fmt.Errorf("cannot encode body custom payload: %w", err)
		}
	}
	if header.Flags&primitive.HeaderFlagWarning != 0 {
		if header.Version < primitive.ProtocolVersion4 && body.Warnings != nil {
			return fmt.Errorf("warnings are not supported in protocol version %v", header.Version)
		} else if err = primitive.WriteStringList(body.Warnings, dest); err != nil {
			return fmt.Errorf("cannot encode body warnings: %w", err)
		}
	}
	if encoder, err := c.findMessageCodec(body.Message.GetOpCode()); err != nil {
		return err
	} else if err = encoder.Encode(body.Message, dest, header.Version); err != nil {
		return fmt.Errorf("cannot encode body message: %w", err)
	}
	return nil
}

func (c *codec) uncompressedBodyLength(header *Header, body *Body) (length int, err error) {
	if encoder, err := c.findMessageCodec(body.Message.GetOpCode()); err != nil {
		return -1, err
	} else if length, err = encoder.EncodedLength(body.Message, header.Version); err != nil {
		return -1, fmt.Errorf("cannot compute message length: %w", err)
	}
	if header.Flags&primitive.HeaderFlagTracing != 0 {
		length += primitive.LengthOfUuid
	}
	if header.Flags&primitive.HeaderFlagCustomPayload != 0 {
		length += primitive.LengthOfBytesMap(body.CustomPayload)
	}
	if header.Flags&primitive.HeaderFlagWarning != 0 {
		length += primitive.LengthOfStringList(body.Warnings)
	}
	return length, nil
}
