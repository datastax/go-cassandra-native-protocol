package frame

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
	"io/ioutil"
)

func (c *codec) DecodeFrame(source io.Reader) (*Frame, error) {
	if header, err := c.DecodeHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else if body, err := c.DecodeBody(header, source); err != nil {
		return nil, fmt.Errorf("cannot decode frame body: %w", err)
	} else {
		return &Frame{Header: header, Body: body}, nil
	}
}

func (c *codec) DecodeRawFrame(source io.Reader) (*RawFrame, error) {
	if header, err := c.DecodeHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else if body, err := c.DecodeRawBody(header, source); err != nil {
		return nil, fmt.Errorf("cannot read frame body: %w", err)
	} else {
		return &RawFrame{Header: header, Body: body}, nil
	}
}

func (c *codec) DecodeHeader(source io.Reader) (*Header, error) {
	if versionAndDirection, err := primitive.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot decode header version and direction: %w", err)
	} else {
		isResponse := (versionAndDirection & 0b1000_0000) > 0
		version := versionAndDirection & 0b0111_1111
		header := &Header{
			IsResponse: isResponse,
			Version:    version,
		}
		var streamId uint16
		if err := primitive.CheckProtocolVersion(version); err != nil {
			return nil, err
		} else if header.Flags, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot decode header flags: %w", err)
		} else if primitive.IsProtocolVersionBeta(version) && header.Flags&primitive.HeaderFlagUseBeta == 0 {
			return nil, fmt.Errorf("expected USE_BETA flag to be set for protocol version %v", version)
		} else if streamId, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot decode header stream id: %w", err)
		} else if header.OpCode, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot decode header opcode: %w", err)
		} else if header.BodyLength, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot decode header body length: %w", err)
		}
		header.StreamId = int16(streamId)
		return header, err
	}
}

func (c *codec) DecodeBody(header *Header, source io.Reader) (body *Body, err error) {
	if compressed := header.Flags&primitive.HeaderFlagCompressed > 0; compressed {
		if c.compressor == nil {
			return nil, errors.New("cannot decompress body: no compressor available")
		} else if source, err = c.decompressBody(header.BodyLength, source); err != nil {
			return nil, fmt.Errorf("cannot decompress body: %w", err)
		}
	}
	body = &Body{}
	if header.IsResponse && header.Flags&primitive.HeaderFlagTracing > 0 {
		if body.TracingId, err = primitive.ReadUuid(source); err != nil {
			return nil, fmt.Errorf("cannot decode body tracing id: %w", err)
		}
	}
	if header.Flags&primitive.HeaderFlagCustomPayload > 0 {
		if body.CustomPayload, err = primitive.ReadBytesMap(source); err != nil {
			return nil, fmt.Errorf("cannot decode body custom payload: %w", err)
		}
	}
	if header.IsResponse && header.Flags&primitive.HeaderFlagWarning > 0 {
		if body.Warnings, err = primitive.ReadStringList(source); err != nil {
			return nil, fmt.Errorf("cannot decode body warnings: %w", err)
		}
	}
	if decoder, err := c.findMessageCodec(header.OpCode); err != nil {
		return nil, err
	} else if body.Message, err = decoder.Decode(source, header.Version); err != nil {
		return nil, fmt.Errorf("cannot decode body message: %w", err)
	}
	return body, err
}

func (c *codec) DecodeRawBody(header *Header, source io.Reader) (body RawBody, err error) {
	if header.BodyLength < 0 {
		return nil, fmt.Errorf("invalid body length: %d", header.BodyLength)
	} else if header.BodyLength == 0 {
		return []byte{}, nil
	}
	count := int64(header.BodyLength)
	buf := &bytes.Buffer{}
	if bytesRead, err := io.CopyN(buf, source, count); err != nil {
		return nil, fmt.Errorf("cannot copy source reader: %w, body length: %d, bytes read: %d", err, count, bytesRead)
	}
	return buf.Bytes(), nil
}

func (c *codec) DiscardBody(header *Header, source io.Reader) (err error) {
	if header.BodyLength < 0 {
		return fmt.Errorf("invalid body length: %d", header.BodyLength)
	} else if header.BodyLength == 0 {
		return nil
	}
	count := int64(header.BodyLength)
	switch r := source.(type) {
	case io.Seeker:
		_, err = r.Seek(count, io.SeekCurrent)
	default:
		_, err = io.CopyN(ioutil.Discard, r, count)
	}
	return err
}

// decompressBody decompresses a compressed frame body and returns a new bytes.Buffer containing the decompressed body.
// The original io.Reader will be fully consumed and should be discarded after calling this method.
func (c *codec) decompressBody(compressedBodyLength int32, source io.Reader) (*bytes.Buffer, error) {
	compressedBody := bytes.Buffer{}
	count := int64(compressedBodyLength)
	if actualBodyLength, err := io.CopyN(&compressedBody, source, count); err != nil {
		return nil, fmt.Errorf("cannot copy source reader: %w, body length in header: %d, bytes read: %d", err, count, actualBodyLength)
	} else if decompressedBody, err := c.compressor.Decompress(&compressedBody); err != nil {
		return nil, fmt.Errorf("cannot decompress frame body: %w", err)
	} else {
		return decompressedBody, nil
	}
}
