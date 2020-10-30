package frame

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
	"io/ioutil"
)

// DecodeFrame decodes the entire frame, decompressing the body if needed.
func (c *Codec) DecodeFrame(source io.Reader) (*Frame, error) {
	if rawHeader, err := c.DecodeRawHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else if body, err := c.DecodeBody(rawHeader, source); err != nil {
		return nil, fmt.Errorf("cannot decode frame body: %w", err)
	} else {
		header := &Header{
			Version:          rawHeader.Version,
			StreamId:         rawHeader.StreamId,
			TracingRequested: body.TracingId != nil || rawHeader.Flags&primitives.HeaderFlagTracing != 0,
		}
		return &Frame{Header: header, Body: body}, nil
	}
}

// DecodeRawFrame decodes the header and reads the body as raw bytes, returning a RawFrame.
func (c *Codec) DecodeRawFrame(source io.Reader) (*RawFrame, error) {
	if rawHeader, err := c.DecodeRawHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else if body, err := c.DecodeRawBody(rawHeader.BodyLength, source); err != nil {
		return nil, fmt.Errorf("cannot read frame body: %w", err)
	} else {
		return &RawFrame{RawHeader: rawHeader, RawBody: body}, nil
	}
}

// DecodeRawHeader only decodes the frame header, leaving the body contents in the source. After calling this function,
// one must either call DecodeBody, DecodeRawBody or DiscardBody to fully read or discard the body contents.
func (c *Codec) DecodeRawHeader(source io.Reader) (*RawHeader, error) {
	if versionAndDirection, err := primitives.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot decode header version and direction: %w", err)
	} else {
		isResponse := (versionAndDirection & 0b1000_0000) > 0
		version := versionAndDirection & 0b0111_1111
		header := &RawHeader{
			IsResponse: isResponse,
			Version:    version,
		}
		var streamId uint16
		if err := primitives.CheckProtocolVersion(version); err != nil {
			return nil, err
		} else if header.Flags, err = primitives.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot decode header flags: %w", err)
		} else if primitives.IsProtocolVersionBeta(version) && header.Flags&primitives.HeaderFlagUseBeta == 0 {
			return nil, fmt.Errorf("expected USE_BETA flag to be set for protocol version %v", version)
		} else if streamId, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot decode header stream id: %w", err)
		} else if header.OpCode, err = primitives.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot decode header opcode: %w", err)
		} else if header.BodyLength, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot decode header body length: %w", err)
		}
		header.StreamId = int16(streamId)
		return header, err
	}
}

// DecodeBody decodes a frame body, decompressing it if required. It is illegal to call this method before calling
// DecodeRawHeader.
func (c *Codec) DecodeBody(header *RawHeader, source io.Reader) (body *Body, err error) {
	if compressed := header.Flags&primitives.HeaderFlagCompressed > 0; compressed {
		if c.compressor == nil {
			return nil, errors.New("cannot decompress body: no compressor available")
		} else if source, err = c.DecompressBody(header.BodyLength, source); err != nil {
			return nil, fmt.Errorf("cannot decompress body: %w", err)
		}
	}
	body = &Body{}
	if header.IsResponse && header.Flags&primitives.HeaderFlagTracing > 0 {
		if body.TracingId, err = primitives.ReadUuid(source); err != nil {
			return nil, fmt.Errorf("cannot decode body tracing id: %w", err)
		}
	}
	if header.Flags&primitives.HeaderFlagCustomPayload > 0 {
		if body.CustomPayload, err = primitives.ReadBytesMap(source); err != nil {
			return nil, fmt.Errorf("cannot decode body custom payload: %w", err)
		}
	}
	if header.IsResponse && header.Flags&primitives.HeaderFlagWarning > 0 {
		if body.Warnings, err = primitives.ReadStringList(source); err != nil {
			return nil, fmt.Errorf("cannot decode body warnings: %w", err)
		}
	}
	if decoder, found := c.messageCodecs[header.OpCode]; !found {
		return nil, errors.New(fmt.Sprintf("unsupported opcode %d", header.OpCode))
	} else if body.Message, err = decoder.Decode(source, header.Version); err != nil {
		return nil, fmt.Errorf("cannot decode body message: %w", err)
	}
	return body, err
}

// DiscardBody discards the contents of a frame body. It is illegal to call this method before calling
// DecodeRawHeader.
func (c *Codec) DiscardBody(bodyLength int32, source io.Reader) (err error) {
	count := int64(bodyLength)
	switch r := source.(type) {
	case io.Seeker:
		_, err = r.Seek(count, io.SeekCurrent)
	default:
		_, err = io.CopyN(ioutil.Discard, r, count)
	}
	return err
}

// DecodeRawBody reads the contents of a frame body without decoding them. It is illegal to call this method before calling
// DecodeRawHeader.
func (c *Codec) DecodeRawBody(bodyLength int32, source io.Reader) (body []byte, err error) {
	if bodyLength < 0 {
		return nil, fmt.Errorf("invalid body length: %d", bodyLength)
	} else if bodyLength == 0 {
		return []byte{}, nil
	}

	count := int64(bodyLength)
	buf := &bytes.Buffer{}
	if bytesRead, err := io.CopyN(buf, source, count); err != nil {
		return nil, fmt.Errorf("cannot copy source reader: %w, body length: %d, bytes read: %d", err, count, bytesRead)
	}
	return buf.Bytes(), nil
}

// DecompressBody decompresses a compressed frame body and returns a new bytes.Buffer containing the decompressed body.
// The original io.Reader will be fully consumed and should be discarded after calling this method.
func (c *Codec) DecompressBody(compressedBodyLength int32, source io.Reader) (*bytes.Buffer, error) {
	compressedBody := bytes.Buffer{}
	count := int64(compressedBodyLength)
	if actualBodyLength, err := io.CopyN(&compressedBody, source, count); err != nil {
		return nil, fmt.Errorf("cannot copy source reader: %w, body length in header: %d, bytes read: %d", err, count, actualBodyLength)
	}
	if decompressedBody, err := c.compressor.Decompress(&compressedBody); err != nil {
		return nil, fmt.Errorf("cannot decompress frame body: %w", err)
	} else {
		return decompressedBody, nil
	}
}
