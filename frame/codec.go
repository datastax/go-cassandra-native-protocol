package frame

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Encoder interface {

	// EncodeFrame encodes the entire frame, compressing the body if needed.
	EncodeFrame(frame *Frame, dest io.Writer) error
}

type RawEncoder interface {

	// EncodeRawFrame encodes the entire frame; the body is simply copied from the frame's raw body.
	EncodeRawFrame(frame *RawFrame, dest io.Writer) error

	// EncodeHeader only encodes the frame header. After calling this method, one must call EncodeBody to encode the
	// to fully encode the entire frame.
	EncodeHeader(header *Header, dest io.Writer) error

	// EncodeBody decodes a frame body, compressing it if required depending on whether the Compressed flag is set in the
	// header. It is illegal to call this method before calling EncodeHeader.
	EncodeBody(header *Header, body *Body, dest io.Writer) error
}

type Decoder interface {

	// DecodeFrame decodes the entire frame, decompressing the body if needed, and returns a Frame.
	DecodeFrame(source io.Reader) (*Frame, error)
}

type RawDecoder interface {

	// DecodeRawFrame decodes the header and reads the body as raw bytes, returning a RawFrame.
	DecodeRawFrame(source io.Reader) (*RawFrame, error)

	// DecodeHeader only decodes the frame header, leaving the body contents in the source. After calling this method,
	// one must either call DecodeBody, DecodeRawBody or DiscardBody to fully read or discard the body contents.
	DecodeHeader(source io.Reader) (*Header, error)

	// DecodeBody decodes a frame body, decompressing it if required. It is illegal to call this method before calling
	// DecodeHeader.
	DecodeBody(header *Header, source io.Reader) (*Body, error)

	// DecodeRawBody reads the contents of a frame body without decoding them. It is illegal to call this method before
	// calling DecodeHeader.
	DecodeRawBody(header *Header, source io.Reader) (RawBody, error)

	// DiscardBody discards the contents of a frame body. It is illegal to call this method before calling DecodeHeader.
	DiscardBody(header *Header, source io.Reader) error
}

type RawConverter interface {

	// ConvertToRawFrame converts a Frame to a RawFrame, encoding the body and compressing it if necessary. The
	// returned RawFrame will share the same header with the initial Frame.
	ConvertToRawFrame(frame *Frame) (*RawFrame, error)

	// ConvertFromRawFrame converts a RawFrame to a Frame, decoding the body and decompressing it if necessary. The
	// returned Frame will share the same header with the initial RawFrame.
	ConvertFromRawFrame(frame *RawFrame) (*Frame, error)
}

type Codec interface {
	Encoder
	Decoder
	CompressionAlgorithm() string
}

type RawCodec interface {
	Codec
	RawEncoder
	RawDecoder
	RawConverter
}

type codec struct {
	compressor    BodyCompressor
	messageCodecs map[primitive.OpCode]message.Codec
}

type CodecCustomizer func(*codec)

func NewCodec(compressor BodyCompressor, messageCodecs ...message.Codec) Codec {
	return NewRawCodec(compressor, messageCodecs...)
}

func NewRawCodec(compressor BodyCompressor, messageCodecs ...message.Codec) RawCodec {
	frameCodec := &codec{
		messageCodecs: make(map[primitive.OpCode]message.Codec, len(message.DefaultMessageCodecs)),
		compressor:    compressor,
	}
	for _, messageCodec := range message.DefaultMessageCodecs {
		frameCodec.messageCodecs[messageCodec.GetOpCode()] = messageCodec
	}
	for _, messageCodec := range messageCodecs {
		frameCodec.messageCodecs[messageCodec.GetOpCode()] = messageCodec
	}
	return frameCodec
}

func (c *codec) CompressionAlgorithm() string {
	if c.compressor == nil {
		return "NONE"
	} else {
		return c.compressor.Algorithm()
	}
}

func (c *codec) findMessageCodec(opCode primitive.OpCode) (message.Codec, error) {
	if encoder, found := c.messageCodecs[opCode]; !found {
		return nil, fmt.Errorf("unsupported opcode %d", opCode)
	} else {
		return encoder, nil
	}
}
