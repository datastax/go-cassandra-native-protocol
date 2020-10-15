package cassandraprotocol

import (
	"errors"
	"fmt"
)

type Frame struct {
	ProtocolVersion ProtocolVersion

	// The protocol spec states that the stream id is a [short], but this is wrong: the stream id
	// is signed and can be negative, which is why it has type int16.
	StreamId int16

	// Whether tracing should be activated for this request. Only valid for request frames, ignored for response frames.
	// Note that only QUERY, PREPARE and EXECUTE queries support tracing. Other requests will simply ignore the tracing flag if set.
	TracingRequested bool

	// The tracing ID. Only valid for response frames, ignored otherwise.
	TracingId *UUID

	CustomPayload map[string][]byte

	Warnings []string

	Message Message
}

func NewRequestFrame(protocolVersion ProtocolVersion, streamId int16, tracing bool, customPayload map[string][]byte, message Message) (*Frame, error) {
	if message.IsResponse() {
		panic("NewRequestFrame cannot be used with response messages")
	}
	return NewFrame(
		protocolVersion,
		streamId,
		tracing,
		nil,
		customPayload,
		nil,
		message)
}

func NewResponseFrame(protocolVersion ProtocolVersion, streamId int16, tracingId *UUID, customPayload map[string][]byte, warnings []string, message Message) (*Frame, error) {
	if !message.IsResponse() {
		panic("NewResponseFrame cannot be used with request messages")
	}
	return NewFrame(
		protocolVersion,
		streamId,
		tracingId != nil,
		tracingId,
		customPayload,
		warnings,
		message)
}

// NewFrame is mainly intended for internal use. If you want to build
// frames to pass for encoding, see NewRequestFrame or NewResponseFrame.
func NewFrame(protocolVersion ProtocolVersion, streamId int16, tracingRequested bool, tracingId *UUID, customPayload map[string][]byte, warnings []string, message Message) (*Frame, error) {
	if customPayload != nil && protocolVersion < ProtocolVersion4 {
		return nil, errors.New("custom payloads require protocol version 4 or higher")
	}
	if warnings != nil && protocolVersion < ProtocolVersion4 {
		return nil, errors.New("warnings require protocol version 4 or higher")
	}
	return &Frame{
		protocolVersion,
		streamId,
		tracingRequested,
		tracingId,
		customPayload,
		warnings,
		message,
	}, nil
}

type FrameCodec struct {
	MessageEncoders map[ProtocolVersion]map[OpCode]MessageEncoder
	MessageDecoders map[ProtocolVersion]map[OpCode]MessageDecoder
	Compressor      MessageCompressor
}

func NewFrameCodec(
	compressor MessageCompressor,
	messageCodecGroups ...MessageCodecGroup) *FrameCodec {
	messageEncoders := make(map[ProtocolVersion]map[OpCode]MessageEncoder)
	messageDecoders := make(map[ProtocolVersion]map[OpCode]MessageDecoder)
	for _, messageCodecGroup := range messageCodecGroups {
		protocolVersion := messageCodecGroup.ProtocolVersion
		for _, encoder := range messageCodecGroup.MessageEncoders {
			encoders, ok := messageEncoders[protocolVersion]
			if !ok {
				encoders = make(map[OpCode]MessageEncoder)
				messageEncoders[protocolVersion] = encoders
			}
			opCode := encoder.GetOpCode()
			encoders[opCode] = encoder
		}
		for _, decoder := range messageCodecGroup.MessageDecoders {
			decoders, ok := messageDecoders[protocolVersion]
			if !ok {
				decoders = make(map[OpCode]MessageDecoder)
				messageDecoders[protocolVersion] = decoders
			}
			opCode := decoder.GetOpCode()
			decoders[opCode] = decoder
		}
	}
	return &FrameCodec{MessageEncoders: messageEncoders, MessageDecoders: messageDecoders, Compressor: compressor}
}

// NewDefaultClientFrameCodec builds a new instance with the default codecs for a client
// (encoding requests, decoding responses).
func NewDefaultClientFrameCodec(compressor MessageCompressor) *FrameCodec {
	return NewFrameCodec(compressor, ProtocolV3ClientCodecs, ProtocolV4ClientCodecs, ProtocolV5ClientCodecs)
}

// NewDefaultServerFrameCodec builds a new instance with the default codecs for a server
// (decoding requests, encoding responses).
func NewDefaultServerFrameCodec(compressor MessageCompressor) *FrameCodec {
	return NewFrameCodec(compressor, ProtocolV3ServerCodecs, ProtocolV4ServerCodecs, ProtocolV5ServerCodecs)
}

const headerEncodedSize = 9

func (c *FrameCodec) Encode(frame *Frame) ([]byte, error) {

	protocolVersion := frame.ProtocolVersion
	message := frame.Message

	if protocolVersion < ProtocolVersion4 && frame.CustomPayload != nil {
		return nil, errors.New("custom payloads are not supported in protocol version " + string(protocolVersion))
	}

	if protocolVersion < ProtocolVersion4 && frame.Warnings != nil {
		return nil, errors.New("warnings are not supported in protocol version " + string(protocolVersion))
	}

	opCode := message.GetOpCode()
	encoder := c.MessageEncoders[protocolVersion][opCode]

	if encoder == nil {
		return nil, errors.New(fmt.Sprintf("unsupported opcode %d in protocol version %d", opCode, protocolVersion))
	}

	compress := c.Compressor != nil && opCode != OpCodeStartup && opCode != OpCodeOptions

	var flags ByteFlag = 0

	if compress {
		flags |= HeaderFlagCompressed
	}
	if message.IsResponse() && frame.TracingId != nil || !message.IsResponse() && frame.TracingRequested {
		flags |= HeaderFlagTracing
	}
	if frame.CustomPayload != nil {
		flags |= HeaderFlagCustomPayload
	}
	if frame.Warnings != nil {
		flags |= HeaderFlagWarning
	}
	if protocolVersion == ProtocolVersionBeta {
		flags |= HeaderFlagUseBeta
	}

	if !compress {
		// No compression: we can optimize and do everything with a single allocation
		messageSize, _ := encoder.EncodedSize(message, protocolVersion)
		if frame.TracingId != nil {
			messageSize += SizeOfUuid
		}
		if frame.CustomPayload != nil {
			messageSize += SizeOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			messageSize += SizeOfStringList(frame.Warnings)
		}
		encodedFrame := make([]byte, headerEncodedSize+messageSize)
		remaining := encodedFrame
		var err error
		remaining, err = encodeHeader(frame, flags, messageSize, remaining)
		if err != nil {
			return nil, err
		}
		if message.IsResponse() && frame.TracingId != nil {
			remaining, err = WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = WriteStringList(frame.Warnings, remaining)
			if err != nil {
				return nil, err
			}
		}
		err = encoder.Encode(message, remaining, protocolVersion)
		if err != nil {
			return nil, err
		}
		return encodedFrame, nil

	} else {
		// We need to compress first in order to know the body size
		// 1) Encode uncompressed message
		uncompressedMessageSize, _ := encoder.EncodedSize(message, protocolVersion)
		if frame.TracingId != nil {
			uncompressedMessageSize += SizeOfUuid
		}
		if frame.CustomPayload != nil {
			uncompressedMessageSize += SizeOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			uncompressedMessageSize += SizeOfStringList(frame.Warnings)
		}
		uncompressedMessage := make([]byte, uncompressedMessageSize)
		remaining := uncompressedMessage
		var err error
		if frame.TracingId != nil {
			remaining, err = WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = WriteStringList(frame.Warnings, remaining)
			if err != nil {
				return nil, err
			}
		}
		err = encoder.Encode(message, remaining, protocolVersion)
		if err != nil {
			return nil, err
		}
		// 2) Compress and measure size, discard uncompressed buffer
		var compressedMessage []byte
		compressedMessage, err = c.Compressor.Compress(uncompressedMessage)
		if err != nil {
			return nil, err
		}
		messageSize := len(compressedMessage)
		// 3) Encode final frame
		header := make([]byte, headerEncodedSize)
		_, err = encodeHeader(frame, flags, messageSize, header)
		if err != nil {
			return nil, err
		}
		encodedFrame := append(header, compressedMessage...)
		return encodedFrame, nil
	}
}

func encodeHeader(frame *Frame, flags ByteFlag, messageSize int, dest []byte) ([]byte, error) {
	versionAndDirection := frame.ProtocolVersion
	if frame.Message.IsResponse() {
		versionAndDirection |= 0b1000_0000
	}
	var err error
	dest, err = WriteByte(versionAndDirection, dest)
	if err != nil {
		return nil, err
	}
	dest, err = WriteByte(uint8(flags), dest)
	if err != nil {
		return nil, err
	}
	dest, err = WriteShort(uint16(frame.StreamId)&0xFFFF, dest)
	if err != nil {
		return nil, err
	}
	dest, err = WriteByte(uint8(frame.Message.GetOpCode()), dest)
	if err != nil {
		return nil, err
	}
	dest, err = WriteInt(int32(messageSize), dest)
	if err != nil {
		return nil, err
	}
	return dest, nil
}

func (c *FrameCodec) Decode(source []byte) (*Frame, error) {
	var b byte
	var err error
	b, source, err = ReadByte(source)
	if err != nil {
		return nil, err
	}
	isResponse := (b & 0b1000_0000) == 0b1000_0000
	protocolVersion := ProtocolVersion(b & 0b0111_1111)
	b, source, err = ReadByte(source)
	if err != nil {
		return nil, err
	}
	flags := ByteFlag(b)
	//beta := flags.contains(HeaderFlagUseBeta)
	var streamId uint16
	streamId, source, err = ReadShort(source)
	if err != nil {
		return nil, err
	}
	b, source, err = ReadByte(source)
	if err != nil {
		return nil, err
	}
	opCode := OpCode(b)
	var i int32
	i, source, err = ReadInt(source)
	if err != nil {
		return nil, err
	}
	length := int(i)
	actualLength := len(source)
	if length != actualLength {
		return nil, errors.New(fmt.Sprintf("Declared length in header (%d) does not match actual length (%d)",
			length,
			actualLength))
	}

	decompressed := flags&HeaderFlagCompressed > 0
	if decompressed {
		source, err = c.Compressor.Decompress(source)
		if err != nil {
			return nil, err
		}
	}

	var tracingId *UUID
	if isResponse && (flags&HeaderFlagTracing > 0) {
		tracingId, source, err = ReadUuid(source)
		if err != nil {
			return nil, err
		}
	}

	var customPayload map[string][]byte
	if flags&HeaderFlagCustomPayload > 0 {
		customPayload, source, err = ReadBytesMap(source)
		if err != nil {
			return nil, err
		}
	}

	var warnings []string
	if isResponse && (flags&HeaderFlagWarning > 0) {
		warnings, source, err = ReadStringList(source)
		if err != nil {
			return nil, err
		}
	}

	decoder := c.MessageDecoders[protocolVersion][opCode]
	if decoder == nil {
		return nil, errors.New(fmt.Sprintf("Unsupported request opcode: %d in protocol version %d", opCode, protocolVersion))
	}

	var response Message
	response, err = decoder.Decode(source, protocolVersion)
	if err != nil {
		return nil, err
	}

	frame := &Frame{
		protocolVersion,
		int16(streamId),
		tracingId != nil,
		tracingId,
		customPayload,
		warnings,
		response}
	return frame, nil
}
