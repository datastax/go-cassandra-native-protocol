package frame

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/message/codec"
)

type Codec struct {
	MessageEncoders map[cassandraprotocol.ProtocolVersion]map[cassandraprotocol.OpCode]codec.MessageEncoder
	MessageDecoders map[cassandraprotocol.ProtocolVersion]map[cassandraprotocol.OpCode]codec.MessageDecoder
	Compressor      cassandraprotocol.MessageCompressor
}

func NewCodec(
	compressor cassandraprotocol.MessageCompressor,
	messageCodecGroups ...MessageCodecGroup) *Codec {
	messageEncoders := make(map[cassandraprotocol.ProtocolVersion]map[cassandraprotocol.OpCode]codec.MessageEncoder)
	messageDecoders := make(map[cassandraprotocol.ProtocolVersion]map[cassandraprotocol.OpCode]codec.MessageDecoder)
	for _, messageCodecGroup := range messageCodecGroups {
		version := messageCodecGroup.ProtocolVersion
		for _, encoder := range messageCodecGroup.MessageEncoders {
			encoders, ok := messageEncoders[version]
			if !ok {
				encoders = make(map[cassandraprotocol.OpCode]codec.MessageEncoder)
				messageEncoders[version] = encoders
			}
			opCode := encoder.GetOpCode()
			encoders[opCode] = encoder
		}
		for _, decoder := range messageCodecGroup.MessageDecoders {
			decoders, ok := messageDecoders[version]
			if !ok {
				decoders = make(map[cassandraprotocol.OpCode]codec.MessageDecoder)
				messageDecoders[version] = decoders
			}
			opCode := decoder.GetOpCode()
			decoders[opCode] = decoder
		}
	}
	return &Codec{MessageEncoders: messageEncoders, MessageDecoders: messageDecoders, Compressor: compressor}
}

// NewDefaultClientFrameCodec builds a new instance with the default codecs for a client
// (encoding requests, decoding responses).
func NewDefaultClientFrameCodec(compressor cassandraprotocol.MessageCompressor) *Codec {
	return NewCodec(compressor, ProtocolV3ClientCodecs, ProtocolV4ClientCodecs, ProtocolV5ClientCodecs)
}

// NewDefaultServerFrameCodec builds a new instance with the default codecs for a server
// (decoding requests, encoding responses).
func NewDefaultServerFrameCodec(compressor cassandraprotocol.MessageCompressor) *Codec {
	return NewCodec(compressor, ProtocolV3ServerCodecs, ProtocolV4ServerCodecs, ProtocolV5ServerCodecs)
}

const headerEncodedSize = 9

func (c *Codec) Encode(frame *Frame) ([]byte, error) {

	version := frame.Version
	msg := frame.Message

	if version < cassandraprotocol.ProtocolVersion4 && frame.CustomPayload != nil {
		return nil, errors.New("custom payloads are not supported in protocol version " + string(version))
	}

	if version < cassandraprotocol.ProtocolVersion4 && frame.Warnings != nil {
		return nil, errors.New("warnings are not supported in protocol version " + string(version))
	}

	opCode := msg.GetOpCode()
	encoder := c.MessageEncoders[version][opCode]

	if encoder == nil {
		return nil, errors.New(fmt.Sprintf("unsupported opcode %d in protocol version %d", opCode, version))
	}

	compress := c.Compressor != nil && opCode != cassandraprotocol.OpCodeStartup && opCode != cassandraprotocol.OpCodeOptions

	var flags cassandraprotocol.ByteFlag = 0

	if compress {
		flags |= cassandraprotocol.HeaderFlagCompressed
	}
	if msg.IsResponse() && frame.TracingId != nil || !msg.IsResponse() && frame.TracingRequested {
		flags |= cassandraprotocol.HeaderFlagTracing
	}
	if frame.CustomPayload != nil {
		flags |= cassandraprotocol.HeaderFlagCustomPayload
	}
	if frame.Warnings != nil {
		flags |= cassandraprotocol.HeaderFlagWarning
	}
	if version == cassandraprotocol.ProtocolVersionBeta {
		flags |= cassandraprotocol.HeaderFlagUseBeta
	}

	if !compress {
		// No compression: we can optimize and do everything with a single allocation
		messageSize, _ := encoder.EncodedSize(msg, version)
		if frame.TracingId != nil {
			messageSize += codec.SizeOfUuid
		}
		if frame.CustomPayload != nil {
			messageSize += codec.SizeOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			messageSize += codec.SizeOfStringList(frame.Warnings)
		}
		encodedFrame := make([]byte, headerEncodedSize+messageSize)
		remaining := encodedFrame
		var err error
		remaining, err = encodeHeader(frame, flags, messageSize, remaining)
		if err != nil {
			return nil, err
		}
		if msg.IsResponse() && frame.TracingId != nil {
			remaining, err = codec.WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = codec.WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = codec.WriteStringList(frame.Warnings, remaining)
			if err != nil {
				return nil, err
			}
		}
		err = encoder.Encode(msg, remaining, version)
		if err != nil {
			return nil, err
		}
		return encodedFrame, nil

	} else {
		// We need to compress first in order to know the body size
		// 1) Encode uncompressed message
		uncompressedMessageSize, _ := encoder.EncodedSize(msg, version)
		if frame.TracingId != nil {
			uncompressedMessageSize += codec.SizeOfUuid
		}
		if frame.CustomPayload != nil {
			uncompressedMessageSize += codec.SizeOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			uncompressedMessageSize += codec.SizeOfStringList(frame.Warnings)
		}
		uncompressedMessage := make([]byte, uncompressedMessageSize)
		remaining := uncompressedMessage
		var err error
		if frame.TracingId != nil {
			remaining, err = codec.WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = codec.WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = codec.WriteStringList(frame.Warnings, remaining)
			if err != nil {
				return nil, err
			}
		}
		err = encoder.Encode(msg, remaining, version)
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

func encodeHeader(frame *Frame, flags cassandraprotocol.ByteFlag, messageSize int, dest []byte) ([]byte, error) {
	versionAndDirection := frame.Version
	if frame.Message.IsResponse() {
		versionAndDirection |= 0b1000_0000
	}
	var err error
	dest, err = codec.WriteByte(versionAndDirection, dest)
	if err != nil {
		return nil, err
	}
	dest, err = codec.WriteByte(uint8(flags), dest)
	if err != nil {
		return nil, err
	}
	dest, err = codec.WriteShort(uint16(frame.StreamId)&0xFFFF, dest)
	if err != nil {
		return nil, err
	}
	dest, err = codec.WriteByte(uint8(frame.Message.GetOpCode()), dest)
	if err != nil {
		return nil, err
	}
	dest, err = codec.WriteInt(int32(messageSize), dest)
	if err != nil {
		return nil, err
	}
	return dest, nil
}

func (c *Codec) Decode(source []byte) (*Frame, error) {
	var b byte
	var err error
	b, source, err = codec.ReadByte(source)
	if err != nil {
		return nil, err
	}
	isResponse := (b & 0b1000_0000) == 0b1000_0000
	version := cassandraprotocol.ProtocolVersion(b & 0b0111_1111)
	b, source, err = codec.ReadByte(source)
	if err != nil {
		return nil, err
	}
	flags := cassandraprotocol.ByteFlag(b)
	//beta := flags.contains(HeaderFlagUseBeta)
	var streamId uint16
	streamId, source, err = codec.ReadShort(source)
	if err != nil {
		return nil, err
	}
	b, source, err = codec.ReadByte(source)
	if err != nil {
		return nil, err
	}
	opCode := cassandraprotocol.OpCode(b)
	var i int32
	i, source, err = codec.ReadInt(source)
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

	decompressed := flags&cassandraprotocol.HeaderFlagCompressed > 0
	if decompressed {
		source, err = c.Compressor.Decompress(source)
		if err != nil {
			return nil, err
		}
	}

	var tracingId *cassandraprotocol.UUID
	if isResponse && (flags&cassandraprotocol.HeaderFlagTracing > 0) {
		tracingId, source, err = codec.ReadUuid(source)
		if err != nil {
			return nil, err
		}
	}

	var customPayload map[string][]byte
	if flags&cassandraprotocol.HeaderFlagCustomPayload > 0 {
		customPayload, source, err = codec.ReadBytesMap(source)
		if err != nil {
			return nil, err
		}
	}

	var warnings []string
	if isResponse && (flags&cassandraprotocol.HeaderFlagWarning > 0) {
		warnings, source, err = codec.ReadStringList(source)
		if err != nil {
			return nil, err
		}
	}

	decoder := c.MessageDecoders[version][opCode]
	if decoder == nil {
		return nil, errors.New(fmt.Sprintf("Unsupported request opcode: %d in protocol version %d", opCode, version))
	}

	var response message.Message
	response, err = decoder.Decode(source, version)
	if err != nil {
		return nil, err
	}

	frame := &Frame{
		version,
		int16(streamId),
		tracingId != nil,
		tracingId,
		customPayload,
		warnings,
		response}
	return frame, nil
}
