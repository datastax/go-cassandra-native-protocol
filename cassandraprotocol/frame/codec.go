package frame

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/compression"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/message/codec"
	"go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type Codec struct {
	Compressor compression.MessageCompressor
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
	var encoder codec.MessageEncoder = findMessageCodec(opCode)

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
			messageSize += primitive.SizeOfUuid
		}
		if frame.CustomPayload != nil {
			messageSize += primitive.SizeOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			messageSize += primitive.SizeOfStringList(frame.Warnings)
		}
		encodedFrame := make([]byte, headerEncodedSize+messageSize)
		remaining := encodedFrame
		var err error
		remaining, err = encodeHeader(frame, flags, messageSize, remaining)
		if err != nil {
			return nil, err
		}
		if msg.IsResponse() && frame.TracingId != nil {
			remaining, err = primitive.WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = primitive.WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = primitive.WriteStringList(frame.Warnings, remaining)
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
			uncompressedMessageSize += primitive.SizeOfUuid
		}
		if frame.CustomPayload != nil {
			uncompressedMessageSize += primitive.SizeOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			uncompressedMessageSize += primitive.SizeOfStringList(frame.Warnings)
		}
		uncompressedMessage := make([]byte, uncompressedMessageSize)
		remaining := uncompressedMessage
		var err error
		if frame.TracingId != nil {
			remaining, err = primitive.WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = primitive.WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = primitive.WriteStringList(frame.Warnings, remaining)
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
	dest, err = primitive.WriteByte(versionAndDirection, dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitive.WriteByte(uint8(flags), dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitive.WriteShort(uint16(frame.StreamId)&0xFFFF, dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitive.WriteByte(uint8(frame.Message.GetOpCode()), dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitive.WriteInt(int32(messageSize), dest)
	if err != nil {
		return nil, err
	}
	return dest, nil
}

func (c *Codec) Decode(source []byte) (*Frame, error) {
	var b byte
	var err error
	b, source, err = primitive.ReadByte(source)
	if err != nil {
		return nil, err
	}
	isResponse := (b & 0b1000_0000) == 0b1000_0000
	version := cassandraprotocol.ProtocolVersion(b & 0b0111_1111)
	b, source, err = primitive.ReadByte(source)
	if err != nil {
		return nil, err
	}
	flags := cassandraprotocol.ByteFlag(b)
	//beta := flags.contains(HeaderFlagUseBeta)
	var streamId uint16
	streamId, source, err = primitive.ReadShort(source)
	if err != nil {
		return nil, err
	}
	b, source, err = primitive.ReadByte(source)
	if err != nil {
		return nil, err
	}
	opCode := cassandraprotocol.OpCode(b)
	var i int32
	i, source, err = primitive.ReadInt(source)
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
		tracingId, source, err = primitive.ReadUuid(source)
		if err != nil {
			return nil, err
		}
	}

	var customPayload map[string][]byte
	if flags&cassandraprotocol.HeaderFlagCustomPayload > 0 {
		customPayload, source, err = primitive.ReadBytesMap(source)
		if err != nil {
			return nil, err
		}
	}

	var warnings []string
	if isResponse && (flags&cassandraprotocol.HeaderFlagWarning > 0) {
		warnings, source, err = primitive.ReadStringList(source)
		if err != nil {
			return nil, err
		}
	}

	var decoder codec.MessageDecoder = findMessageCodec(opCode)
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

func findMessageCodec(opCode cassandraprotocol.OpCode) codec.MessageCodec {
	switch opCode {
	// requests
	case cassandraprotocol.OpCodeStartup:
		return codec.StartupCodec{}
	case cassandraprotocol.OpCodeOptions:
		return codec.OptionsCodec{}
	case cassandraprotocol.OpCodeQuery:
		return nil // TODO
	case cassandraprotocol.OpCodePrepare:
		return nil // TODO
	case cassandraprotocol.OpCodeExecute:
		return nil // TODO
	case cassandraprotocol.OpCodeRegister:
		return codec.RegisterCodec{}
	case cassandraprotocol.OpCodeBatch:
		return nil // TODO
	case cassandraprotocol.OpCodeAuthResponse:
		return codec.AuthResponseCodec{}
	// responses
	case cassandraprotocol.OpCodeError:
		return nil // TODO
	case cassandraprotocol.OpCodeReady:
		return codec.ReadyCodec{}
	case cassandraprotocol.OpCodeAuthenticate:
		return codec.AuthenticateCodec{}
	case cassandraprotocol.OpCodeSupported:
		return codec.SupportedCodec{}
	case cassandraprotocol.OpCodeResult:
		return nil // TODO
	case cassandraprotocol.OpCodeEvent:
		return codec.EventCodec{}
	case cassandraprotocol.OpCodeAuthChallenge:
		return codec.AuthChallengeCodec{}
	case cassandraprotocol.OpCodeAuthSuccess:
		return codec.AuthSuccessCodec{}
	}
	return nil
}
