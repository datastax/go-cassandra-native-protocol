package frame

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/compression"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
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
	var encoder message.Encoder = findCodec(opCode)

	if encoder == nil {
		return nil, errors.New(fmt.Sprintf("unsupported opcode %d in protocol version %d", opCode, version))
	}

	compress := c.Compressor != nil && opCode != cassandraprotocol.OpCodeStartup && opCode != cassandraprotocol.OpCodeOptions

	var flags cassandraprotocol.HeaderFlag = 0

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
			messageSize += primitives.LengthOfUuid
		}
		if frame.CustomPayload != nil {
			messageSize += primitives.LengthOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			messageSize += primitives.LengthOfStringList(frame.Warnings)
		}
		encodedFrame := make([]byte, headerEncodedSize+messageSize)
		remaining := encodedFrame
		var err error
		remaining, err = encodeHeader(frame, flags, messageSize, remaining)
		if err != nil {
			return nil, err
		}
		if msg.IsResponse() && frame.TracingId != nil {
			remaining, err = primitives.WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = primitives.WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = primitives.WriteStringList(frame.Warnings, remaining)
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
			uncompressedMessageSize += primitives.LengthOfUuid
		}
		if frame.CustomPayload != nil {
			uncompressedMessageSize += primitives.LengthOfBytesMap(frame.CustomPayload)
		}
		if frame.Warnings != nil {
			uncompressedMessageSize += primitives.LengthOfStringList(frame.Warnings)
		}
		uncompressedMessage := make([]byte, uncompressedMessageSize)
		remaining := uncompressedMessage
		var err error
		if frame.TracingId != nil {
			remaining, err = primitives.WriteUuid(frame.TracingId, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.CustomPayload != nil {
			remaining, err = primitives.WriteBytesMap(frame.CustomPayload, remaining)
			if err != nil {
				return nil, err
			}
		}
		if frame.Warnings != nil {
			remaining, err = primitives.WriteStringList(frame.Warnings, remaining)
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

func encodeHeader(frame *Frame, flags cassandraprotocol.HeaderFlag, messageSize int, dest []byte) ([]byte, error) {
	versionAndDirection := frame.Version
	if frame.Message.IsResponse() {
		versionAndDirection |= 0b1000_0000
	}
	var err error
	dest, err = primitives.WriteByte(versionAndDirection, dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitives.WriteByte(uint8(flags), dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitives.WriteShort(uint16(frame.StreamId)&0xFFFF, dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitives.WriteByte(uint8(frame.Message.GetOpCode()), dest)
	if err != nil {
		return nil, err
	}
	dest, err = primitives.WriteInt(int32(messageSize), dest)
	if err != nil {
		return nil, err
	}
	return dest, nil
}

func (c *Codec) Decode(source []byte) (*Frame, error) {
	var b byte
	var err error
	b, source, err = primitives.ReadByte(source)
	if err != nil {
		return nil, err
	}
	isResponse := (b & 0b1000_0000) == 0b1000_0000
	version := cassandraprotocol.ProtocolVersion(b & 0b0111_1111)
	b, source, err = primitives.ReadByte(source)
	if err != nil {
		return nil, err
	}
	flags := cassandraprotocol.HeaderFlag(b)
	//beta := flags.contains(HeaderFlagUseBeta)
	var streamId uint16
	streamId, source, err = primitives.ReadShort(source)
	if err != nil {
		return nil, err
	}
	b, source, err = primitives.ReadByte(source)
	if err != nil {
		return nil, err
	}
	opCode := cassandraprotocol.OpCode(b)
	var i int32
	i, source, err = primitives.ReadInt(source)
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
		tracingId, source, err = primitives.ReadUuid(source)
		if err != nil {
			return nil, err
		}
	}

	var customPayload map[string][]byte
	if flags&cassandraprotocol.HeaderFlagCustomPayload > 0 {
		customPayload, source, err = primitives.ReadBytesMap(source)
		if err != nil {
			return nil, err
		}
	}

	var warnings []string
	if isResponse && (flags&cassandraprotocol.HeaderFlagWarning > 0) {
		warnings, source, err = primitives.ReadStringList(source)
		if err != nil {
			return nil, err
		}
	}

	var decoder message.Decoder = findCodec(opCode)
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

func findCodec(opCode cassandraprotocol.OpCode) message.Codec {
	switch opCode {
	// requests
	case cassandraprotocol.OpCodeStartup:
		return message.StartupCodec{}
	case cassandraprotocol.OpCodeOptions:
		return message.OptionsCodec{}
	case cassandraprotocol.OpCodeQuery:
		return message.QueryCodec{}
	case cassandraprotocol.OpCodePrepare:
		return nil // TODO
	case cassandraprotocol.OpCodeExecute:
		return message.ExecuteCodec{}
	case cassandraprotocol.OpCodeRegister:
		return message.RegisterCodec{}
	case cassandraprotocol.OpCodeBatch:
		return nil // TODO
	case cassandraprotocol.OpCodeAuthResponse:
		return message.AuthResponseCodec{}
	// responses
	case cassandraprotocol.OpCodeError:
		return nil // TODO
	case cassandraprotocol.OpCodeReady:
		return message.ReadyCodec{}
	case cassandraprotocol.OpCodeAuthenticate:
		return message.AuthenticateCodec{}
	case cassandraprotocol.OpCodeSupported:
		return message.SupportedCodec{}
	case cassandraprotocol.OpCodeResult:
		return nil // TODO
	case cassandraprotocol.OpCodeEvent:
		return message.EventCodec{}
	case cassandraprotocol.OpCodeAuthChallenge:
		return message.AuthChallengeCodec{}
	case cassandraprotocol.OpCodeAuthSuccess:
		return message.AuthSuccessCodec{}
	}
	return nil
}
