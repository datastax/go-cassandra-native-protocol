package frame

import (
	"github.com/datastax/go-cassandra-native-protocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type Codec struct {
	compressor    compression.MessageCompressor
	messageCodecs map[primitive.OpCode]message.Codec
}

type CodecCustomizer func(*Codec)

func NewCodec(customizers ...CodecCustomizer) *Codec {
	frameCodec := &Codec{messageCodecs: make(map[primitive.OpCode]message.Codec, len(message.DefaultMessageCodecs))}
	for _, messageCodec := range message.DefaultMessageCodecs {
		frameCodec.messageCodecs[messageCodec.GetOpCode()] = messageCodec
	}
	for _, customizer := range customizers {
		customizer(frameCodec)
	}
	return frameCodec
}

func WithCompressor(compressor compression.MessageCompressor) CodecCustomizer {
	return func(frameCodec *Codec) {
		frameCodec.compressor = compressor
	}
}

func WithMessageCodecs(messageCodecs ...message.Codec) CodecCustomizer {
	return func(frameCodec *Codec) {
		for _, codec := range messageCodecs {
			frameCodec.messageCodecs[codec.GetOpCode()] = codec
		}
	}
}

func (c *Codec) CompressionAlgorithm() string {
	if c.compressor == nil {
		return "NONE"
	} else {
		return c.compressor.Algorithm()
	}
}
