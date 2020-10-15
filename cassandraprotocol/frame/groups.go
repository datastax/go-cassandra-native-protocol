package frame

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message/codec"
)

type MessageCodecGroup struct {
	ProtocolVersion cassandraprotocol.ProtocolVersion
	MessageEncoders []codec.MessageEncoder
	MessageDecoders []codec.MessageDecoder
}

var ProtocolV3ClientCodecs = MessageCodecGroup{
	cassandraprotocol.ProtocolVersion3,
	[]codec.MessageEncoder{
		&codec.StartupCodec{},
		&codec.AuthResponseCodec{},
		&codec.RegisterCodec{},
		&codec.OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
	[]codec.MessageDecoder{
		&codec.AuthenticateCodec{},
		&codec.AuthChallengeCodec{},
		&codec.AuthSuccessCodec{},
		&codec.ReadyCodec{},
		&codec.SupportedCodec{},
		&codec.EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
}

var ProtocolV4ClientCodecs = MessageCodecGroup{
	cassandraprotocol.ProtocolVersion4,
	[]codec.MessageEncoder{
		&codec.StartupCodec{},
		&codec.AuthResponseCodec{},
		&codec.RegisterCodec{},
		&codec.OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
	[]codec.MessageDecoder{
		&codec.AuthenticateCodec{},
		&codec.AuthChallengeCodec{},
		&codec.AuthSuccessCodec{},
		&codec.ReadyCodec{},
		&codec.SupportedCodec{},
		&codec.EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
}

var ProtocolV5ClientCodecs = MessageCodecGroup{
	cassandraprotocol.ProtocolVersion5,
	[]codec.MessageEncoder{
		&codec.StartupCodec{},
		&codec.AuthResponseCodec{},
		&codec.RegisterCodec{},
		&codec.OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
	[]codec.MessageDecoder{
		&codec.AuthenticateCodec{},
		&codec.AuthChallengeCodec{},
		&codec.AuthSuccessCodec{},
		&codec.ReadyCodec{},
		&codec.SupportedCodec{},
		&codec.EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
}

var ProtocolV3ServerCodecs = MessageCodecGroup{
	cassandraprotocol.ProtocolVersion3,
	[]codec.MessageEncoder{
		&codec.AuthenticateCodec{},
		&codec.AuthChallengeCodec{},
		&codec.AuthSuccessCodec{},
		&codec.ReadyCodec{},
		&codec.SupportedCodec{},
		&codec.EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
	[]codec.MessageDecoder{
		&codec.StartupCodec{},
		&codec.AuthResponseCodec{},
		&codec.RegisterCodec{},
		&codec.OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
}

var ProtocolV4ServerCodecs = MessageCodecGroup{
	cassandraprotocol.ProtocolVersion4,
	[]codec.MessageEncoder{
		&codec.AuthenticateCodec{},
		&codec.AuthChallengeCodec{},
		&codec.AuthSuccessCodec{},
		&codec.ReadyCodec{},
		&codec.SupportedCodec{},
		&codec.EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
	[]codec.MessageDecoder{
		&codec.StartupCodec{},
		&codec.AuthResponseCodec{},
		&codec.RegisterCodec{},
		&codec.OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
}

var ProtocolV5ServerCodecs = MessageCodecGroup{
	cassandraprotocol.ProtocolVersion5,
	[]codec.MessageEncoder{
		&codec.AuthenticateCodec{},
		&codec.AuthChallengeCodec{},
		&codec.AuthSuccessCodec{},
		&codec.ReadyCodec{},
		&codec.SupportedCodec{},
		&codec.EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
	[]codec.MessageDecoder{
		&codec.StartupCodec{},
		&codec.AuthResponseCodec{},
		&codec.RegisterCodec{},
		&codec.OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
}
