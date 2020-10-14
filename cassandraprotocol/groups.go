package cassandraprotocol

type MessageCodecGroup struct {
	ProtocolVersion ProtocolVersion
	MessageEncoders []MessageEncoder
	MessageDecoders []MessageDecoder
}

var ProtocolV3ClientCodecs = MessageCodecGroup{
	ProtocolVersion3,
	[]MessageEncoder{
		&StartupCodec{},
		&AuthResponseCodec{},
		&RegisterCodec{},
		&OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
	[]MessageDecoder{
		&AuthenticateCodec{},
		&AuthChallengeCodec{},
		&AuthSuccessCodec{},
		&ReadyCodec{},
		&SupportedCodec{},
		//&EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
}

var ProtocolV4ClientCodecs = MessageCodecGroup{
	ProtocolVersion4,
	[]MessageEncoder{
		&StartupCodec{},
		&AuthResponseCodec{},
		&RegisterCodec{},
		&OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
	[]MessageDecoder{
		&AuthenticateCodec{},
		&AuthChallengeCodec{},
		&AuthSuccessCodec{},
		&ReadyCodec{},
		&SupportedCodec{},
		//&EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
}

var ProtocolV5ClientCodecs = MessageCodecGroup{
	ProtocolVersion5,
	[]MessageEncoder{
		&StartupCodec{},
		&AuthResponseCodec{},
		&RegisterCodec{},
		&OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
	[]MessageDecoder{
		&AuthenticateCodec{},
		&AuthChallengeCodec{},
		&AuthSuccessCodec{},
		&ReadyCodec{},
		&SupportedCodec{},
		//&EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
}

var ProtocolV3ServerCodecs = MessageCodecGroup{
	ProtocolVersion3,
	[]MessageEncoder{
		&AuthenticateCodec{},
		&AuthChallengeCodec{},
		&AuthSuccessCodec{},
		&ReadyCodec{},
		&SupportedCodec{},
		//&EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
	[]MessageDecoder{
		&StartupCodec{},
		&AuthResponseCodec{},
		&RegisterCodec{},
		&OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
}

var ProtocolV4ServerCodecs = MessageCodecGroup{
	ProtocolVersion4,
	[]MessageEncoder{
		&AuthenticateCodec{},
		&AuthChallengeCodec{},
		&AuthSuccessCodec{},
		&ReadyCodec{},
		&SupportedCodec{},
		//&EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
	[]MessageDecoder{
		&StartupCodec{},
		&AuthResponseCodec{},
		&RegisterCodec{},
		&OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
}

var ProtocolV5ServerCodecs = MessageCodecGroup{
	ProtocolVersion5,
	[]MessageEncoder{
		&AuthenticateCodec{},
		&AuthChallengeCodec{},
		&AuthSuccessCodec{},
		&ReadyCodec{},
		&SupportedCodec{},
		//&EventCodec{},
		//&ResultCodec{},
		//&ErrorCodec{},
	},
	[]MessageDecoder{
		&StartupCodec{},
		&AuthResponseCodec{},
		&RegisterCodec{},
		&OptionsCodec{},
		//&QueryCodec{},
		//&PrepareCodec{},
		//&ExecuteCodec{},
		//&BatchCodec{},
	},
}
