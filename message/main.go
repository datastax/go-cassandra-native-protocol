package message

var DefaultMessageCodecs = []Codec{
	&startupCodec{},
	&optionsCodec{},
	&queryCodec{},
	&prepareCodec{},
	&executeCodec{},
	&registerCodec{},
	&batchCodec{},
	&authResponseCodec{},
	&errorCodec{},
	&readyCodec{},
	&authenticateCodec{},
	&supportedCodec{},
	&resultCodec{},
	&eventCodec{},
	&authChallengeCodec{},
	&authSuccessCodec{},
	// DSE-specific
	&reviseCodec{},
}
