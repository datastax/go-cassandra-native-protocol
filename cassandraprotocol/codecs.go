package cassandraprotocol

type MessageEncoder interface {
	GetOpCode() OpCode
	Encode(message Message, dest []byte) error
	EncodedSize(message Message) int
}

type MessageDecoder interface {
	GetOpCode() OpCode
	Decode(source []byte) (Message, error)
}

// STARTUP

type StartupCodec struct{}

func (c StartupCodec) GetOpCode() OpCode {
	return OpCodeStartup
}

func (c StartupCodec) Encode(message Message, dest []byte) error {
	startup := message.(*Startup)
	_, err := WriteStringMap(startup.Options, dest)
	return err
}

func (c StartupCodec) EncodedSize(message Message) int {
	startup := message.(*Startup)
	return SizeOfStringMap(startup.Options)
}

func (c StartupCodec) Decode(source []byte) (Message, error) {
	options, _, err := ReadStringMap(source)
	if err != nil {
		return nil, err
	}
	return NewStartupWithOptions(options), nil
}

// AUTHENTICATE

type AuthenticateCodec struct {}

func (c AuthenticateCodec) GetOpCode() OpCode {
	return OpCodeAuthenticate
}

func (c AuthenticateCodec) Encode(message Message, dest []byte) error {
	authenticate := message.(*Authenticate)
	_, err := WriteString(authenticate.Authenticator, dest)
	return err
}

func (c AuthenticateCodec) EncodedSize(message Message) int {
	authenticate := message.(*Authenticate)
	return SizeOfString(authenticate.Authenticator)
}

func (c AuthenticateCodec) Decode(source []byte) (Message, error) {
	authenticator, _, err := ReadString(source)
	if err != nil {
		return nil, err
	}
	return &Authenticate{authenticator}, nil
}

// AUTH RESPONSE

type AuthResponseCodec struct {}

func (c AuthResponseCodec) GetOpCode() OpCode {
	return OpCodeAuthResponse
}

func (c AuthResponseCodec) Encode(message Message, dest []byte) error {
	authResponse := message.(*AuthResponse)
	_, err := WriteBytes(authResponse.Token, dest)
	return err
}

func (c AuthResponseCodec) EncodedSize(message Message) int {
	authResponse := message.(*AuthResponse)
	return SizeOfBytes(authResponse.Token)
}

func (c AuthResponseCodec) Decode(source []byte) (Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthResponse{token}, nil
}

// AUTH CHALLENGE

type AuthChallengeCodec struct {}

func (c AuthChallengeCodec) GetOpCode() OpCode {
	return OpCodeAuthChallenge
}

func (c AuthChallengeCodec) Encode(message Message, dest []byte) error {
	authChallenge := message.(*AuthChallenge)
	_, err := WriteBytes(authChallenge.Token, dest)
	return err
}

func (c AuthChallengeCodec) EncodedSize(message Message) int {
	authChallenge := message.(*AuthChallenge)
	return SizeOfBytes(authChallenge.Token)
}

func (c AuthChallengeCodec) Decode(source []byte) (Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthChallenge{token}, nil
}

// AUTH SUCCESS

type AuthSuccessCodec struct {}

func (c AuthSuccessCodec) GetOpCode() OpCode {
	return OpCodeAuthSuccess
}

func (c AuthSuccessCodec) Encode(message Message, dest []byte) error {
	authSuccess := message.(*AuthSuccess)
	_, err := WriteBytes(authSuccess.Token, dest)
	return err
}

func (c AuthSuccessCodec) EncodedSize(message Message) int {
	authSuccess := message.(*AuthSuccess)
	return SizeOfBytes(authSuccess.Token)
}

func (c AuthSuccessCodec) Decode(source []byte) (Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthSuccess{token}, nil
}

// REGISTER

type RegisterCodec struct{}

func (c RegisterCodec) GetOpCode() OpCode {
	return OpCodeRegister
}

func (c RegisterCodec) Encode(message Message, dest []byte) error {
	register := message.(*Register)
	_, err := WriteStringList(register.EventTypes, dest)
	return err
}

func (c RegisterCodec) EncodedSize(message Message) int {
	register := message.(*Register)
	return SizeOfStringList(register.EventTypes)
}

func (c RegisterCodec) Decode(source []byte) (Message, error) {
	eventTypes, _, err := ReadStringList(source)
	if err != nil {
		return nil, err
	}
	return NewRegister(eventTypes), nil
}

// OPTIONS

type OptionsCodec struct{}

func (c OptionsCodec) GetOpCode() OpCode {
	return OpCodeOptions
}

func (c OptionsCodec) Encode(message Message, dest []byte) error {
	return nil
}

func (c OptionsCodec) EncodedSize(message Message) int {
	return 0
}

func (c OptionsCodec) Decode(source []byte) (Message, error) {
	return &Options{}, nil
}

// READY

type ReadyCodec struct{}

func (c ReadyCodec) GetOpCode() OpCode {
	return OpCodeReady
}

func (c ReadyCodec) Encode(message Message, dest []byte) error {
	return nil
}

func (c ReadyCodec) EncodedSize(message Message) int {
	return 0
}

func (c ReadyCodec) Decode(source []byte) (Message, error) {
	return &Ready{}, nil
}

