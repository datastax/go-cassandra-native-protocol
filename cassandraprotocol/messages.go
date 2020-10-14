package cassandraprotocol

import "fmt"

type Message interface {
	IsResponse() bool
	GetOpCode() OpCode
}

// STARTUP

type Startup struct {
	Options map[string]string
}

func NewStartup() *Startup {
	return &Startup{map[string]string{"CQL_VERSION": "3.0.0"}}
}

func NewStartupWithCompression(compression string) *Startup {
	return &Startup{map[string]string{
		"CQL_VERSION": "3.0.0",
		"COMPRESSION": compression}}
}

func NewStartupWithOptions(options map[string]string) *Startup {
	return &Startup{options}
}

func (s Startup) IsResponse() bool {
	return false
}

func (s Startup) GetOpCode() OpCode {
	return OpCodeStartup
}

func (s Startup) String() string {
	return fmt.Sprint("STARTUP ", s.Options)
}

// AUTHENTICATE

type Authenticate struct {
	Authenticator string
}

func (a Authenticate) IsResponse() bool {
	return true
}

func (a Authenticate) GetOpCode() OpCode {
	return OpCodeAuthenticate
}

func (a Authenticate) String() string {
	return "AUTHENTICATE " + a.Authenticator
}

// AUTH RESPONSE

type AuthResponse struct {
	Token []byte
}

func (a AuthResponse) IsResponse() bool {
	return false
}

func (a AuthResponse) GetOpCode() OpCode {
	return OpCodeAuthResponse
}

func (a AuthResponse) String() string {
	return "AUTH_RESPONSE " + string(a.Token)
}

// AUTH CHALLENGE

type AuthChallenge struct {
	Token []byte
}

func (a AuthChallenge) IsResponse() bool {
	return true
}

func (a AuthChallenge) GetOpCode() OpCode {
	return OpCodeAuthChallenge
}

func (a AuthChallenge) String() string {
	return "AUTH_CHALLENGE " + string(a.Token)
}

// AUTH SUCCESS

type AuthSuccess struct {
	Token []byte
}

func (a AuthSuccess) IsResponse() bool {
	return true
}

func (a AuthSuccess) GetOpCode() OpCode {
	return OpCodeAuthSuccess
}

func (a AuthSuccess) String() string {
	return "AUTH_SUCCESS " + string(a.Token)
}

// REGISTER

type Register struct {
	EventTypes []string
}

func NewRegister(eventTypes []string) *Register {
	return &Register{EventTypes: eventTypes}
}

func (r Register) IsResponse() bool {
	return false
}

func (r Register) GetOpCode() OpCode {
	return OpCodeRegister
}

func (r Register) String() string {
	return fmt.Sprint("REGISTER ", r.EventTypes)
}

// OPTIONS

type Options struct {
}

func (r Options) IsResponse() bool {
	return false
}

func (r Options) GetOpCode() OpCode {
	return OpCodeOptions
}

func (r Options) String() string {
	return "OPTIONS"
}

// READY

type Ready struct {
}

func (r Ready) IsResponse() bool {
	return false
}

func (r Ready) GetOpCode() OpCode {
	return OpCodeReady
}

func (r Ready) String() string {
	return "READY"
}
