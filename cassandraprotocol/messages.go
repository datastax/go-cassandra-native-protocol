package cassandraprotocol

import (
	"fmt"
	"net"
)

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

func (m Startup) IsResponse() bool {
	return false
}

func (m Startup) GetOpCode() OpCode {
	return OpCodeStartup
}

func (m Startup) String() string {
	return fmt.Sprint("STARTUP ", m.Options)
}

// AUTHENTICATE

type Authenticate struct {
	Authenticator string
}

func (m Authenticate) IsResponse() bool {
	return true
}

func (m Authenticate) GetOpCode() OpCode {
	return OpCodeAuthenticate
}

func (m Authenticate) String() string {
	return "AUTHENTICATE " + m.Authenticator
}

// AUTH RESPONSE

type AuthResponse struct {
	Token []byte
}

func (m AuthResponse) IsResponse() bool {
	return false
}

func (m AuthResponse) GetOpCode() OpCode {
	return OpCodeAuthResponse
}

func (m AuthResponse) String() string {
	return "AUTH_RESPONSE " + string(m.Token)
}

// AUTH CHALLENGE

type AuthChallenge struct {
	Token []byte
}

func (m AuthChallenge) IsResponse() bool {
	return true
}

func (m AuthChallenge) GetOpCode() OpCode {
	return OpCodeAuthChallenge
}

func (m AuthChallenge) String() string {
	return "AUTH_CHALLENGE " + string(m.Token)
}

// AUTH SUCCESS

type AuthSuccess struct {
	Token []byte
}

func (m AuthSuccess) IsResponse() bool {
	return true
}

func (m AuthSuccess) GetOpCode() OpCode {
	return OpCodeAuthSuccess
}

func (m AuthSuccess) String() string {
	return "AUTH_SUCCESS " + string(m.Token)
}

// REGISTER

type Register struct {
	EventTypes []EventType
}

func NewRegister(eventTypes []EventType) *Register {
	return &Register{EventTypes: eventTypes}
}

func (m Register) IsResponse() bool {
	return false
}

func (m Register) GetOpCode() OpCode {
	return OpCodeRegister
}

func (m Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}

// READY

type Ready struct {
}

func (m Ready) IsResponse() bool {
	return false
}

func (m Ready) GetOpCode() OpCode {
	return OpCodeReady
}

func (m Ready) String() string {
	return "READY"
}

// OPTIONS

type Options struct {
}

func (m Options) IsResponse() bool {
	return false
}

func (m Options) GetOpCode() OpCode {
	return OpCodeOptions
}

func (m Options) String() string {
	return "OPTIONS"
}

// SUPPORTED

type Supported struct {
	Options map[string][]string
}

func (m Supported) IsResponse() bool {
	return true
}

func (m Supported) GetOpCode() OpCode {
	return OpCodeSupported
}

func (m Supported) String() string {
	return fmt.Sprintf("SUPPORTED %v", m.Options)
}

// EVENT

type Event struct {
	Type EventType
}

func (m Event) IsResponse() bool {
	return true
}

func (m Event) GetOpCode() OpCode {
	return OpCodeEvent
}

// SCHEMA CHANGE EVENT

type SchemaChangeEvent struct {
	Event
	ChangeType SchemaChangeType
	Target     SchemaChangeTarget
	Keyspace   string
	Object     string
	Arguments  []string
}

func (m SchemaChangeEvent) String() string {
	return fmt.Sprintf("EVENT %v (change=%s target=%s keyspace=%s object=%s args=%s)",
		m.Type,
		m.ChangeType,
		m.Target,
		m.Keyspace,
		m.Object,
		m.Arguments)
}

// STATUS CHANGE EVENT

type StatusChangeEvent struct {
	Event
	ChangeType StatusChangeType
	Address    net.IP
	Port       int32
}

func (m StatusChangeEvent) String() string {
	return fmt.Sprintf("EVENT %v (change=%s address=%s port=%d)", m.Type, m.ChangeType, m.Address, m.Port)
}

// TOPOLOGY CHANGE EVENT

type TopologyChangeEvent struct {
	Event
	ChangeType TopologyChangeType
	Address    net.IP
	Port       int32
}

func (m TopologyChangeEvent) String() string {
	return fmt.Sprintf("EVENT %v (change=%s address=%s port=%d)", m.Type, m.ChangeType, m.Address, m.Port)
}
