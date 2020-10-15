package cassandraprotocol

import (
	"errors"
	"fmt"
)

type MessageEncoder interface {
	GetOpCode() OpCode
	Encode(message Message, dest []byte, version ProtocolVersion) error
	EncodedSize(message Message, version ProtocolVersion) (int, error)
}

type MessageDecoder interface {
	GetOpCode() OpCode
	Decode(source []byte, version ProtocolVersion) (Message, error)
}

// STARTUP

type StartupCodec struct{}

func (c StartupCodec) GetOpCode() OpCode {
	return OpCodeStartup
}

func (c StartupCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	startup := message.(*Startup)
	_, err := WriteStringMap(startup.Options, dest)
	return err
}

func (c StartupCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	startup := message.(*Startup)
	return SizeOfStringMap(startup.Options), nil
}

func (c StartupCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	options, _, err := ReadStringMap(source)
	if err != nil {
		return nil, err
	}
	return NewStartupWithOptions(options), nil
}

// AUTHENTICATE

type AuthenticateCodec struct{}

func (c AuthenticateCodec) GetOpCode() OpCode {
	return OpCodeAuthenticate
}

func (c AuthenticateCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	authenticate := message.(*Authenticate)
	_, err := WriteString(authenticate.Authenticator, dest)
	return err
}

func (c AuthenticateCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	authenticate := message.(*Authenticate)
	return SizeOfString(authenticate.Authenticator), nil
}

func (c AuthenticateCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	authenticator, _, err := ReadString(source)
	if err != nil {
		return nil, err
	}
	return &Authenticate{authenticator}, nil
}

// AUTH RESPONSE

type AuthResponseCodec struct{}

func (c AuthResponseCodec) GetOpCode() OpCode {
	return OpCodeAuthResponse
}

func (c AuthResponseCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	authResponse := message.(*AuthResponse)
	_, err := WriteBytes(authResponse.Token, dest)
	return err
}

func (c AuthResponseCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	authResponse := message.(*AuthResponse)
	return SizeOfBytes(authResponse.Token), nil
}

func (c AuthResponseCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthResponse{token}, nil
}

// AUTH CHALLENGE

type AuthChallengeCodec struct{}

func (c AuthChallengeCodec) GetOpCode() OpCode {
	return OpCodeAuthChallenge
}

func (c AuthChallengeCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	authChallenge := message.(*AuthChallenge)
	_, err := WriteBytes(authChallenge.Token, dest)
	return err
}

func (c AuthChallengeCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	authChallenge := message.(*AuthChallenge)
	return SizeOfBytes(authChallenge.Token), nil
}

func (c AuthChallengeCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	token, _, err := ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &AuthChallenge{token}, nil
}

// AUTH SUCCESS

type AuthSuccessCodec struct{}

func (c AuthSuccessCodec) GetOpCode() OpCode {
	return OpCodeAuthSuccess
}

func (c AuthSuccessCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	authSuccess := message.(*AuthSuccess)
	_, err := WriteBytes(authSuccess.Token, dest)
	return err
}

func (c AuthSuccessCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	authSuccess := message.(*AuthSuccess)
	return SizeOfBytes(authSuccess.Token), nil
}

func (c AuthSuccessCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
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

func (c RegisterCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	register := message.(*Register)
	_, err := WriteStringList(register.EventTypes, dest)
	return err
}

type Int = string

func (c RegisterCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	register := message.(*Register)
	return SizeOfStringList(register.EventTypes), nil
}

func (c RegisterCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	eventTypes, _, err := ReadStringList(source)
	if err != nil {
		return nil, err
	}
	return NewRegister(eventTypes), nil
}

// READY

type ReadyCodec struct{}

func (c ReadyCodec) GetOpCode() OpCode {
	return OpCodeReady
}

func (c ReadyCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	return nil
}

func (c ReadyCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	return 0, nil
}

func (c ReadyCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	return &Ready{}, nil
}

// OPTIONS

type OptionsCodec struct{}

func (c OptionsCodec) GetOpCode() OpCode {
	return OpCodeOptions
}

func (c OptionsCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	return nil
}

func (c OptionsCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	return 0, nil
}

func (c OptionsCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	return &Options{}, nil
}

// SUPPORTED

type SupportedCodec struct{}

func (c SupportedCodec) GetOpCode() OpCode {
	return OpCodeSupported
}

func (c SupportedCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	supported := message.(*Supported)
	_, err := WriteStringMultiMap(supported.Options, dest)
	if err != nil {
		return err
	}
	return nil
}

func (c SupportedCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	supported := message.(*Supported)
	return SizeOfStringMultiMap(supported.Options), nil
}

func (c SupportedCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	options, _, err := ReadStringMultiMap(source)
	if err != nil {
		return nil, err
	}
	return &Supported{options}, nil
}

// EVENT

type EventCodec struct{}

func (c EventCodec) GetOpCode() OpCode {
	return OpCodeEvent
}

func (c EventCodec) Encode(message Message, dest []byte, version ProtocolVersion) error {
	event := message.(*Event)
	var err error
	dest, err = WriteString(event.Type, dest)
	if err != nil {
		return err
	}
	switch event.Type {
	case EventTypeSchemaChange:
		sce, ok := message.(*SchemaChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected SchemaChangeEvent struct, got %T", sce))
		}
		dest, err = WriteString(sce.ChangeType, dest)
		if err != nil {
			return err
		}
		dest, err = WriteString(sce.Target, dest)
		if err != nil {
			return err
		}
		dest, err = WriteString(sce.Keyspace, dest)
		if err != nil {
			return err
		}
		switch sce.Target {
		case SchemaChangeTargetKeyspace:
		case SchemaChangeTargetTable:
			fallthrough
		case SchemaChangeTargetType:
			dest, err = WriteString(sce.Object, dest)
			if err != nil {
				return err
			}
		case SchemaChangeTargetAggregate:
			fallthrough
		case SchemaChangeTargetFunction:
			if version < ProtocolVersion4 {
				return errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			dest, err = WriteString(sce.Object, dest)
			if err != nil {
				return err
			}
			dest, err = WriteStringList(sce.Arguments, dest)
			if err != nil {
				return err
			}
		default:
			return errors.New(fmt.Sprintf("unknown schema change target: " + sce.Target))
		}
		return nil
	case EventTypeStatusChange:
		sce, ok := message.(*StatusChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected StatusChangeEvent struct, got %T", sce))
		}
		dest, err = WriteString(sce.ChangeType, dest)
		if err != nil {
			return err
		}
		dest, err = WriteInet(sce.Address, sce.Port, dest)
		if err != nil {
			return err
		}
		return nil
	case EventTypeTopologyChange:
		tce, ok := message.(*TopologyChangeEvent)
		if !ok {
			return errors.New(fmt.Sprintf("expected TopologyChangeEvent struct, got %T", tce))
		}
		dest, err = WriteString(tce.ChangeType, dest)
		if err != nil {
			return err
		}
		dest, err = WriteInet(tce.Address, tce.Port, dest)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("unknown event type: " + event.Type)
}

func (c EventCodec) EncodedSize(message Message, version ProtocolVersion) (int, error) {
	event := message.(*Event)
	size := SizeOfString(event.Type)
	switch event.Type {
	case EventTypeSchemaChange:
		sce, ok := message.(*SchemaChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected SchemaChangeEvent struct, got %T", sce))
		}
		size += SizeOfString(sce.ChangeType)
		size += SizeOfString(sce.Target)
		size += SizeOfString(sce.Keyspace)
		switch sce.Target {
		case SchemaChangeTargetKeyspace:
		case SchemaChangeTargetTable:
			fallthrough
		case SchemaChangeTargetType:
			size += SizeOfString(sce.Object)
			break
		case SchemaChangeTargetAggregate:
			fallthrough
		case SchemaChangeTargetFunction:
			if version < ProtocolVersion4 {
				return -1, errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			size += SizeOfString(sce.Object)
			size += SizeOfStringList(sce.Arguments)
		default:
			return -1, errors.New(fmt.Sprintf("unknown schema change target: " + sce.Target))
		}
		return size, nil
	case EventTypeStatusChange:
		sce, ok := message.(*StatusChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected StatusChangeEvent struct, got %T", sce))
		}
		size += SizeOfString(sce.ChangeType)
		size += SizeOfInet(sce.Address)
		return size, nil
	case EventTypeTopologyChange:
		tce, ok := message.(*TopologyChangeEvent)
		if !ok {
			return -1, errors.New(fmt.Sprintf("expected TopologyChangeEvent struct, got %T", tce))
		}
		size += SizeOfString(tce.ChangeType)
		size += SizeOfInet(tce.Address)
		return size, nil
	}
	return -1, errors.New("unknown event type: " + event.Type)
}

func (c EventCodec) Decode(source []byte, version ProtocolVersion) (Message, error) {
	eventType, _, err := ReadString(source)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case EventTypeSchemaChange:
		sce := &SchemaChangeEvent{Event: Event{Type: eventType}}
		sce.ChangeType, source, err = ReadString(source)
		if err != nil {
			return nil, err
		}
		sce.Target, source, err = ReadString(source)
		if err != nil {
			return nil, err
		}
		sce.Keyspace, source, err = ReadString(source)
		if err != nil {
			return nil, err
		}
		switch sce.Target {
		case SchemaChangeTargetKeyspace:
		case SchemaChangeTargetTable:
			fallthrough
		case SchemaChangeTargetType:
			sce.Object, source, err = ReadString(source)
			if err != nil {
				return nil, err
			}
		case SchemaChangeTargetAggregate:
			fallthrough
		case SchemaChangeTargetFunction:
			if version < ProtocolVersion4 {
				return nil, errors.New(fmt.Sprintf("%s schema change events are not supported in protocol version %d", sce.Target, version))
			}
			sce.Object, source, err = ReadString(source)
			if err != nil {
				return nil, err
			}
			sce.Arguments, source, err = ReadStringList(source)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(fmt.Sprintf("unknown schema change target: " + sce.Target))
		}
		return sce, nil
	case EventTypeStatusChange:
		sce := &StatusChangeEvent{Event: Event{Type: eventType}}
		sce.ChangeType, source, err = ReadString(source)
		if err != nil {
			return nil, err
		}
		sce.Address, sce.Port, source, err = ReadInet(source)
		if err != nil {
			return nil, err
		}
		return sce, nil
	case EventTypeTopologyChange:
		tce := &TopologyChangeEvent{Event: Event{Type: eventType}}
		tce.ChangeType, source, err = ReadString(source)
		if err != nil {
			return nil, err
		}
		tce.Address, tce.Port, source, err = ReadInet(source)
		if err != nil {
			return nil, err
		}
		return tce, nil
	}
	return nil, errors.New("unknown event type: " + eventType)
}
