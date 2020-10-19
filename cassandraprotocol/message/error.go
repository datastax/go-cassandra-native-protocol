package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Error interface {
	Message
	GetErrorCode() cassandraprotocol.ErrorCode
	GetErrorMessage() string
}

// SERVER ERROR

type ServerError struct {
	ErrorMessage string
}

func (m *ServerError) IsResponse() bool {
	return true
}

func (m *ServerError) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *ServerError) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeServerError
}

func (m *ServerError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ServerError) String() string {
	return fmt.Sprintf("ERROR SERVER ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// PROTOCOL ERROR

type ProtocolError struct {
	ErrorMessage string
}

func (m *ProtocolError) IsResponse() bool {
	return true
}

func (m *ProtocolError) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *ProtocolError) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeProtocolError
}

func (m *ProtocolError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ProtocolError) String() string {
	return fmt.Sprintf("ERROR PROTOCOL ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// AUTHENTICATION ERROR

type AuthenticationError struct {
	ErrorMessage string
}

func (m *AuthenticationError) IsResponse() bool {
	return true
}

func (m *AuthenticationError) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *AuthenticationError) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeAuthenticationError
}

func (m *AuthenticationError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *AuthenticationError) String() string {
	return fmt.Sprintf("ERROR AUTHENTICATION ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// OVERLOADED

type Overloaded struct {
	ErrorMessage string
}

func (m *Overloaded) IsResponse() bool {
	return true
}

func (m *Overloaded) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *Overloaded) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeOverloaded
}

func (m *Overloaded) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Overloaded) String() string {
	return fmt.Sprintf("ERROR OVERLOADED (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// IS BOOTSTRAPPING

type IsBootstrapping struct {
	ErrorMessage string
}

func (m *IsBootstrapping) IsResponse() bool {
	return true
}

func (m *IsBootstrapping) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *IsBootstrapping) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeIsBootstrapping
}

func (m *IsBootstrapping) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *IsBootstrapping) String() string {
	return fmt.Sprintf("ERROR IS BOOTSTRAPPING (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// TRUNCATE ERROR

type TruncateError struct {
	ErrorMessage string
}

func (m *TruncateError) IsResponse() bool {
	return true
}

func (m *TruncateError) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *TruncateError) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeTruncateError
}

func (m *TruncateError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *TruncateError) String() string {
	return fmt.Sprintf("ERROR TRUNCATE ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// SYNTAX ERROR

type SyntaxError struct {
	ErrorMessage string
}

func (m *SyntaxError) IsResponse() bool {
	return true
}

func (m *SyntaxError) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *SyntaxError) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeSyntaxError
}

func (m *SyntaxError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *SyntaxError) String() string {
	return fmt.Sprintf("ERROR SYNTAX ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// UNAUTHORIZED

type Unauthorized struct {
	ErrorMessage string
}

func (m *Unauthorized) IsResponse() bool {
	return true
}

func (m *Unauthorized) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *Unauthorized) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeUnauthorized
}

func (m *Unauthorized) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Unauthorized) String() string {
	return fmt.Sprintf("ERROR UNAUTHORIZED (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// INVALID

type Invalid struct {
	ErrorMessage string
}

func (m *Invalid) IsResponse() bool {
	return true
}

func (m *Invalid) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *Invalid) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeInvalid
}

func (m *Invalid) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Invalid) String() string {
	return fmt.Sprintf("ERROR INVALID (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// CONFIG ERROR

type ConfigError struct {
	ErrorMessage string
}

func (m *ConfigError) IsResponse() bool {
	return true
}

func (m *ConfigError) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *ConfigError) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeConfigError
}

func (m *ConfigError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ConfigError) String() string {
	return fmt.Sprintf("ERROR CONFIG ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// UNAVAILABLE

type Unavailable struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency cassandraprotocol.ConsistencyLevel
	// The number of nodes that should be alive to respect Consistency.
	Required int32
	//The number of replicas that were known to be alive when the request was processed (since an
	// unavailable exception has been triggered, Alive < Required).
	Alive int32
}

func (m *Unavailable) IsResponse() bool {
	return true
}

func (m *Unavailable) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *Unavailable) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeUnavailable
}

func (m *Unavailable) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Unavailable) String() string {
	return fmt.Sprintf(
		"ERROR UNAVAILABLE (code=%v, msg=%v, cl=%v, required=%v, alive=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Consistency,
		m.Required,
		m.Alive,
	)
}

// READ TIMEOUT

type ReadTimeout struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency cassandraprotocol.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	// It is possible to have Received >= BlockFor if DataPresent is false. Also
	// in the (unlikely) case where Consistency is achieved but the coordinator node
	// times out while waiting for read-repair acknowledgement.
	BlockFor int32
	// Whether the replica that was asked for data responded.
	DataPresent bool
}

func (m *ReadTimeout) IsResponse() bool {
	return true
}

func (m *ReadTimeout) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *ReadTimeout) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeReadTimeout
}

func (m *ReadTimeout) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ReadTimeout) String() string {
	return fmt.Sprintf(
		"ERROR READ TIMEOUT (code=%v, msg=%v, cl=%v, received=%v, blockfor=%v, data=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Consistency,
		m.Received,
		m.BlockFor,
		m.DataPresent,
	)
}

// WRITE TIMEOUT

type WriteTimeout struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency cassandraprotocol.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	BlockFor int32
	// The type of the write that failed.
	WriteType cassandraprotocol.WriteType
}

func (m *WriteTimeout) IsResponse() bool {
	return true
}

func (m *WriteTimeout) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *WriteTimeout) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeWriteTimeout
}

func (m *WriteTimeout) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *WriteTimeout) String() string {
	return fmt.Sprintf(
		"ERROR WRITE TIMEOUT (code=%v, msg=%v, cl=%v, received=%v, blockfor=%v, type=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Consistency,
		m.Received,
		m.BlockFor,
		m.WriteType,
	)
}

// READ FAILURE

type ReadFailure struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency cassandraprotocol.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	BlockFor int32
	// The number of nodes that experienced a failure while executing the request.
	// Only filled when the protocol versions is < 5.
	NumFailures int32
	// A map of endpoint to failure reason codes. This maps the endpoints of the replica nodes that
	// failed when executing the request to a code representing the reason for the failure.
	// Only filled when the protocol versions is >= 5.
	ReasonMap map[string]uint16
	// Whether the replica that was asked for data responded.
	DataPresent bool
}

func (m *ReadFailure) IsResponse() bool {
	return true
}

func (m *ReadFailure) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *ReadFailure) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeReadFailure
}

func (m *ReadFailure) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ReadFailure) String() string {
	return fmt.Sprintf(
		"ERROR READ FAILURE (code=%v, msg=%v, cl=%v, received=%v, blockfor=%v, data=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Consistency,
		m.Received,
		m.BlockFor,
		m.DataPresent,
	)
}

// WRITE FAILURE

type WriteFailure struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency cassandraprotocol.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	BlockFor int32
	// The number of nodes that experienced a failure while executing the request.
	// Only filled when the protocol versions is < 5.
	NumFailures int32
	// A map of endpoint to failure reason codes. This maps the endpoints of the replica nodes that
	// failed when executing the request to a code representing the reason for the failure.
	// Only filled when the protocol versions is >= 5.
	ReasonMap map[string]uint16
	// The type of the write that failed.
	WriteType cassandraprotocol.WriteType
}

func (m *WriteFailure) IsResponse() bool {
	return true
}

func (m *WriteFailure) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *WriteFailure) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeWriteFailure
}

func (m *WriteFailure) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *WriteFailure) String() string {
	return fmt.Sprintf(
		"ERROR WRITE FAILURE (code=%v, msg=%v, cl=%v, received=%v, blockfor=%v, type=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Consistency,
		m.Received,
		m.BlockFor,
		m.WriteType,
	)
}

// FUNCTION FAILURE

type FunctionFailure struct {
	ErrorMessage string
	Keyspace     string
	Function     string
	Arguments    []string
}

func (m *FunctionFailure) IsResponse() bool {
	return true
}

func (m *FunctionFailure) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *FunctionFailure) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeFunctionFailure
}

func (m *FunctionFailure) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *FunctionFailure) String() string {
	return fmt.Sprintf(
		"ERROR FUNCTION FAILURE (code=%v, msg=%v, ks=%v, function=%v, args=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Keyspace,
		m.Function,
		m.Arguments,
	)
}

// UNPREPARED

type Unprepared struct {
	ErrorMessage string
	Id           []byte
}

func (m *Unprepared) IsResponse() bool {
	return true
}

func (m *Unprepared) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *Unprepared) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeUnprepared
}

func (m *Unprepared) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Unprepared) String() string {
	return fmt.Sprintf(
		"ERROR UNPREPARED (code=%v, msg=%v, id=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Id,
	)
}

// ALREADY EXISTS

type AlreadyExists struct {
	ErrorMessage string
	Keyspace     string
	Table        string
}

func (m *AlreadyExists) IsResponse() bool {
	return true
}

func (m *AlreadyExists) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}

func (m *AlreadyExists) GetErrorCode() cassandraprotocol.ErrorCode {
	return cassandraprotocol.ErrorCodeAlreadyExists
}

func (m *AlreadyExists) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *AlreadyExists) String() string {
	return fmt.Sprintf(
		"ERROR ALREADY EXISTS (code=%v, msg=%v, ks=%v, table=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Keyspace,
		m.Table,
	)
}

// CODEC

type ErrorCodec struct{}

func (c *ErrorCodec) Encode(msg Message, dest []byte, version cassandraprotocol.ProtocolVersion) (err error) {
	errMsg, ok := msg.(Error)
	if !ok {
		return errors.New(fmt.Sprintf("expected Error interface, got %T", msg))
	}
	if dest, err = primitives.WriteInt(errMsg.GetErrorCode(), dest); err != nil {
		return fmt.Errorf("cannot write ERROR code: %w", err)
	}
	if dest, err = primitives.WriteString(errMsg.GetErrorMessage(), dest); err != nil {
		return fmt.Errorf("cannot write ERROR message: %w", err)
	}
	switch errMsg.GetErrorCode() {
	case cassandraprotocol.ErrorCodeServerError:
	case cassandraprotocol.ErrorCodeProtocolError:
	case cassandraprotocol.ErrorCodeAuthenticationError:
	case cassandraprotocol.ErrorCodeOverloaded:
	case cassandraprotocol.ErrorCodeIsBootstrapping:
	case cassandraprotocol.ErrorCodeTruncateError:
	case cassandraprotocol.ErrorCodeSyntaxError:
	case cassandraprotocol.ErrorCodeUnauthorized:
	case cassandraprotocol.ErrorCodeInvalid:
	case cassandraprotocol.ErrorCodeConfigError:

	case cassandraprotocol.ErrorCodeUnavailable:
		unavailable, ok := errMsg.(*Unavailable)
		if !ok {
			return errors.New(fmt.Sprintf("expected Unavailable, got %T", msg))
		}
		if dest, err = primitives.WriteShort(unavailable.Consistency, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNAVAILABLE consistency: %w", err)
		}
		if dest, err = primitives.WriteInt(unavailable.Required, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNAVAILABLE required: %w", err)
		}
		if dest, err = primitives.WriteInt(unavailable.Alive, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNAVAILABLE alive: %w", err)
		}

	case cassandraprotocol.ErrorCodeReadTimeout:
		readTimeout, ok := errMsg.(*ReadTimeout)
		if !ok {
			return errors.New(fmt.Sprintf("expected ReadTimeout, got %T", msg))
		}
		if dest, err = primitives.WriteShort(readTimeout.Consistency, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT consistency: %w", err)
		}
		if dest, err = primitives.WriteInt(readTimeout.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT received: %w", err)
		}
		if dest, err = primitives.WriteInt(readTimeout.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT block for: %w", err)
		}
		if readTimeout.DataPresent {
			dest, err = primitives.WriteByte(1, dest)
		} else {
			dest, err = primitives.WriteByte(0, dest)
		}
		if err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT data present: %w", err)
		}

	case cassandraprotocol.ErrorCodeWriteTimeout:
		writeTimeout, ok := errMsg.(*WriteTimeout)
		if !ok {
			return errors.New(fmt.Sprintf("expected WriteTimeout, got %T", msg))
		}
		if dest, err = primitives.WriteShort(writeTimeout.Consistency, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT consistency: %w", err)
		}
		if dest, err = primitives.WriteInt(writeTimeout.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT received: %w", err)
		}
		if dest, err = primitives.WriteInt(writeTimeout.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT block for: %w", err)
		}
		if dest, err = primitives.WriteString(writeTimeout.WriteType, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT write type: %w", err)
		}

	case cassandraprotocol.ErrorCodeReadFailure:
		readFailure, ok := errMsg.(*ReadFailure)
		if !ok {
			return errors.New(fmt.Sprintf("expected ReadFailure, got %T", msg))
		}
		if dest, err = primitives.WriteShort(readFailure.Consistency, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE consistency: %w", err)
		}
		if dest, err = primitives.WriteInt(readFailure.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE received: %w", err)
		}
		if dest, err = primitives.WriteInt(readFailure.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE block for: %w", err)
		}
		if version > cassandraprotocol.ProtocolVersion5 {
			if dest, err = primitives.WriteReasonMap(readFailure.ReasonMap, dest); err != nil {
				return fmt.Errorf("cannot write ERROR READ FAILURE reason map: %w", err)
			}
		} else {
			if dest, err = primitives.WriteInt(readFailure.NumFailures, dest); err != nil {
				return fmt.Errorf("cannot write ERROR READ FAILURE num failures: %w", err)
			}
		}
		if readFailure.DataPresent {
			dest, err = primitives.WriteByte(1, dest)
		} else {
			dest, err = primitives.WriteByte(0, dest)
		}
		if err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE data present: %w", err)
		}

	case cassandraprotocol.ErrorCodeWriteFailure:
		writeFailure, ok := errMsg.(*WriteFailure)
		if !ok {
			return errors.New(fmt.Sprintf("expected WriteFailure, got %T", msg))
		}
		if dest, err = primitives.WriteShort(writeFailure.Consistency, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE consistency: %w", err)
		}
		if dest, err = primitives.WriteInt(writeFailure.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE received: %w", err)
		}
		if dest, err = primitives.WriteInt(writeFailure.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE block for: %w", err)
		}
		if version > cassandraprotocol.ProtocolVersion5 {
			if dest, err = primitives.WriteReasonMap(writeFailure.ReasonMap, dest); err != nil {
				return fmt.Errorf("cannot write ERROR WRITE FAILURE reason map: %w", err)
			}
		} else {
			if dest, err = primitives.WriteInt(writeFailure.NumFailures, dest); err != nil {
				return fmt.Errorf("cannot write ERROR WRITE FAILURE num failures: %w", err)
			}
		}
		if dest, err = primitives.WriteString(writeFailure.WriteType, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE write type: %w", err)
		}

	case cassandraprotocol.ErrorCodeFunctionFailure:
		functionFailure, ok := errMsg.(*FunctionFailure)
		if !ok {
			return errors.New(fmt.Sprintf("expected FunctionFailure, got %T", msg))
		}
		if dest, err = primitives.WriteString(functionFailure.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write ERROR FUNCTION FAILURE keyspace: %w", err)
		}
		if dest, err = primitives.WriteString(functionFailure.Function, dest); err != nil {
			return fmt.Errorf("cannot write ERROR FUNCTION FAILURE function: %w", err)
		}
		if dest, err = primitives.WriteStringList(functionFailure.Arguments, dest); err != nil {
			return fmt.Errorf("cannot write ERROR FUNCTION FAILURE arguments: %w", err)
		}

	case cassandraprotocol.ErrorCodeAlreadyExists:
		alreadyExists, ok := errMsg.(*AlreadyExists)
		if !ok {
			return errors.New(fmt.Sprintf("expected AlreadyExists, got %T", msg))
		}
		if dest, err = primitives.WriteString(alreadyExists.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write ERROR ALREADY EXISTS keyspace: %w", err)
		}
		if dest, err = primitives.WriteString(alreadyExists.Table, dest); err != nil {
			return fmt.Errorf("cannot write ERROR ALREADY EXISTS table: %w", err)
		}

	case cassandraprotocol.ErrorCodeUnprepared:
		unprepared, ok := errMsg.(*Unprepared)
		if !ok {
			return errors.New(fmt.Sprintf("expected Unprepared, got %T", msg))
		}
		if dest, err = primitives.WriteShortBytes(unprepared.Id, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNPREPARED id: %w", err)
		}

	default:
		err = errors.New(fmt.Sprintf("unknown error code: %v", errMsg.GetErrorCode()))
	}
	return err
}

func (c *ErrorCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	errMsg := msg.(Error)
	length += primitives.LengthOfInt // error code
	length += primitives.LengthOfString(errMsg.GetErrorMessage())
	switch errMsg.GetErrorCode() {
	case cassandraprotocol.ErrorCodeServerError:
	case cassandraprotocol.ErrorCodeProtocolError:
	case cassandraprotocol.ErrorCodeAuthenticationError:
	case cassandraprotocol.ErrorCodeOverloaded:
	case cassandraprotocol.ErrorCodeIsBootstrapping:
	case cassandraprotocol.ErrorCodeTruncateError:
	case cassandraprotocol.ErrorCodeSyntaxError:
	case cassandraprotocol.ErrorCodeUnauthorized:
	case cassandraprotocol.ErrorCodeInvalid:
	case cassandraprotocol.ErrorCodeConfigError:

	case cassandraprotocol.ErrorCodeUnavailable:
		length += primitives.LengthOfShort // consistency
		length += primitives.LengthOfInt   // required
		length += primitives.LengthOfInt   // alive

	case cassandraprotocol.ErrorCodeReadTimeout:
		length += primitives.LengthOfShort // consistency
		length += primitives.LengthOfInt   // received
		length += primitives.LengthOfInt   // block for
		length += primitives.LengthOfByte  // data present

	case cassandraprotocol.ErrorCodeWriteTimeout:
		length += primitives.LengthOfShort // consistency
		length += primitives.LengthOfInt   // received
		length += primitives.LengthOfInt   // block for
		length += primitives.LengthOfByte  // write type

	case cassandraprotocol.ErrorCodeReadFailure:
		length += primitives.LengthOfShort // consistency
		length += primitives.LengthOfInt   // received
		length += primitives.LengthOfInt   // block for
		length += primitives.LengthOfByte  // data present
		if version > cassandraprotocol.ProtocolVersion5 {
			readFailure, ok := errMsg.(*ReadFailure)
			if !ok {
				return -1, errors.New(fmt.Sprintf("expected ReadFailure, got %T", msg))
			}
			if reasonMapLength, err := primitives.LengthOfReasonMap(readFailure.ReasonMap); err != nil {
				return -1, fmt.Errorf("cannot compute length of ERROR READ FAILURE rason map: %w", err)
			} else {
				return length + reasonMapLength, nil
			}
		} else {
			return length + primitives.LengthOfInt /* num failures */, nil
		}

	case cassandraprotocol.ErrorCodeWriteFailure:
		length += primitives.LengthOfShort // consistency
		length += primitives.LengthOfInt   // received
		length += primitives.LengthOfInt   // block for
		length += primitives.LengthOfByte  // write type
		if version > cassandraprotocol.ProtocolVersion5 {
			writeFailure, ok := errMsg.(*WriteFailure)
			if !ok {
				return -1, errors.New(fmt.Sprintf("expected WriteFailure, got %T", msg))
			}
			if reasonMapLength, err := primitives.LengthOfReasonMap(writeFailure.ReasonMap); err != nil {
				return -1, fmt.Errorf("cannot compute length of ERROR WRITE FAILURE rason map: %w", err)
			} else {
				return length + reasonMapLength, nil
			}
		} else {
			return length + primitives.LengthOfInt /* num failures */, nil
		}

	case cassandraprotocol.ErrorCodeFunctionFailure:
		functionFailure := errMsg.(*FunctionFailure)
		length += primitives.LengthOfString(functionFailure.Keyspace)
		length += primitives.LengthOfString(functionFailure.Function)
		length += primitives.LengthOfStringList(functionFailure.Arguments)

	case cassandraprotocol.ErrorCodeAlreadyExists:
		alreadyExists := errMsg.(*AlreadyExists)
		length += primitives.LengthOfString(alreadyExists.Keyspace)
		length += primitives.LengthOfString(alreadyExists.Table)

	case cassandraprotocol.ErrorCodeUnprepared:
		unprepared := errMsg.(*Unprepared)
		length += primitives.LengthOfShortBytes(unprepared.Id)

	default:
		err = errors.New(fmt.Sprintf("unknown error code: %v", errMsg.GetErrorCode()))

	}
	return
}

func (c *ErrorCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	var code cassandraprotocol.ErrorCode
	if code, source, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read ERROR code: %w", err)
	}
	var errorMsg string
	if errorMsg, source, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read ERROR message: %w", err)
	}
	switch code {
	case cassandraprotocol.ErrorCodeServerError:
		return &ServerError{errorMsg}, nil
	case cassandraprotocol.ErrorCodeProtocolError:
		return &ProtocolError{errorMsg}, nil
	case cassandraprotocol.ErrorCodeAuthenticationError:
		return &AuthenticationError{errorMsg}, nil
	case cassandraprotocol.ErrorCodeOverloaded:
		return &Overloaded{errorMsg}, nil
	case cassandraprotocol.ErrorCodeIsBootstrapping:
		return &IsBootstrapping{errorMsg}, nil
	case cassandraprotocol.ErrorCodeTruncateError:
		return &TruncateError{errorMsg}, nil
	case cassandraprotocol.ErrorCodeSyntaxError:
		return &SyntaxError{errorMsg}, nil
	case cassandraprotocol.ErrorCodeUnauthorized:
		return &Unauthorized{errorMsg}, nil
	case cassandraprotocol.ErrorCodeInvalid:
		return &Invalid{errorMsg}, nil
	case cassandraprotocol.ErrorCodeConfigError:
		return &ConfigError{errorMsg}, nil

	case cassandraprotocol.ErrorCodeUnavailable:
		var msg = &Unavailable{}
		if msg.Consistency, source, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNAVAILABLE consistency: %w", err)
		}
		if msg.Required, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNAVAILABLE required: %w", err)
		}
		if msg.Alive, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNAVAILABLE alive: %w", err)
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeReadTimeout:
		var msg = &ReadTimeout{}
		if msg.Consistency, source, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT consistency: %w", err)
		}
		if msg.Received, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT received: %w", err)
		}
		if msg.BlockFor, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT block for: %w", err)
		}
		var b byte
		if b, source, err = primitives.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT data present: %w", err)
		}
		if b > 0 {
			msg.DataPresent = true
		} else {
			msg.DataPresent = false
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeWriteTimeout:
		var msg = &WriteTimeout{}
		if msg.Consistency, source, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT consistency: %w", err)
		}
		if msg.Received, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT received: %w", err)
		}
		if msg.BlockFor, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT block for: %w", err)
		}
		if msg.WriteType, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT write type: %w", err)
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeReadFailure:
		var msg = &ReadFailure{}
		if msg.Consistency, source, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE consistency: %w", err)
		}
		if msg.Received, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE received: %w", err)
		}
		if msg.BlockFor, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE block for: %w", err)
		}
		if version > cassandraprotocol.ProtocolVersion5 {
			if msg.ReasonMap, source, err = primitives.ReadReasonMap(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR READ FAILURE reason map: %w", err)
			}
		} else {
			if msg.NumFailures, source, err = primitives.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR READ FAILURE num failures: %w", err)
			}
		}
		var b byte
		if b, source, err = primitives.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE data present: %w", err)
		}
		if b > 0 {
			msg.DataPresent = true
		} else {
			msg.DataPresent = false
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeWriteFailure:
		var msg = &WriteFailure{}
		if msg.Consistency, source, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE consistency: %w", err)
		}
		if msg.Received, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE received: %w", err)
		}
		if msg.BlockFor, source, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE block for: %w", err)
		}
		if version > cassandraprotocol.ProtocolVersion5 {
			if msg.ReasonMap, source, err = primitives.ReadReasonMap(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE reason map: %w", err)
			}
		} else {
			if msg.NumFailures, source, err = primitives.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE num failures: %w", err)
			}
		}
		if msg.WriteType, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE write type: %w", err)
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeFunctionFailure:
		var msg = &FunctionFailure{}
		if msg.Keyspace, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR FUNCTION FAILURE keyspace: %w", err)
		}
		if msg.Function, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR FUNCTION FAILURE function: %w", err)
		}
		if msg.Arguments, source, err = primitives.ReadStringList(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR FUNCTION FAILURE arguments: %w", err)
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeAlreadyExists:
		var msg = &AlreadyExists{}
		if msg.Keyspace, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR ALREADY EXISTS keyspace: %w", err)
		}
		if msg.Table, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR ALREADY EXISTS table: %w", err)
		}
		return msg, nil

	case cassandraprotocol.ErrorCodeUnprepared:
		var msg = &Unprepared{}
		if msg.Id, source, err = primitives.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNPREPARED id: %w", err)
		}
		return msg, nil

	default:
		err = errors.New(fmt.Sprintf("unknown error code: %v", code))

	}
	return msg, err
}

func (c *ErrorCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeError
}
