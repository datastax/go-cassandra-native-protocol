// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type Error interface {
	Message
	GetErrorCode() primitive.ErrorCode
	GetErrorMessage() string
}

// SERVER ERROR

// ServerError is a server error response.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type ServerError struct {
	ErrorMessage string
}

func (m *ServerError) IsResponse() bool {
	return true
}

func (m *ServerError) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *ServerError) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeServerError
}

func (m *ServerError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ServerError) String() string {
	return fmt.Sprintf("ERROR SERVER ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// PROTOCOL ERROR

// ProtocolError is a protocol error response.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type ProtocolError struct {
	ErrorMessage string
}

func (m *ProtocolError) IsResponse() bool {
	return true
}

func (m *ProtocolError) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *ProtocolError) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeProtocolError
}

func (m *ProtocolError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ProtocolError) String() string {
	return fmt.Sprintf("ERROR PROTOCOL ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// AUTHENTICATION ERROR

// AuthenticationError is an authentication error response.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type AuthenticationError struct {
	ErrorMessage string
}

func (m *AuthenticationError) IsResponse() bool {
	return true
}

func (m *AuthenticationError) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *AuthenticationError) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeAuthenticationError
}

func (m *AuthenticationError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *AuthenticationError) String() string {
	return fmt.Sprintf("ERROR AUTHENTICATION ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// OVERLOADED

// Overloaded is an error response sent when the coordinator is overloaded.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Overloaded struct {
	ErrorMessage string
}

func (m *Overloaded) IsResponse() bool {
	return true
}

func (m *Overloaded) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *Overloaded) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeOverloaded
}

func (m *Overloaded) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Overloaded) String() string {
	return fmt.Sprintf("ERROR OVERLOADED (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// IS BOOTSTRAPPING

// IsBootstrapping is an error response sent when the coordinator is bootstrapping.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type IsBootstrapping struct {
	ErrorMessage string
}

func (m *IsBootstrapping) IsResponse() bool {
	return true
}

func (m *IsBootstrapping) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *IsBootstrapping) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeIsBootstrapping
}

func (m *IsBootstrapping) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *IsBootstrapping) String() string {
	return fmt.Sprintf("ERROR IS BOOTSTRAPPING (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// TRUNCATE ERROR

// TruncateError is an error response notifying that a TRUNCATE statement failed.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type TruncateError struct {
	ErrorMessage string
}

func (m *TruncateError) IsResponse() bool {
	return true
}

func (m *TruncateError) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *TruncateError) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeTruncateError
}

func (m *TruncateError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *TruncateError) String() string {
	return fmt.Sprintf("ERROR TRUNCATE ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// SYNTAX ERROR

// SyntaxError is an error response notifying that the query has a syntax error.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type SyntaxError struct {
	ErrorMessage string
}

func (m *SyntaxError) IsResponse() bool {
	return true
}

func (m *SyntaxError) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *SyntaxError) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeSyntaxError
}

func (m *SyntaxError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *SyntaxError) String() string {
	return fmt.Sprintf("ERROR SYNTAX ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// UNAUTHORIZED

// Unauthorized is an error response notifying that the logged user is not authorized to perform the request.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Unauthorized struct {
	ErrorMessage string
}

func (m *Unauthorized) IsResponse() bool {
	return true
}

func (m *Unauthorized) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *Unauthorized) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeUnauthorized
}

func (m *Unauthorized) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Unauthorized) String() string {
	return fmt.Sprintf("ERROR UNAUTHORIZED (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// INVALID

// Invalid is an error response sent when the query is syntactically correct but invalid.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Invalid struct {
	ErrorMessage string
}

func (m *Invalid) IsResponse() bool {
	return true
}

func (m *Invalid) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *Invalid) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeInvalid
}

func (m *Invalid) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *Invalid) String() string {
	return fmt.Sprintf("ERROR INVALID (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// CONFIG ERROR

// ConfigError is an error response sent when the query cannot be executed due to some configuration issue.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type ConfigError struct {
	ErrorMessage string
}

func (m *ConfigError) IsResponse() bool {
	return true
}

func (m *ConfigError) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *ConfigError) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeConfigError
}

func (m *ConfigError) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *ConfigError) String() string {
	return fmt.Sprintf("ERROR CONFIG ERROR (code=%v, msg=%v)", m.GetErrorCode(), m.ErrorMessage)
}

// UNAVAILABLE

// Unavailable is an error response sent when the coordinator knows that the consistency level cannot be fulfilled.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Unavailable struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency primitive.ConsistencyLevel
	// The number of nodes that should be alive to respect Consistency.
	Required int32
	// The number of replicas that were known to be alive when the request was processed (since an
	// unavailable exception has been triggered, Alive < Required).
	Alive int32
}

func (m *Unavailable) IsResponse() bool {
	return true
}

func (m *Unavailable) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *Unavailable) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeUnavailable
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

// ReadTimeout is an error response sent when the coordinator does not receive enough responses from replicas for a read
// query.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type ReadTimeout struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency primitive.ConsistencyLevel
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

func (m *ReadTimeout) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *ReadTimeout) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeReadTimeout
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

// WriteTimeout is an error response sent when the coordinator does not receive enough responses from replicas for a
// write query.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type WriteTimeout struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency primitive.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	BlockFor int32
	// The type of the write that failed.
	WriteType primitive.WriteType
	// The number of contentions occurred during the CAS operation. This field is only present when WriteType is "CAS".
	Contentions uint16
}

func (m *WriteTimeout) IsResponse() bool {
	return true
}

func (m *WriteTimeout) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *WriteTimeout) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeWriteTimeout
}

func (m *WriteTimeout) GetErrorMessage() string {
	return m.ErrorMessage
}

func (m *WriteTimeout) String() string {
	return fmt.Sprintf(
		"ERROR WRITE TIMEOUT (code=%v, msg=%v, cl=%v, received=%v, blockfor=%v, type=%v, contentions=%v)",
		m.GetErrorCode(),
		m.GetErrorMessage(),
		m.Consistency,
		m.Received,
		m.BlockFor,
		m.WriteType,
		m.Contentions,
	)
}

// READ FAILURE

// ReadFailure is an error response sent when the coordinator receives a read failure from a replica.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type ReadFailure struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency primitive.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	BlockFor int32
	// The number of nodes that experienced a failure while executing the request.
	// Only filled when the protocol versions is < 5.
	NumFailures int32
	// A collection of endpoints with failure reason codes. Only filled when the protocol versions is >= 5, including
	// DSE versions v1 and v2.
	FailureReasons []*primitive.FailureReason
	// Whether the replica that was asked for data responded.
	DataPresent bool
}

func (m *ReadFailure) IsResponse() bool {
	return true
}

func (m *ReadFailure) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *ReadFailure) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeReadFailure
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

// WriteFailure is an error response sent when the coordinator receives a write failure from a replica.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type WriteFailure struct {
	ErrorMessage string
	// The consistency level of the query that triggered the exception.
	Consistency primitive.ConsistencyLevel
	// The number of nodes having answered the request.
	Received int32
	// The number of replicas whose response is required to achieve Consistency.
	BlockFor int32
	// The number of nodes that experienced a failure while executing the request.
	// Only filled when the protocol versions is < 5.
	NumFailures int32
	// A collection of endpoints with failure reason codes. Only filled when the protocol versions is >= 5, including
	// DSE versions v1 and v2.
	FailureReasons []*primitive.FailureReason
	// The type of the write that failed.
	WriteType primitive.WriteType
}

func (m *WriteFailure) IsResponse() bool {
	return true
}

func (m *WriteFailure) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *WriteFailure) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeWriteFailure
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

// FunctionFailure is an error response sent when the coordinator receives an error from a replica while executing a
// function.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type FunctionFailure struct {
	ErrorMessage string
	Keyspace     string
	Function     string
	Arguments    []string
}

func (m *FunctionFailure) IsResponse() bool {
	return true
}

func (m *FunctionFailure) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *FunctionFailure) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeFunctionFailure
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

// Unprepared is an error response sent when an unprepared query execution is attempted.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Unprepared struct {
	ErrorMessage string
	Id           []byte
}

func (m *Unprepared) IsResponse() bool {
	return true
}

func (m *Unprepared) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *Unprepared) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeUnprepared
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

// AlreadyExists is an error response sent when the creation of a schema object fails because the object already exists.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type AlreadyExists struct {
	ErrorMessage string
	Keyspace     string
	Table        string
}

func (m *AlreadyExists) IsResponse() bool {
	return true
}

func (m *AlreadyExists) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}

func (m *AlreadyExists) GetErrorCode() primitive.ErrorCode {
	return primitive.ErrorCodeAlreadyExists
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

type errorCodec struct{}

func (c *errorCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	errMsg, ok := msg.(Error)
	if !ok {
		return fmt.Errorf("expected Error, got %T", msg)
	}
	if err = primitive.WriteInt(int32(errMsg.GetErrorCode()), dest); err != nil {
		return fmt.Errorf("cannot write ERROR code: %w", err)
	}
	if err = primitive.WriteString(errMsg.GetErrorMessage(), dest); err != nil {
		return fmt.Errorf("cannot write ERROR message: %w", err)
	}
	switch errMsg.GetErrorCode() {
	case primitive.ErrorCodeServerError:
	case primitive.ErrorCodeProtocolError:
	case primitive.ErrorCodeAuthenticationError:
	case primitive.ErrorCodeOverloaded:
	case primitive.ErrorCodeIsBootstrapping:
	case primitive.ErrorCodeTruncateError:
	case primitive.ErrorCodeSyntaxError:
	case primitive.ErrorCodeUnauthorized:
	case primitive.ErrorCodeInvalid:
	case primitive.ErrorCodeConfigError:

	case primitive.ErrorCodeUnavailable:
		unavailable, ok := errMsg.(*Unavailable)
		if !ok {
			return fmt.Errorf("expected *message.Unavailable, got %T", msg)
		}
		if err = primitive.WriteShort(uint16(unavailable.Consistency), dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNAVAILABLE consistency: %w", err)
		} else if err = primitive.WriteInt(unavailable.Required, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNAVAILABLE required: %w", err)
		} else if err = primitive.WriteInt(unavailable.Alive, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNAVAILABLE alive: %w", err)
		}

	case primitive.ErrorCodeReadTimeout:
		readTimeout, ok := errMsg.(*ReadTimeout)
		if !ok {
			return fmt.Errorf("expected *message.ReadTimeout, got %T", msg)
		}
		if err = primitive.WriteShort(uint16(readTimeout.Consistency), dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT consistency: %w", err)
		} else if err = primitive.WriteInt(readTimeout.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT received: %w", err)
		} else if err = primitive.WriteInt(readTimeout.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT block for: %w", err)
		}
		if readTimeout.DataPresent {
			err = primitive.WriteByte(1, dest)
		} else {
			err = primitive.WriteByte(0, dest)
		}
		if err != nil {
			return fmt.Errorf("cannot write ERROR READ TIMEOUT data present: %w", err)
		}

	case primitive.ErrorCodeWriteTimeout:
		writeTimeout, ok := errMsg.(*WriteTimeout)
		if !ok {
			return fmt.Errorf("expected *message.WriteTimeout, got %T", msg)
		}
		if err = primitive.WriteShort(uint16(writeTimeout.Consistency), dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT consistency: %w", err)
		} else if err = primitive.WriteInt(writeTimeout.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT received: %w", err)
		} else if err = primitive.WriteInt(writeTimeout.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT block for: %w", err)
		} else if err = primitive.WriteString(string(writeTimeout.WriteType), dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE TIMEOUT write type: %w", err)
		} else if version.SupportsWriteTimeoutContentions() && writeTimeout.WriteType == primitive.WriteTypeCas {
			if err = primitive.WriteShort(uint16(writeTimeout.Contentions), dest); err != nil {
				return fmt.Errorf("cannot write ERROR WRITE TIMEOUT contentions: %w", err)
			}
		}

	case primitive.ErrorCodeReadFailure:
		readFailure, ok := errMsg.(*ReadFailure)
		if !ok {
			return fmt.Errorf("expected *message.ReadFailure, got %T", msg)
		}
		if err = primitive.WriteShort(uint16(readFailure.Consistency), dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE consistency: %w", err)
		} else if err = primitive.WriteInt(readFailure.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE received: %w", err)
		} else if err = primitive.WriteInt(readFailure.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE block for: %w", err)
		}
		if version.SupportsReadWriteFailureReasonMap() {
			if err = primitive.WriteReasonMap(readFailure.FailureReasons, dest); err != nil {
				return fmt.Errorf("cannot write ERROR READ FAILURE reason map: %w", err)
			}
		} else {
			if err = primitive.WriteInt(readFailure.NumFailures, dest); err != nil {
				return fmt.Errorf("cannot write ERROR READ FAILURE num failures: %w", err)
			}
		}
		if readFailure.DataPresent {
			err = primitive.WriteByte(1, dest)
		} else {
			err = primitive.WriteByte(0, dest)
		}
		if err != nil {
			return fmt.Errorf("cannot write ERROR READ FAILURE data present: %w", err)
		}

	case primitive.ErrorCodeWriteFailure:
		writeFailure, ok := errMsg.(*WriteFailure)
		if !ok {
			return fmt.Errorf("expected *message.WriteFailure, got %T", msg)
		}
		if err = primitive.WriteShort(uint16(writeFailure.Consistency), dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE consistency: %w", err)
		} else if err = primitive.WriteInt(writeFailure.Received, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE received: %w", err)
		} else if err = primitive.WriteInt(writeFailure.BlockFor, dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE block for: %w", err)
		}
		if version.SupportsReadWriteFailureReasonMap() {
			if err = primitive.WriteReasonMap(writeFailure.FailureReasons, dest); err != nil {
				return fmt.Errorf("cannot write ERROR WRITE FAILURE reason map: %w", err)
			}
		} else {
			if err = primitive.WriteInt(writeFailure.NumFailures, dest); err != nil {
				return fmt.Errorf("cannot write ERROR WRITE FAILURE num failures: %w", err)
			}
		}
		if err = primitive.WriteString(string(writeFailure.WriteType), dest); err != nil {
			return fmt.Errorf("cannot write ERROR WRITE FAILURE write type: %w", err)
		}

	case primitive.ErrorCodeFunctionFailure:
		functionFailure, ok := errMsg.(*FunctionFailure)
		if !ok {
			return fmt.Errorf("expected *message.FunctionFailure, got %T", msg)
		}
		if err = primitive.WriteString(functionFailure.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write ERROR FUNCTION FAILURE keyspace: %w", err)
		} else if err = primitive.WriteString(functionFailure.Function, dest); err != nil {
			return fmt.Errorf("cannot write ERROR FUNCTION FAILURE function: %w", err)
		} else if err = primitive.WriteStringList(functionFailure.Arguments, dest); err != nil {
			return fmt.Errorf("cannot write ERROR FUNCTION FAILURE arguments: %w", err)
		}

	case primitive.ErrorCodeAlreadyExists:
		alreadyExists, ok := errMsg.(*AlreadyExists)
		if !ok {
			return fmt.Errorf("expected *message.AlreadyExists, got %T", msg)
		}
		if err = primitive.WriteString(alreadyExists.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write ERROR ALREADY EXISTS keyspace: %w", err)
		} else if err = primitive.WriteString(alreadyExists.Table, dest); err != nil {
			return fmt.Errorf("cannot write ERROR ALREADY EXISTS table: %w", err)
		}

	case primitive.ErrorCodeUnprepared:
		unprepared, ok := errMsg.(*Unprepared)
		if !ok {
			return fmt.Errorf("expected *message.Unprepared, got %T", msg)
		}
		if err = primitive.WriteShortBytes(unprepared.Id, dest); err != nil {
			return fmt.Errorf("cannot write ERROR UNPREPARED id: %w", err)
		}

	default:
		err = fmt.Errorf("unknown ERROR code: %v", errMsg.GetErrorCode())
	}
	return err
}

func (c *errorCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (length int, err error) {
	errMsg := msg.(Error)
	length += primitive.LengthOfInt // error code
	length += primitive.LengthOfString(errMsg.GetErrorMessage())
	switch errMsg.GetErrorCode() {
	case primitive.ErrorCodeServerError:
	case primitive.ErrorCodeProtocolError:
	case primitive.ErrorCodeAuthenticationError:
	case primitive.ErrorCodeOverloaded:
	case primitive.ErrorCodeIsBootstrapping:
	case primitive.ErrorCodeTruncateError:
	case primitive.ErrorCodeSyntaxError:
	case primitive.ErrorCodeUnauthorized:
	case primitive.ErrorCodeInvalid:
	case primitive.ErrorCodeConfigError:

	case primitive.ErrorCodeUnavailable:
		length += primitive.LengthOfShort // consistency
		length += primitive.LengthOfInt   // required
		length += primitive.LengthOfInt   // alive

	case primitive.ErrorCodeReadTimeout:
		length += primitive.LengthOfShort // consistency
		length += primitive.LengthOfInt   // received
		length += primitive.LengthOfInt   // block for
		length += primitive.LengthOfByte  // data present

	case primitive.ErrorCodeWriteTimeout:
		writeTimeout, ok := errMsg.(*WriteTimeout)
		if !ok {
			return -1, fmt.Errorf("expected *message.WriteTimeout, got %T", msg)
		}
		length += primitive.LengthOfShort                                  // consistency
		length += primitive.LengthOfInt                                    // received
		length += primitive.LengthOfInt                                    // block for
		length += primitive.LengthOfString(string(writeTimeout.WriteType)) // write type
		if version.SupportsWriteTimeoutContentions() && writeTimeout.WriteType == primitive.WriteTypeCas {
			length += primitive.LengthOfShort // contentions
		}

	case primitive.ErrorCodeReadFailure:
		length += primitive.LengthOfShort // consistency
		length += primitive.LengthOfInt   // received
		length += primitive.LengthOfInt   // block for
		length += primitive.LengthOfByte  // data present
		if version.SupportsReadWriteFailureReasonMap() {
			readFailure, ok := errMsg.(*ReadFailure)
			if !ok {
				return -1, fmt.Errorf("expected *message.ReadFailure, got %T", msg)
			}
			if reasonMapLength, err := primitive.LengthOfReasonMap(readFailure.FailureReasons); err != nil {
				return -1, fmt.Errorf("cannot compute length of ERROR READ FAILURE rason map: %w", err)
			} else {
				return length + reasonMapLength, nil
			}
		} else {
			return length + primitive.LengthOfInt /* num failures */, nil
		}

	case primitive.ErrorCodeWriteFailure:
		writeFailure, ok := errMsg.(*WriteFailure)
		if !ok {
			return -1, fmt.Errorf("expected *message.WriteFailure, got %T", msg)
		}
		length += primitive.LengthOfShort                                  // consistency
		length += primitive.LengthOfInt                                    // received
		length += primitive.LengthOfInt                                    // block for
		length += primitive.LengthOfString(string(writeFailure.WriteType)) // write type
		if version.SupportsReadWriteFailureReasonMap() {
			if reasonMapLength, err := primitive.LengthOfReasonMap(writeFailure.FailureReasons); err != nil {
				return -1, fmt.Errorf("cannot compute length of ERROR WRITE FAILURE rason map: %w", err)
			} else {
				return length + reasonMapLength, nil
			}
		} else {
			return length + primitive.LengthOfInt /* num failures */, nil
		}

	case primitive.ErrorCodeFunctionFailure:
		functionFailure := errMsg.(*FunctionFailure)
		length += primitive.LengthOfString(functionFailure.Keyspace)
		length += primitive.LengthOfString(functionFailure.Function)
		length += primitive.LengthOfStringList(functionFailure.Arguments)

	case primitive.ErrorCodeAlreadyExists:
		alreadyExists := errMsg.(*AlreadyExists)
		length += primitive.LengthOfString(alreadyExists.Keyspace)
		length += primitive.LengthOfString(alreadyExists.Table)

	case primitive.ErrorCodeUnprepared:
		unprepared := errMsg.(*Unprepared)
		length += primitive.LengthOfShortBytes(unprepared.Id)

	default:
		err = fmt.Errorf("unknown ERROR code: %v", errMsg.GetErrorCode())

	}
	return
}

func (c *errorCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	var code int32
	if code, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read ERROR code: %w", err)
	}
	var errorMsg string
	if errorMsg, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read ERROR message: %w", err)
	}
	switch primitive.ErrorCode(code) {
	case primitive.ErrorCodeServerError:
		return &ServerError{errorMsg}, nil
	case primitive.ErrorCodeProtocolError:
		return &ProtocolError{errorMsg}, nil
	case primitive.ErrorCodeAuthenticationError:
		return &AuthenticationError{errorMsg}, nil
	case primitive.ErrorCodeOverloaded:
		return &Overloaded{errorMsg}, nil
	case primitive.ErrorCodeIsBootstrapping:
		return &IsBootstrapping{errorMsg}, nil
	case primitive.ErrorCodeTruncateError:
		return &TruncateError{errorMsg}, nil
	case primitive.ErrorCodeSyntaxError:
		return &SyntaxError{errorMsg}, nil
	case primitive.ErrorCodeUnauthorized:
		return &Unauthorized{errorMsg}, nil
	case primitive.ErrorCodeInvalid:
		return &Invalid{errorMsg}, nil
	case primitive.ErrorCodeConfigError:
		return &ConfigError{errorMsg}, nil

	case primitive.ErrorCodeUnavailable:
		var msg = &Unavailable{ErrorMessage: errorMsg}
		var consistency uint16
		if consistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNAVAILABLE consistency: %w", err)
		}
		msg.Consistency = primitive.ConsistencyLevel(consistency)
		if msg.Required, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNAVAILABLE required: %w", err)
		}
		if msg.Alive, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNAVAILABLE alive: %w", err)
		}
		return msg, nil

	case primitive.ErrorCodeReadTimeout:
		var msg = &ReadTimeout{ErrorMessage: errorMsg}
		var consistency uint16
		if consistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT consistency: %w", err)
		}
		msg.Consistency = primitive.ConsistencyLevel(consistency)
		if msg.Received, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT received: %w", err)
		}
		if msg.BlockFor, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT block for: %w", err)
		}
		var b byte
		if b, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ TIMEOUT data present: %w", err)
		}
		if b > 0 {
			msg.DataPresent = true
		} else {
			msg.DataPresent = false
		}
		return msg, nil

	case primitive.ErrorCodeWriteTimeout:
		var msg = &WriteTimeout{ErrorMessage: errorMsg}
		var consistency uint16
		if consistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT consistency: %w", err)
		}
		msg.Consistency = primitive.ConsistencyLevel(consistency)
		if msg.Received, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT received: %w", err)
		}
		if msg.BlockFor, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT block for: %w", err)
		}
		var writeType string
		if writeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT write type: %w", err)
		}
		msg.WriteType = primitive.WriteType(writeType)
		if msg.WriteType == primitive.WriteTypeCas && version.SupportsWriteTimeoutContentions() {
			var contentions uint16
			if contentions, err = primitive.ReadShort(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR WRITE TIMEOUT contentions: %w", err)
			}
			msg.Contentions = contentions
		}
		return msg, nil

	case primitive.ErrorCodeReadFailure:
		var msg = &ReadFailure{ErrorMessage: errorMsg}
		var consistency uint16
		if consistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE consistency: %w", err)
		}
		msg.Consistency = primitive.ConsistencyLevel(consistency)
		if msg.Received, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE received: %w", err)
		}
		if msg.BlockFor, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE block for: %w", err)
		}
		if version.SupportsReadWriteFailureReasonMap() {
			if msg.FailureReasons, err = primitive.ReadReasonMap(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR READ FAILURE reason map: %w", err)
			}
		} else {
			if msg.NumFailures, err = primitive.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR READ FAILURE num failures: %w", err)
			}
		}
		var b byte
		if b, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR READ FAILURE data present: %w", err)
		}
		if b > 0 {
			msg.DataPresent = true
		} else {
			msg.DataPresent = false
		}
		return msg, nil

	case primitive.ErrorCodeWriteFailure:
		var msg = &WriteFailure{ErrorMessage: errorMsg}
		var consistency uint16
		if consistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE consistency: %w", err)
		}
		msg.Consistency = primitive.ConsistencyLevel(consistency)
		if msg.Received, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE received: %w", err)
		}
		if msg.BlockFor, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE block for: %w", err)
		}
		if version.SupportsReadWriteFailureReasonMap() {
			if msg.FailureReasons, err = primitive.ReadReasonMap(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE reason map: %w", err)
			}
		} else {
			if msg.NumFailures, err = primitive.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE num failures: %w", err)
			}
		}
		var writeType string
		if writeType, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR WRITE FAILURE write type: %w", err)
		}
		msg.WriteType = primitive.WriteType(writeType)
		if err = primitive.CheckValidWriteType(msg.WriteType); err != nil {
			return nil, err
		}
		return msg, nil

	case primitive.ErrorCodeFunctionFailure:
		var msg = &FunctionFailure{ErrorMessage: errorMsg}
		if msg.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR FUNCTION FAILURE keyspace: %w", err)
		}
		if msg.Function, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR FUNCTION FAILURE function: %w", err)
		}
		if msg.Arguments, err = primitive.ReadStringList(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR FUNCTION FAILURE arguments: %w", err)
		}
		return msg, nil

	case primitive.ErrorCodeAlreadyExists:
		var msg = &AlreadyExists{ErrorMessage: errorMsg}
		if msg.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR ALREADY EXISTS keyspace: %w", err)
		}
		if msg.Table, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR ALREADY EXISTS table: %w", err)
		}
		return msg, nil

	case primitive.ErrorCodeUnprepared:
		var msg = &Unprepared{ErrorMessage: errorMsg}
		if msg.Id, err = primitive.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read ERROR UNPREPARED id: %w", err)
		}
		return msg, nil

	default:
		err = fmt.Errorf("unknown ERROR code: %v", code)

	}
	return msg, err
}

func (c *errorCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeError
}
