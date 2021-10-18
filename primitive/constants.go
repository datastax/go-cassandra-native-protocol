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

package primitive

import "fmt"

type ProtocolVersion uint8

// Supported OSS versions
const (
	ProtocolVersion2 = ProtocolVersion(0x2)
	ProtocolVersion3 = ProtocolVersion(0x3)
	ProtocolVersion4 = ProtocolVersion(0x4)
	ProtocolVersion5 = ProtocolVersion(0x5)
)

// Supported DSE versions
// Note: all DSE versions have the 7th bit set to 1
const (
	ProtocolVersionDse1 = ProtocolVersion(0b_1_000001) // 1 + DSE bit = 65
	ProtocolVersionDse2 = ProtocolVersion(0b_1_000010) // 2 + DSE bit = 66
)

func (v ProtocolVersion) IsSupported() bool {
	for _, supported := range SupportedProtocolVersions() {
		if v == supported {
			return true
		}
	}
	return false
}

func (v ProtocolVersion) IsOss() bool {
	switch v {
	case ProtocolVersion2:
	case ProtocolVersion3:
	case ProtocolVersion4:
	case ProtocolVersion5:
	default:
		return false
	}
	return true
}

func (v ProtocolVersion) IsDse() bool {
	switch v {
	case ProtocolVersionDse1:
	case ProtocolVersionDse2:
	default:
		return false
	}
	return true
}

func (v ProtocolVersion) IsBeta() bool {
	return false // no beta version supported currently
}

func (v ProtocolVersion) String() string {
	switch v {
	case ProtocolVersion2:
		return "ProtocolVersion OSS 2"
	case ProtocolVersion3:
		return "ProtocolVersion OSS 3"
	case ProtocolVersion4:
		return "ProtocolVersion OSS 4"
	case ProtocolVersion5:
		return "ProtocolVersion OSS 5"
	case ProtocolVersionDse1:
		return "ProtocolVersion DSE 1"
	case ProtocolVersionDse2:
		return "ProtocolVersion DSE 2"
	}
	return fmt.Sprintf("ProtocolVersion ? [%#.2X]", uint8(v))
}

func (v ProtocolVersion) Uses4BytesCollectionLength() bool {
	return v >= ProtocolVersion3
}

func (v ProtocolVersion) Uses4BytesQueryFlags() bool {
	return v >= ProtocolVersion5
}

func (v ProtocolVersion) SupportsCompression(compression Compression) bool {
	switch compression {
	case CompressionNone:
		return true
	case CompressionLz4:
		return true
	case CompressionSnappy:
		return v != ProtocolVersion5
	}
	return false // unknown compression
}

func (v ProtocolVersion) SupportsBatchQueryFlags() bool {
	return v >= ProtocolVersion3
}

func (v ProtocolVersion) SupportsPrepareFlags() bool {
	return v >= ProtocolVersion5 && v != ProtocolVersionDse1
}

func (v ProtocolVersion) SupportsQueryFlag(flag QueryFlag) bool {
	switch flag {
	case QueryFlagValues:
		return v >= ProtocolVersion2
	case QueryFlagSkipMetadata:
		return v >= ProtocolVersion2
	case QueryFlagPageSize:
		return v >= ProtocolVersion2
	case QueryFlagPagingState:
		return v >= ProtocolVersion2
	case QueryFlagSerialConsistency:
		return v >= ProtocolVersion2
	case QueryFlagDefaultTimestamp:
		return v >= ProtocolVersion3
	case QueryFlagValueNames:
		return v >= ProtocolVersion3
	case QueryFlagWithKeyspace:
		return v >= ProtocolVersion5 && v != ProtocolVersionDse1
	case QueryFlagNowInSeconds:
		return v >= ProtocolVersion5 && v != ProtocolVersionDse1 && v != ProtocolVersionDse2
	// DSE-specific flags
	case QueryFlagDsePageSizeBytes:
		return v.IsDse()
	case QueryFlagDseWithContinuousPagingOptions:
		return v.IsDse()
	}
	// Unknown flag
	return false
}

func (v ProtocolVersion) SupportsResultMetadataId() bool {
	return v >= ProtocolVersion5 && v != ProtocolVersionDse1
}

func (v ProtocolVersion) SupportsReadWriteFailureReasonMap() bool {
	return v >= ProtocolVersion5
}

func (v ProtocolVersion) SupportsWriteTimeoutContentions() bool {
	return v >= ProtocolVersion5 && v != ProtocolVersionDse1 && v != ProtocolVersionDse2
}

func (v ProtocolVersion) SupportsDataType(code DataTypeCode) bool {
	switch code {
	case DataTypeCodeCustom:
	case DataTypeCodeAscii:
	case DataTypeCodeBigint:
	case DataTypeCodeBlob:
	case DataTypeCodeBoolean:
	case DataTypeCodeCounter:
	case DataTypeCodeDecimal:
	case DataTypeCodeDouble:
	case DataTypeCodeFloat:
	case DataTypeCodeInt:
	case DataTypeCodeTimestamp:
	case DataTypeCodeUuid:
	case DataTypeCodeVarchar:
	case DataTypeCodeVarint:
	case DataTypeCodeTimeuuid:
	case DataTypeCodeInet:
	case DataTypeCodeList:
	case DataTypeCodeMap:
	case DataTypeCodeSet:
	case DataTypeCodeText:
		return v <= ProtocolVersion2 // removed in version 3
	case DataTypeCodeUdt:
		return v >= ProtocolVersion3
	case DataTypeCodeTuple:
		return v >= ProtocolVersion3
	case DataTypeCodeDate:
		return v >= ProtocolVersion4
	case DataTypeCodeTime:
		return v >= ProtocolVersion4
	case DataTypeCodeSmallint:
		return v >= ProtocolVersion4
	case DataTypeCodeTinyint:
		return v >= ProtocolVersion4
	case DataTypeCodeDuration:
		return v >= ProtocolVersion5
	default:
		// Unknown code
		return false
	}
	return true
}

func (v ProtocolVersion) SupportsSchemaChangeTarget(target SchemaChangeTarget) bool {
	switch target {
	case SchemaChangeTargetKeyspace:
		return true
	case SchemaChangeTargetTable:
		return true
	case SchemaChangeTargetType:
		return v >= ProtocolVersion3
	case SchemaChangeTargetFunction:
		return v >= ProtocolVersion4
	case SchemaChangeTargetAggregate:
		return v >= ProtocolVersion4
	}
	// Unknown target
	return false
}

func (v ProtocolVersion) SupportsTopologyChangeType(t TopologyChangeType) bool {
	switch t {
	case TopologyChangeTypeNewNode:
		return true
	case TopologyChangeTypeRemovedNode:
		return true
	case TopologyChangeTypeMovedNode:
		return v >= ProtocolVersion3
	}
	// Unknown type
	return false
}

func (v ProtocolVersion) SupportsDseRevisionType(t DseRevisionType) bool {
	switch t {
	case DseRevisionTypeCancelContinuousPaging:
		return v >= ProtocolVersionDse1
	case DseRevisionTypeMoreContinuousPages:
		return v >= ProtocolVersionDse2
	}
	// Unknown type
	return false
}

const (
	FrameHeaderLengthV3AndHigher = 9
	FrameHeaderLengthV2AndLower  = 8
)

func (v ProtocolVersion) FrameHeaderLengthInBytes() int {
	if v >= ProtocolVersion3 {
		return FrameHeaderLengthV3AndHigher
	} else {
		return FrameHeaderLengthV2AndLower
	}
}

func (v ProtocolVersion) SupportsModernFramingLayout() bool {
	return v >= ProtocolVersion5 && v != ProtocolVersionDse1 && v != ProtocolVersionDse2
}

func (v ProtocolVersion) SupportsUnsetValues() bool {
	return v >= ProtocolVersion4
}

type OpCode uint8

// requests
const (
	OpCodeStartup      = OpCode(0x01)
	OpCodeOptions      = OpCode(0x05)
	OpCodeQuery        = OpCode(0x07)
	OpCodePrepare      = OpCode(0x09)
	OpCodeExecute      = OpCode(0x0A)
	OpCodeRegister     = OpCode(0x0B)
	OpCodeBatch        = OpCode(0x0D)
	OpCodeAuthResponse = OpCode(0x0F)
	OpCodeDseRevise    = OpCode(0xFF) // DSE v1
)

// responses
const (
	OpCodeError         = OpCode(0x00)
	OpCodeReady         = OpCode(0x02)
	OpCodeAuthenticate  = OpCode(0x03)
	OpCodeSupported     = OpCode(0x06)
	OpCodeResult        = OpCode(0x08)
	OpCodeEvent         = OpCode(0x0C)
	OpCodeAuthChallenge = OpCode(0x0E)
	OpCodeAuthSuccess   = OpCode(0x10)
)

func (c OpCode) IsValid() bool {
	switch c {
	case OpCodeStartup:
	case OpCodeOptions:
	case OpCodeQuery:
	case OpCodePrepare:
	case OpCodeExecute:
	case OpCodeRegister:
	case OpCodeBatch:
	case OpCodeAuthResponse:
	case OpCodeDseRevise:
	case OpCodeError:
	case OpCodeReady:
	case OpCodeAuthenticate:
	case OpCodeSupported:
	case OpCodeResult:
	case OpCodeEvent:
	case OpCodeAuthChallenge:
	case OpCodeAuthSuccess:
	default:
		return false
	}
	return true
}

func (c OpCode) IsRequest() bool {
	switch c {
	case OpCodeStartup:
	case OpCodeOptions:
	case OpCodeQuery:
	case OpCodePrepare:
	case OpCodeExecute:
	case OpCodeRegister:
	case OpCodeBatch:
	case OpCodeAuthResponse:
	case OpCodeDseRevise:
	default:
		return false
	}
	return true
}

func (c OpCode) IsResponse() bool {
	switch c {
	case OpCodeError:
	case OpCodeReady:
	case OpCodeAuthenticate:
	case OpCodeSupported:
	case OpCodeResult:
	case OpCodeEvent:
	case OpCodeAuthChallenge:
	case OpCodeAuthSuccess:
	default:
		return false
	}
	return true
}

func (c OpCode) IsDse() bool {
	switch c {
	case OpCodeDseRevise:
	default:
		return false
	}
	return true
}

func (c OpCode) String() string {
	switch c {
	case OpCodeStartup:
		return "OpCode STARTUP [0x01]"
	case OpCodeOptions:
		return "OpCode OPTIONS [0x05]"
	case OpCodeQuery:
		return "OpCode QUERY [0x07]"
	case OpCodePrepare:
		return "OpCode PREPARE [0x09]"
	case OpCodeExecute:
		return "OpCode EXECUTE [0x0A]"
	case OpCodeRegister:
		return "OpCode REGISTER [0x0B]"
	case OpCodeBatch:
		return "OpCode BATCH [0x0D]"
	case OpCodeAuthResponse:
		return "OpCode AUTH RESPONSE [0x0F]"
	case OpCodeDseRevise:
		return "OpCode REVISE [0xFF]"
	// responses
	case OpCodeError:
		return "OpCode ERROR [0x00]"
	case OpCodeReady:
		return "OpCode READY [0x02]"
	case OpCodeAuthenticate:
		return "OpCode AUTHENTICATE [0x03]"
	case OpCodeSupported:
		return "OpCode SUPPORTED [0x06]"
	case OpCodeResult:
		return "OpCode RESULT [0x08]"
	case OpCodeEvent:
		return "OpCode EVENT [0x0C]"
	case OpCodeAuthChallenge:
		return "OpCode AUTH CHALLENGE [0x0E]"
	case OpCodeAuthSuccess:
		return "OpCode AUTH SUCCESS [0x10]"
	}
	return fmt.Sprintf("OpCode ? [%#.2X]", uint8(c))
}

type ResultType uint32

const (
	ResultTypeVoid         = ResultType(0x00000001)
	ResultTypeRows         = ResultType(0x00000002)
	ResultTypeSetKeyspace  = ResultType(0x00000003)
	ResultTypePrepared     = ResultType(0x00000004)
	ResultTypeSchemaChange = ResultType(0x00000005)
)

func (t ResultType) IsValid() bool {
	switch t {
	case ResultTypeVoid:
	case ResultTypeRows:
	case ResultTypeSetKeyspace:
	case ResultTypePrepared:
	case ResultTypeSchemaChange:
	default:
		return false
	}
	return true
}

func (t ResultType) String() string {
	switch t {
	case ResultTypeVoid:
		return "ResultType Void [0x00000001]"
	case ResultTypeRows:
		return "ResultType Rows [0x00000002]"
	case ResultTypeSetKeyspace:
		return "ResultType SetKeyspace [0x00000003]"
	case ResultTypePrepared:
		return "ResultType Prepared [0x00000004]"
	case ResultTypeSchemaChange:
		return "ResultType SchemaChange [0x00000005]"
	}
	return fmt.Sprintf("ResultType ? [%#.8X]", uint32(t))
}

type ErrorCode uint32

// 0xx: fatal errors
const (
	ErrorCodeServerError         = ErrorCode(0x00000000)
	ErrorCodeProtocolError       = ErrorCode(0x0000000A)
	ErrorCodeAuthenticationError = ErrorCode(0x00000100)
)

// 1xx: request execution
const (
	ErrorCodeUnavailable     = ErrorCode(0x00001000)
	ErrorCodeOverloaded      = ErrorCode(0x00001001)
	ErrorCodeIsBootstrapping = ErrorCode(0x00001002)
	ErrorCodeTruncateError   = ErrorCode(0x00001003)
	ErrorCodeWriteTimeout    = ErrorCode(0x00001100)
	ErrorCodeReadTimeout     = ErrorCode(0x00001200)
	ErrorCodeReadFailure     = ErrorCode(0x00001300)
	ErrorCodeFunctionFailure = ErrorCode(0x00001400)
	ErrorCodeWriteFailure    = ErrorCode(0x00001500)
)

// 2xx: query validation
const (
	ErrorCodeSyntaxError   = ErrorCode(0x00002000)
	ErrorCodeUnauthorized  = ErrorCode(0x00002100)
	ErrorCodeInvalid       = ErrorCode(0x00002200)
	ErrorCodeConfigError   = ErrorCode(0x00002300)
	ErrorCodeAlreadyExists = ErrorCode(0x00002400)
	ErrorCodeUnprepared    = ErrorCode(0x00002500)
)

func (c ErrorCode) IsValid() bool {
	switch c {
	case ErrorCodeServerError:
	case ErrorCodeProtocolError:
	case ErrorCodeAuthenticationError:
	case ErrorCodeUnavailable:
	case ErrorCodeOverloaded:
	case ErrorCodeIsBootstrapping:
	case ErrorCodeTruncateError:
	case ErrorCodeWriteTimeout:
	case ErrorCodeReadTimeout:
	case ErrorCodeReadFailure:
	case ErrorCodeFunctionFailure:
	case ErrorCodeWriteFailure:
	case ErrorCodeSyntaxError:
	case ErrorCodeUnauthorized:
	case ErrorCodeInvalid:
	case ErrorCodeConfigError:
	case ErrorCodeAlreadyExists:
	case ErrorCodeUnprepared:
	default:
		return false
	}
	return true
}

func (c ErrorCode) IsFatalError() bool {
	switch c {
	case ErrorCodeServerError:
	case ErrorCodeProtocolError:
	case ErrorCodeAuthenticationError:
	default:
		return false
	}
	return true
}

func (c ErrorCode) IsRequestExecutionError() bool {
	switch c {
	case ErrorCodeUnavailable:
	case ErrorCodeOverloaded:
	case ErrorCodeIsBootstrapping:
	case ErrorCodeTruncateError:
	case ErrorCodeWriteTimeout:
	case ErrorCodeReadTimeout:
	case ErrorCodeReadFailure:
	case ErrorCodeFunctionFailure:
	case ErrorCodeWriteFailure:
	default:
		return false
	}
	return true
}

func (c ErrorCode) IsQueryValidationError() bool {
	switch c {
	case ErrorCodeSyntaxError:
	case ErrorCodeUnauthorized:
	case ErrorCodeInvalid:
	case ErrorCodeConfigError:
	case ErrorCodeAlreadyExists:
	case ErrorCodeUnprepared:
	default:
		return false
	}
	return true
}

func (c ErrorCode) String() string {
	switch c {
	case ErrorCodeServerError:
		return "ErrorCode ServerError [0x00000000]"
	case ErrorCodeProtocolError:
		return "ErrorCode ProtocolError [0x0000000A]"
	case ErrorCodeAuthenticationError:
		return "ErrorCode AuthenticationError [0x00000100]"
	case ErrorCodeUnavailable:
		return "ErrorCode Unavailable [0x00001000]"
	case ErrorCodeOverloaded:
		return "ErrorCode Overloaded [0x00001001]"
	case ErrorCodeIsBootstrapping:
		return "ErrorCode IsBootstrapping [0x00001002]"
	case ErrorCodeTruncateError:
		return "ErrorCode TruncateError [0x00001003]"
	case ErrorCodeWriteTimeout:
		return "ErrorCode WriteTimeout [0x00001100]"
	case ErrorCodeReadTimeout:
		return "ErrorCode ReadTimeout [0x00001200]"
	case ErrorCodeReadFailure:
		return "ErrorCode ReadFailure [0x00001300]"
	case ErrorCodeFunctionFailure:
		return "ErrorCode FunctionFailure [0x00001400]"
	case ErrorCodeWriteFailure:
		return "ErrorCode WriteFailure [0x00001500]"
	case ErrorCodeSyntaxError:
		return "ErrorCode SyntaxError [0x00002000]"
	case ErrorCodeUnauthorized:
		return "ErrorCode Unauthorized [0x00002100]"
	case ErrorCodeInvalid:
		return "ErrorCode Invalid [0x00002200]"
	case ErrorCodeConfigError:
		return "ErrorCode ConfigError [0x00002300]"
	case ErrorCodeAlreadyExists:
		return "ErrorCode AlreadyExists [0x00002400]"
	case ErrorCodeUnprepared:
		return "ErrorCode Unprepared [0x00002500]"
	}
	return fmt.Sprintf("ErrorCode ? [%#.8X]", uint32(c))
}

// ConsistencyLevel corresponds to protocol section 3 [consistency] data type.
type ConsistencyLevel uint16

const (
	ConsistencyLevelAny         = ConsistencyLevel(0x0000)
	ConsistencyLevelOne         = ConsistencyLevel(0x0001)
	ConsistencyLevelTwo         = ConsistencyLevel(0x0002)
	ConsistencyLevelThree       = ConsistencyLevel(0x0003)
	ConsistencyLevelQuorum      = ConsistencyLevel(0x0004)
	ConsistencyLevelAll         = ConsistencyLevel(0x0005)
	ConsistencyLevelLocalQuorum = ConsistencyLevel(0x0006)
	ConsistencyLevelEachQuorum  = ConsistencyLevel(0x0007)
	ConsistencyLevelSerial      = ConsistencyLevel(0x0008)
	ConsistencyLevelLocalSerial = ConsistencyLevel(0x0009)
	ConsistencyLevelLocalOne    = ConsistencyLevel(0x000A)
)

func (c ConsistencyLevel) IsValid() bool {
	switch c {
	case ConsistencyLevelAny:
	case ConsistencyLevelOne:
	case ConsistencyLevelTwo:
	case ConsistencyLevelThree:
	case ConsistencyLevelQuorum:
	case ConsistencyLevelAll:
	case ConsistencyLevelLocalQuorum:
	case ConsistencyLevelEachQuorum:
	case ConsistencyLevelSerial:
	case ConsistencyLevelLocalSerial:
	case ConsistencyLevelLocalOne:
	default:
		return false
	}
	return true
}

func (c ConsistencyLevel) IsSerial() bool {
	switch c {
	case ConsistencyLevelSerial:
	case ConsistencyLevelLocalSerial:
	default:
		return false
	}
	return true
}

func (c ConsistencyLevel) IsNonSerial() bool {
	switch c {
	case ConsistencyLevelAny:
	case ConsistencyLevelOne:
	case ConsistencyLevelTwo:
	case ConsistencyLevelThree:
	case ConsistencyLevelQuorum:
	case ConsistencyLevelAll:
	case ConsistencyLevelLocalQuorum:
	case ConsistencyLevelEachQuorum:
	case ConsistencyLevelLocalOne:
	default:
		return false
	}
	return true
}

func (c ConsistencyLevel) IsLocal() bool {
	switch c {
	case ConsistencyLevelLocalQuorum:
	case ConsistencyLevelLocalSerial:
	case ConsistencyLevelLocalOne:
	default:
		return false
	}
	return true
}

func (c ConsistencyLevel) IsNonLocal() bool {
	switch c {
	case ConsistencyLevelAny:
	case ConsistencyLevelOne:
	case ConsistencyLevelTwo:
	case ConsistencyLevelThree:
	case ConsistencyLevelQuorum:
	case ConsistencyLevelAll:
	case ConsistencyLevelEachQuorum:
	case ConsistencyLevelSerial:
	default:
		return false
	}
	return true
}

func (c ConsistencyLevel) String() string {
	switch c {
	case ConsistencyLevelAny:
		return "ConsistencyLevel ANY [0x0000]"
	case ConsistencyLevelOne:
		return "ConsistencyLevel ONE [0x0001]"
	case ConsistencyLevelTwo:
		return "ConsistencyLevel TWO [0x0002]"
	case ConsistencyLevelThree:
		return "ConsistencyLevel THREE [0x0003]"
	case ConsistencyLevelQuorum:
		return "ConsistencyLevel QUORUM [0x0004]"
	case ConsistencyLevelAll:
		return "ConsistencyLevel ALL [0x0005]"
	case ConsistencyLevelLocalQuorum:
		return "ConsistencyLevel LOCAL_QUORUM [0x0006]"
	case ConsistencyLevelEachQuorum:
		return "ConsistencyLevel EACH_QUORUM [0x0007]"
	case ConsistencyLevelSerial:
		return "ConsistencyLevel SERIAL [0x0008]"
	case ConsistencyLevelLocalSerial:
		return "ConsistencyLevel LOCAL_SERIAL [0x0009]"
	case ConsistencyLevelLocalOne:
		return "ConsistencyLevel LOCAL_ONE [0x000A]"
	}
	return fmt.Sprintf("ConsistencyLevel ? [%#.4X]", uint16(c))
}

type WriteType string

const (
	WriteTypeSimple        = WriteType("SIMPLE")
	WriteTypeBatch         = WriteType("BATCH")
	WriteTypeUnloggedBatch = WriteType("UNLOGGED_BATCH")
	WriteTypeCounter       = WriteType("COUNTER")
	WriteTypeBatchLog      = WriteType("BATCH_LOG")
	WriteTypeCas           = WriteType("CAS")
	WriteTypeView          = WriteType("VIEW")
	WriteTypeCdc           = WriteType("CDC")
)

func (t WriteType) IsValid() bool {
	switch t {
	case WriteTypeSimple:
	case WriteTypeBatch:
	case WriteTypeUnloggedBatch:
	case WriteTypeCounter:
	case WriteTypeBatchLog:
	case WriteTypeView:
	case WriteTypeCdc:
	default:
		return false
	}
	return true
}

type DataTypeCode uint16

const (
	DataTypeCodeCustom    = DataTypeCode(0x0000)
	DataTypeCodeAscii     = DataTypeCode(0x0001)
	DataTypeCodeBigint    = DataTypeCode(0x0002)
	DataTypeCodeBlob      = DataTypeCode(0x0003)
	DataTypeCodeBoolean   = DataTypeCode(0x0004)
	DataTypeCodeCounter   = DataTypeCode(0x0005)
	DataTypeCodeDecimal   = DataTypeCode(0x0006)
	DataTypeCodeDouble    = DataTypeCode(0x0007)
	DataTypeCodeFloat     = DataTypeCode(0x0008)
	DataTypeCodeInt       = DataTypeCode(0x0009)
	DataTypeCodeText      = DataTypeCode(0x000A) // removed in v3, alias for DataTypeCodeVarchar
	DataTypeCodeTimestamp = DataTypeCode(0x000B)
	DataTypeCodeUuid      = DataTypeCode(0x000C)
	DataTypeCodeVarchar   = DataTypeCode(0x000D)
	DataTypeCodeVarint    = DataTypeCode(0x000E)
	DataTypeCodeTimeuuid  = DataTypeCode(0x000F)
	DataTypeCodeInet      = DataTypeCode(0x0010)
	DataTypeCodeDate      = DataTypeCode(0x0011) // v4+
	DataTypeCodeTime      = DataTypeCode(0x0012) // v4+
	DataTypeCodeSmallint  = DataTypeCode(0x0013) // v4+
	DataTypeCodeTinyint   = DataTypeCode(0x0014) // v4+
	DataTypeCodeDuration  = DataTypeCode(0x0015) // v5, DSE v1 and DSE v2
	DataTypeCodeList      = DataTypeCode(0x0020)
	DataTypeCodeMap       = DataTypeCode(0x0021)
	DataTypeCodeSet       = DataTypeCode(0x0022)
	DataTypeCodeUdt       = DataTypeCode(0x0030) // v3+
	DataTypeCodeTuple     = DataTypeCode(0x0031) // v3+
)

func (c DataTypeCode) IsValid() bool {
	switch c {
	case DataTypeCodeCustom:
	case DataTypeCodeAscii:
	case DataTypeCodeBigint:
	case DataTypeCodeBlob:
	case DataTypeCodeBoolean:
	case DataTypeCodeCounter:
	case DataTypeCodeDecimal:
	case DataTypeCodeDouble:
	case DataTypeCodeFloat:
	case DataTypeCodeInt:
	case DataTypeCodeText:
	case DataTypeCodeTimestamp:
	case DataTypeCodeUuid:
	case DataTypeCodeVarchar:
	case DataTypeCodeVarint:
	case DataTypeCodeTimeuuid:
	case DataTypeCodeInet:
	case DataTypeCodeDate:
	case DataTypeCodeTime:
	case DataTypeCodeSmallint:
	case DataTypeCodeTinyint:
	case DataTypeCodeDuration:
	case DataTypeCodeList:
	case DataTypeCodeMap:
	case DataTypeCodeSet:
	case DataTypeCodeUdt:
	case DataTypeCodeTuple:
	default:
		return false
	}
	return true
}

func (c DataTypeCode) IsPrimitive() bool {
	switch c {
	case DataTypeCodeCustom:
	case DataTypeCodeAscii:
	case DataTypeCodeBigint:
	case DataTypeCodeBlob:
	case DataTypeCodeBoolean:
	case DataTypeCodeCounter:
	case DataTypeCodeDecimal:
	case DataTypeCodeDouble:
	case DataTypeCodeFloat:
	case DataTypeCodeInt:
	case DataTypeCodeText:
	case DataTypeCodeTimestamp:
	case DataTypeCodeUuid:
	case DataTypeCodeVarchar:
	case DataTypeCodeVarint:
	case DataTypeCodeTimeuuid:
	case DataTypeCodeInet:
	case DataTypeCodeDate:
	case DataTypeCodeTime:
	case DataTypeCodeSmallint:
	case DataTypeCodeTinyint:
	case DataTypeCodeDuration:
	default:
		return false
	}
	return true
}

func (c DataTypeCode) String() string {
	switch c {
	case DataTypeCodeCustom:
		return "DataTypeCode Custom [0x0000]"
	case DataTypeCodeAscii:
		return "DataTypeCode Ascii [0x0001]"
	case DataTypeCodeBigint:
		return "DataTypeCode Bigint [0x0002]"
	case DataTypeCodeBlob:
		return "DataTypeCode Blob [0x0003]"
	case DataTypeCodeBoolean:
		return "DataTypeCode Boolean [0x0004]"
	case DataTypeCodeCounter:
		return "DataTypeCode Counter [0x0005]"
	case DataTypeCodeDecimal:
		return "DataTypeCode Decimal [0x0006]"
	case DataTypeCodeDouble:
		return "DataTypeCode Double [0x0007]"
	case DataTypeCodeFloat:
		return "DataTypeCode Float [0x0008]"
	case DataTypeCodeInt:
		return "DataTypeCode Int [0x0009]"
	case DataTypeCodeText:
		return "DataTypeCode Text [0x000A]"
	case DataTypeCodeTimestamp:
		return "DataTypeCode Timestamp [0x000B]"
	case DataTypeCodeUuid:
		return "DataTypeCode Uuid [0x000C]"
	case DataTypeCodeVarchar:
		return "DataTypeCode Varchar [0x000D]"
	case DataTypeCodeVarint:
		return "DataTypeCode Varint [0x000E]"
	case DataTypeCodeTimeuuid:
		return "DataTypeCode Timeuuid [0x000F]"
	case DataTypeCodeInet:
		return "DataTypeCode Inet [0x0010]"
	case DataTypeCodeDate:
		return "DataTypeCode Date [0x0011]"
	case DataTypeCodeTime:
		return "DataTypeCode Time [0x0012]"
	case DataTypeCodeSmallint:
		return "DataTypeCode Smallint [0x0013]"
	case DataTypeCodeTinyint:
		return "DataTypeCode Tinyint [0x0014]"
	case DataTypeCodeDuration:
		return "DataTypeCode Duration [0x0015]"
	case DataTypeCodeList:
		return "DataTypeCode List [0x0020]"
	case DataTypeCodeMap:
		return "DataTypeCode Map [0x0021]"
	case DataTypeCodeSet:
		return "DataTypeCode Set [0x0022]"
	case DataTypeCodeUdt:
		return "DataTypeCode Udt [0x0030]"
	case DataTypeCodeTuple:
		return "DataTypeCode Tuple [0x0031]"
	}
	return fmt.Sprintf("DataType ? [%#.4X]", uint16(c))
}

type EventType string

const (
	EventTypeTopologyChange = EventType("TOPOLOGY_CHANGE")
	EventTypeStatusChange   = EventType("STATUS_CHANGE")
	EventTypeSchemaChange   = EventType("SCHEMA_CHANGE")
)

func (e EventType) IsValid() bool {
	switch e {
	case EventTypeSchemaChange:
	case EventTypeTopologyChange:
	case EventTypeStatusChange:
	default:
		return false
	}
	return true
}

type SchemaChangeType string

const (
	SchemaChangeTypeCreated = SchemaChangeType("CREATED")
	SchemaChangeTypeUpdated = SchemaChangeType("UPDATED")
	SchemaChangeTypeDropped = SchemaChangeType("DROPPED")
)

func (t SchemaChangeType) IsValid() bool {
	switch t {
	case SchemaChangeTypeCreated:
	case SchemaChangeTypeUpdated:
	case SchemaChangeTypeDropped:
	default:
		return false
	}
	return true
}

type SchemaChangeTarget string

const (
	SchemaChangeTargetKeyspace  = SchemaChangeTarget("KEYSPACE")
	SchemaChangeTargetTable     = SchemaChangeTarget("TABLE")
	SchemaChangeTargetType      = SchemaChangeTarget("TYPE")      // v3+
	SchemaChangeTargetFunction  = SchemaChangeTarget("FUNCTION")  // v3+
	SchemaChangeTargetAggregate = SchemaChangeTarget("AGGREGATE") // v3+
)

func (t SchemaChangeTarget) IsValid() bool {
	switch t {
	case SchemaChangeTargetKeyspace:
	case SchemaChangeTargetTable:
	case SchemaChangeTargetType:
	case SchemaChangeTargetFunction:
	case SchemaChangeTargetAggregate:
	default:
		return false
	}
	return true
}

type TopologyChangeType string

const (
	TopologyChangeTypeNewNode     = TopologyChangeType("NEW_NODE")
	TopologyChangeTypeRemovedNode = TopologyChangeType("REMOVED_NODE")
	TopologyChangeTypeMovedNode   = TopologyChangeType("MOVED_NODE") // v3+
)

func (t TopologyChangeType) IsValid() bool {
	switch t {
	case TopologyChangeTypeNewNode:
	case TopologyChangeTypeRemovedNode:
	case TopologyChangeTypeMovedNode:
	default:
		return false
	}
	return true
}

type StatusChangeType string

const (
	StatusChangeTypeUp   = StatusChangeType("UP")
	StatusChangeTypeDown = StatusChangeType("DOWN")
)

func (t StatusChangeType) IsValid() bool {
	switch t {
	case StatusChangeTypeUp:
	case StatusChangeTypeDown:
	default:
		return false
	}
	return true
}

type BatchType uint8

const (
	BatchTypeLogged   = BatchType(0x00)
	BatchTypeUnlogged = BatchType(0x01)
	BatchTypeCounter  = BatchType(0x02)
)

func (t BatchType) IsValid() bool {
	switch t {
	case BatchTypeLogged:
	case BatchTypeUnlogged:
	case BatchTypeCounter:
	default:
		return false
	}
	return true
}

func (t BatchType) String() string {
	switch t {
	case BatchTypeLogged:
		return "BatchType LOGGED [0x00]"
	case BatchTypeUnlogged:
		return "BatchType UNLOGGED [0x01]"
	case BatchTypeCounter:
		return "BatchType COUNTER [0x02]"
	}
	return fmt.Sprintf("BatchType ? [%#.2X]", uint8(t))
}

type BatchChildType uint8

const (
	BatchChildTypeQueryString = BatchChildType(0x00)
	BatchChildTypePreparedId  = BatchChildType(0x01)
)

func (t BatchChildType) IsValid() bool {
	switch t {
	case BatchChildTypeQueryString:
	case BatchChildTypePreparedId:
	default:
		return false
	}
	return true
}

func (t BatchChildType) String() string {
	switch t {
	case BatchChildTypeQueryString:
		return "BatchChildType QueryString [0x00]"
	case BatchChildTypePreparedId:
		return "BatchChildType PreparedId [0x01]"
	}
	return fmt.Sprintf("BatchChildType ? [%#.2X]", uint8(t))
}

type HeaderFlag uint8

const (
	HeaderFlagCompressed    = HeaderFlag(0x01)
	HeaderFlagTracing       = HeaderFlag(0x02)
	HeaderFlagCustomPayload = HeaderFlag(0x04)
	HeaderFlagWarning       = HeaderFlag(0x08)
	HeaderFlagUseBeta       = HeaderFlag(0x10)
)

func (f HeaderFlag) Add(other HeaderFlag) HeaderFlag {
	return f | other
}

func (f HeaderFlag) Remove(other HeaderFlag) HeaderFlag {
	return f &^ other
}

func (f HeaderFlag) Contains(other HeaderFlag) bool {
	return f&other != 0
}

func (f HeaderFlag) String() string {
	switch f {
	case HeaderFlagCompressed:
		return fmt.Sprintf("HeaderFlag Compressed [0x01 %#.8b]", f)
	case HeaderFlagTracing:
		return fmt.Sprintf("HeaderFlag Tracing [0x02 %#.8b]", f)
	case HeaderFlagCustomPayload:
		return fmt.Sprintf("HeaderFlag CustomPayload [0x04 %#.8b]", f)
	case HeaderFlagWarning:
		return fmt.Sprintf("HeaderFlag Warning [0x08 %#.8b]", f)
	case HeaderFlagUseBeta:
		return fmt.Sprintf("HeaderFlag UseBeta [0x10 %#.8b]", f)
	}
	return fmt.Sprintf("HeaderFlag ? [%#.2X %#.8b]", uint8(f), uint8(f))
}

// QueryFlag was encoded as [byte] in v3 and v4, but changed to [int] in v5.
type QueryFlag uint32

const (
	QueryFlagValues            = QueryFlag(0x00000001)
	QueryFlagSkipMetadata      = QueryFlag(0x00000002)
	QueryFlagPageSize          = QueryFlag(0x00000004)
	QueryFlagPagingState       = QueryFlag(0x00000008)
	QueryFlagSerialConsistency = QueryFlag(0x00000010)
	QueryFlagDefaultTimestamp  = QueryFlag(0x00000020)
	QueryFlagValueNames        = QueryFlag(0x00000040)
	QueryFlagWithKeyspace      = QueryFlag(0x00000080) // protocol v5+ and DSE v2
	QueryFlagNowInSeconds      = QueryFlag(0x00000100) // protocol v5+
)

// DSE-specific query flags
const (
	QueryFlagDsePageSizeBytes               = QueryFlag(0x40000000) // DSE v1+
	QueryFlagDseWithContinuousPagingOptions = QueryFlag(0x80000000) // DSE v1+
)

func (f QueryFlag) Add(other QueryFlag) QueryFlag {
	return f | other
}

func (f QueryFlag) Remove(other QueryFlag) QueryFlag {
	return f &^ other
}

func (f QueryFlag) Contains(other QueryFlag) bool {
	return f&other != 0
}

func (f QueryFlag) String() string {
	switch f {
	case QueryFlagValues:
		return fmt.Sprintf("QueryFlag Values [0x00000001 %#.32b]", f)
	case QueryFlagSkipMetadata:
		return fmt.Sprintf("QueryFlag SkipMetadata [0x00000002 %#.32b]", f)
	case QueryFlagPageSize:
		return fmt.Sprintf("QueryFlag PageSize [0x00000004 %#.32b]", f)
	case QueryFlagPagingState:
		return fmt.Sprintf("QueryFlag PagingState [0x00000008 %#.32b]", f)
	case QueryFlagSerialConsistency:
		return fmt.Sprintf("QueryFlag SerialConsistency [0x00000010 %#.32b]", f)
	case QueryFlagDefaultTimestamp:
		return fmt.Sprintf("QueryFlag DefaultTimestamp [0x00000020 %#.32b]", f)
	case QueryFlagValueNames:
		return fmt.Sprintf("QueryFlag ValueNames [0x00000040 %#.32b]", f)
	case QueryFlagWithKeyspace:
		return fmt.Sprintf("QueryFlag WithKeyspace [0x00000080 %#.32b]", f)
	case QueryFlagNowInSeconds:
		return fmt.Sprintf("QueryFlag NowInSeconds [0x00000100 %#.32b]", f)
	case QueryFlagDsePageSizeBytes:
		return fmt.Sprintf("QueryFlag DsePageSizeBytes [0x40000000 %#.32b]", f)
	case QueryFlagDseWithContinuousPagingOptions:
		return fmt.Sprintf("QueryFlag DseWithContinuousPagingOptions [0x80000000 %#.32b]", f)
	}
	return fmt.Sprintf("QueryFlag ? [%#.8X %#.32b]", uint32(f), uint32(f))
}

type RowsFlag uint32

const (
	RowsFlagGlobalTablesSpec = RowsFlag(0x00000001)
	RowsFlagHasMorePages     = RowsFlag(0x00000002)
	RowsFlagNoMetadata       = RowsFlag(0x00000004)
	RowsFlagMetadataChanged  = RowsFlag(0x00000008)
)

// DSE-specific rows flags
const (
	RowsFlagDseContinuousPaging   = RowsFlag(0x40000000) // DSE v1+
	RowsFlagDseLastContinuousPage = RowsFlag(0x80000000) // DSE v1+
)

func (f RowsFlag) Add(other RowsFlag) RowsFlag {
	return f | other
}

func (f RowsFlag) Remove(other RowsFlag) RowsFlag {
	return f &^ other
}

func (f RowsFlag) Contains(other RowsFlag) bool {
	return f&other != 0
}

func (f RowsFlag) String() string {
	switch f {
	case RowsFlagGlobalTablesSpec:
		return fmt.Sprintf("RowsFlag GlobalTablesSpec [0x00000001 %#.32b]", f)
	case RowsFlagHasMorePages:
		return fmt.Sprintf("RowsFlag HasMorePages [0x00000002 %#.32b]", f)
	case RowsFlagNoMetadata:
		return fmt.Sprintf("RowsFlag NoMetadata [0x00000004 %#.32b]", f)
	case RowsFlagMetadataChanged:
		return fmt.Sprintf("RowsFlag MetadataChanged [0x00000008 %#.32b]", f)
	// DSE-specific flags
	case RowsFlagDseContinuousPaging:
		return fmt.Sprintf("RowsFlag ContinuousPaging [0x40000000 %#.32b]", f)
	case RowsFlagDseLastContinuousPage:
		return fmt.Sprintf("RowsFlag LastContinuousPage [0x80000000 %#.32b]", f)
	}
	return fmt.Sprintf("RowsFlag ? [%#.8X %#.32b]", uint32(f), uint32(f))
}

type VariablesFlag uint32

const (
	VariablesFlagGlobalTablesSpec = VariablesFlag(0x00000001)
)

func (f VariablesFlag) Add(other VariablesFlag) VariablesFlag {
	return f | other
}

func (f VariablesFlag) Remove(other VariablesFlag) VariablesFlag {
	return f &^ other
}

func (f VariablesFlag) Contains(other VariablesFlag) bool {
	return f&other != 0
}

func (f VariablesFlag) String() string {
	switch f {
	case VariablesFlagGlobalTablesSpec:
		return fmt.Sprintf("VariablesFlag GlobalTablesSpec [0x00000001 %#.32b]", f)
	}
	return fmt.Sprintf("VariablesFlag ? [%#.8X %#.32b]", uint32(f), uint32(f))
}

type PrepareFlag uint32

const (
	PrepareFlagWithKeyspace = PrepareFlag(0x00000001) // v5 and DSE v2
)

func (f PrepareFlag) Add(other PrepareFlag) PrepareFlag {
	return f | other
}

func (f PrepareFlag) Remove(other PrepareFlag) PrepareFlag {
	return f &^ other
}

func (f PrepareFlag) Contains(other PrepareFlag) bool {
	return f&other != 0
}

func (f PrepareFlag) String() string {
	switch f {
	case PrepareFlagWithKeyspace:
		return fmt.Sprintf("PrepareFlag WithKeyspace [0x00000001 %#.32b]", f)
	}
	return fmt.Sprintf("PrepareFlag ? [%#.8X %#.32b]", uint32(f), uint32(f))
}

type DseRevisionType uint32

const (
	DseRevisionTypeCancelContinuousPaging = DseRevisionType(0x00000001)
	DseRevisionTypeMoreContinuousPages    = DseRevisionType(0x00000002) // DSE v2+
)

func (t DseRevisionType) IsValid() bool {
	switch t {
	case DseRevisionTypeCancelContinuousPaging:
	case DseRevisionTypeMoreContinuousPages:
	default:
		return false
	}
	return true
}

func (t DseRevisionType) String() string {
	switch t {
	case DseRevisionTypeCancelContinuousPaging:
		return "DseRevisionType CancelContinuousPaging [0x00000001]"
	case DseRevisionTypeMoreContinuousPages:
		return "DseRevisionType MoreContinuousPages [0x00000002]"
	}
	return fmt.Sprintf("DseRevisionType ? [%#.8X]", uint32(t))
}

type FailureCode uint16

const (
	FailureCodeUnknown               = FailureCode(0x0000)
	FailureCodeTooManyTombstonesRead = FailureCode(0x0001)
	FailureCodeIndexNotAvailable     = FailureCode(0x0002)
	FailureCodeCdcSpaceFull          = FailureCode(0x0003)
	FailureCodeCounterWrite          = FailureCode(0x0004)
	FailureCodeTableNotFound         = FailureCode(0x0005)
	FailureCodeKeyspaceNotFound      = FailureCode(0x0006)
)

func (c FailureCode) IsValid() bool {
	switch c {
	case FailureCodeUnknown:
	case FailureCodeTooManyTombstonesRead:
	case FailureCodeIndexNotAvailable:
	case FailureCodeCdcSpaceFull:
	case FailureCodeCounterWrite:
	case FailureCodeTableNotFound:
	case FailureCodeKeyspaceNotFound:
	default:
		return false
	}
	return true
}

func (c FailureCode) String() string {
	switch c {
	case FailureCodeUnknown:
		return "FailureCode Unknown [0x0000]"
	case FailureCodeTooManyTombstonesRead:
		return "FailureCode TooManyTombstonesRead [0x0001]"
	case FailureCodeIndexNotAvailable:
		return "FailureCode IndexNotAvailable [0x0002]"
	case FailureCodeCdcSpaceFull:
		return "FailureCode CdcSpaceFull [0x0003]"
	case FailureCodeCounterWrite:
		return "FailureCode CounterWrite [0x0004]"
	case FailureCodeTableNotFound:
		return "FailureCode TableNotFound [0x0005]"
	case FailureCodeKeyspaceNotFound:
		return "FailureCode KeyspaceNotFound [0x0006]"
	}
	return fmt.Sprintf("FailureCode ? [%#.4X]", uint16(c))
}

type Compression string

const (
	CompressionNone   Compression = "NONE"
	CompressionLz4    Compression = "LZ4"
	CompressionSnappy Compression = "SNAPPY"
)

func (c Compression) IsValid() bool {
	switch c {
	case CompressionNone:
	case CompressionLz4:
	case CompressionSnappy:
	default:
		return false
	}
	return true
}
