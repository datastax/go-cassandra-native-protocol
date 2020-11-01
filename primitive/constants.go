package primitive

type ProtocolVersion = uint8

const (

	// Supported OSS versions
	ProtocolVersion3 = ProtocolVersion(0x3)
	ProtocolVersion4 = ProtocolVersion(0x4)
	ProtocolVersion5 = ProtocolVersion(0x5) // currently beta, should not be used in production

	// Supported DSE versions
	// Note: all DSE versions have the 7th bit set to 1
	ProtocolVersionDse1 = ProtocolVersion(0b_1_000001) // 1 + DSE bit = 65
	ProtocolVersionDse2 = ProtocolVersion(0b_1_000010) // 2 + DSE bit = 66
)

type OpCode = uint8

const (
	// requests
	OpCodeStartup      = OpCode(0x01)
	OpCodeOptions      = OpCode(0x05)
	OpCodeQuery        = OpCode(0x07)
	OpCodePrepare      = OpCode(0x09)
	OpCodeExecute      = OpCode(0x0A)
	OpCodeRegister     = OpCode(0x0B)
	OpCodeBatch        = OpCode(0x0D)
	OpCodeAuthResponse = OpCode(0x0F)
	OpCodeDseRevise    = OpCode(0xFF) // DSE v1
	// responses
	OpCodeError         = OpCode(0x00)
	OpCodeReady         = OpCode(0x02)
	OpCodeAuthenticate  = OpCode(0x03)
	OpCodeSupported     = OpCode(0x06)
	OpCodeResult        = OpCode(0x08)
	OpCodeEvent         = OpCode(0x0C)
	OpCodeAuthChallenge = OpCode(0x0E)
	OpCodeAuthSuccess   = OpCode(0x10)
)

type ResultType = int32

const (
	ResultTypeVoid         = ResultType(0x0001)
	ResultTypeRows         = ResultType(0x0002)
	ResultTypeSetKeyspace  = ResultType(0x0003)
	ResultTypePrepared     = ResultType(0x0004)
	ResultTypeSchemaChange = ResultType(0x0005)
)

type ErrorCode = int32

const (
	ErrorCodeServerError         = ErrorCode(0x0000)
	ErrorCodeProtocolError       = ErrorCode(0x000A)
	ErrorCodeAuthenticationError = ErrorCode(0x0100)
	ErrorCodeUnavailable         = ErrorCode(0x1000)
	ErrorCodeOverloaded          = ErrorCode(0x1001)
	ErrorCodeIsBootstrapping     = ErrorCode(0x1002)
	ErrorCodeTruncateError       = ErrorCode(0x1003)
	ErrorCodeWriteTimeout        = ErrorCode(0x1100)
	ErrorCodeReadTimeout         = ErrorCode(0x1200)
	ErrorCodeReadFailure         = ErrorCode(0x1300)
	ErrorCodeFunctionFailure     = ErrorCode(0x1400)
	ErrorCodeWriteFailure        = ErrorCode(0x1500)
	ErrorCodeSyntaxError         = ErrorCode(0x2000)
	ErrorCodeUnauthorized        = ErrorCode(0x2100)
	ErrorCodeInvalid             = ErrorCode(0x2200)
	ErrorCodeConfigError         = ErrorCode(0x2300)
	ErrorCodeAlreadyExists       = ErrorCode(0x2400)
	ErrorCodeUnprepared          = ErrorCode(0x2500)
)

// Corresponds to protocol section 3 [consistency]
type ConsistencyLevel = uint16

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

type WriteType = string

const (
	WriteTypeSimple        = WriteType("SIMPLE")
	WriteTypeBatch         = WriteType("BATCH")
	WriteTypeUnloggedBatch = WriteType("UNLOGGED_BATCH")
	WriteTypeCounter       = WriteType("COUNTER")
	WriteTypeBatchLog      = WriteType("BATCH_LOG")
	WriteTypeView          = WriteType("VIEW")
	WriteTypeCdc           = WriteType("CDC")
)

type DataTypeCode = uint16

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
	DataTypeCodeTimestamp = DataTypeCode(0x000B)
	DataTypeCodeUuid      = DataTypeCode(0x000C)
	DataTypeCodeVarchar   = DataTypeCode(0x000D)
	DataTypeCodeVarint    = DataTypeCode(0x000E)
	DataTypeCodeTimeuuid  = DataTypeCode(0x000F)
	DataTypeCodeInet      = DataTypeCode(0x0010)
	DataTypeCodeDate      = DataTypeCode(0x0011)
	DataTypeCodeTime      = DataTypeCode(0x0012)
	DataTypeCodeSmallint  = DataTypeCode(0x0013)
	DataTypeCodeTinyint   = DataTypeCode(0x0014)
	DataTypeCodeDuration  = DataTypeCode(0x0015) //v5, DSE v1 and DSE v2
	DataTypeCodeList      = DataTypeCode(0x0020)
	DataTypeCodeMap       = DataTypeCode(0x0021)
	DataTypeCodeSet       = DataTypeCode(0x0022)
	DataTypeCodeUdt       = DataTypeCode(0x0030)
	DataTypeCodeTuple     = DataTypeCode(0x0031)
)

type EventType = string

const (
	EventTypeTopologyChange = EventType("TOPOLOGY_CHANGE")
	EventTypeStatusChange   = EventType("STATUS_CHANGE")
	EventTypeSchemaChange   = EventType("SCHEMA_CHANGE")
)

type SchemaChangeType = string

const (
	SchemaChangeTypeCreated = SchemaChangeType("CREATED")
	SchemaChangeTypeUpdated = SchemaChangeType("UPDATED")
	SchemaChangeTypeDropped = SchemaChangeType("DROPPED")
)

type SchemaChangeTarget = string

const (
	SchemaChangeTargetKeyspace  = SchemaChangeTarget("KEYSPACE")
	SchemaChangeTargetTable     = SchemaChangeTarget("TABLE")
	SchemaChangeTargetType      = SchemaChangeTarget("TYPE")
	SchemaChangeTargetFunction  = SchemaChangeTarget("FUNCTION")
	SchemaChangeTargetAggregate = SchemaChangeTarget("AGGREGATE")
)

type TopologyChangeType = string

const (
	TopologyChangeTypeNewNode     = TopologyChangeType("NEW_NODE")
	TopologyChangeTypeRemovedNode = TopologyChangeType("REMOVED_NODE")
)

type StatusChangeType = string

const (
	StatusChangeTypeUp   = StatusChangeType("UP")
	StatusChangeTypeDown = StatusChangeType("DOWN")
)

type BatchType = uint8

const (
	BatchTypeLogged   = BatchType(0x00)
	BatchTypeUnlogged = BatchType(0x01)
	BatchTypeCounter  = BatchType(0x02)
)

type BatchChildStatementType = uint8

const (
	BatchChildTypeQueryString = BatchChildStatementType(0x00)
	BatchChildTypePreparedId  = BatchChildStatementType(0x01)
)

type HeaderFlag = uint8

const (
	HeaderFlagCompressed    = HeaderFlag(0x01)
	HeaderFlagTracing       = HeaderFlag(0x02)
	HeaderFlagCustomPayload = HeaderFlag(0x04)
	HeaderFlagWarning       = HeaderFlag(0x08)
	HeaderFlagUseBeta       = HeaderFlag(0x10)
)

// Note: QueryFlag was encoded as [byte] in v3 and v4, but changed to [int] in v5
type QueryFlag = uint32

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
	// DSE-specific flags
	QueryFlagDsePageSizeBytes               = QueryFlag(0x40000000) // DSE v1+
	QueryFlagDseWithContinuousPagingOptions = QueryFlag(0x80000000) // DSE v1+
)

type RowsFlag = uint32

const (
	RowsFlagGlobalTablesSpec = RowsFlag(0x00000001)
	RowsFlagHasMorePages     = RowsFlag(0x00000002)
	RowsFlagNoMetadata       = RowsFlag(0x00000004)
	RowsFlagMetadataChanged  = RowsFlag(0x00000008)
	// DSE-specific flags
	RowsFlagDseContinuousPaging   = RowsFlag(0x40000000) // DSE v1+
	RowsFlagDseLastContinuousPage = RowsFlag(0x80000000) // DSE v1+
)

type VariablesFlag = uint32

const (
	VariablesFlagGlobalTablesSpec = VariablesFlag(0x00000001)
)

type PrepareFlag = uint32

const (
	PrepareFlagWithKeyspace = PrepareFlag(0x00000001) // v5 and DSE v2
)

type DseRevisionType = int32

const (
	DseRevisionTypeCancelContinuousPaging = DseRevisionType(0x00000001)
	DseRevisionTypeMoreContinuousPages    = DseRevisionType(0x00000002) // DSE v2+
)

type FailureCode = uint16

const (
	FailureCodeUnknown               = FailureCode(0x0000)
	FailureCodeTooManyTombstonesRead = FailureCode(0x0001)
	FailureCodeIndexNotAvailable     = FailureCode(0x0002)
	FailureCodeCdcSpaceFull          = FailureCode(0x0003)
	FailureCodeCounterWrite          = FailureCode(0x0004)
	FailureCodeTableNotFound         = FailureCode(0x0005)
	FailureCodeKeyspaceNotFound      = FailureCode(0x0006)
)
