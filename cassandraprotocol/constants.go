package cassandraprotocol

type ProtocolVersion = uint8

const (
	ProtocolVersion3    = ProtocolVersion(0x3)
	ProtocolVersion4    = ProtocolVersion(0x4)
	ProtocolVersion5    = ProtocolVersion(0x5)
	ProtocolVersionMin  = ProtocolVersion3
	ProtocolVersionMax  = ProtocolVersion4
	ProtocolVersionBeta = ProtocolVersion5
)

type OpCode uint8

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

type ResultKind uint32

const (
	ResultKindVoid         = ResultKind(0x0001)
	ResultKindRows         = ResultKind(0x0002)
	ResultKindSetKeyspace  = ResultKind(0x0003)
	ResultKindPrepared     = ResultKind(0x0004)
	ResultKindSchemaChange = ResultKind(0x0005)
)

type ErrorCode uint32

const (
	ErrorCodeServerError          = ErrorCode(0x0000)
	ErrorCodeProtocolError        = ErrorCode(0x000A)
	ErrorCodeAuthenticationError  = ErrorCode(0x0100)
	ErrorCodeUnavailableException = ErrorCode(0x1000)
	ErrorCodeOverloaded           = ErrorCode(0x1001)
	ErrorCodeIsBootstrapping      = ErrorCode(0x1002)
	ErrorCodeTruncateError        = ErrorCode(0x1003)
	ErrorCodeWriteTimeout         = ErrorCode(0x1100)
	ErrorCodeReadTimeout          = ErrorCode(0x1200)
	ErrorCodeReadFailure          = ErrorCode(0x1300)
	ErrorCodeFunctionFailure      = ErrorCode(0x1400)
	ErrorCodeWriteFailure         = ErrorCode(0x1500)
	ErrorCodeSyntaxError          = ErrorCode(0x2000)
	ErrorCodeUnauthorized         = ErrorCode(0x2100)
	ErrorCodeInvalid              = ErrorCode(0x2200)
	ErrorCodeConfigError          = ErrorCode(0x2300)
	ErrorCodeAlreadyExists        = ErrorCode(0x2400)
	ErrorCodeUnprepared           = ErrorCode(0x2500)
)

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

type WriteType string

const (
	WriteTypeSimple        = WriteType("SIMPLE")
	WriteTypeBatch         = WriteType("BATCH")
	WriteTypeUnloggedBatch = WriteType("UNLOGGED_BATCH")
	WriteTypeCounter       = WriteType("COUNTER")
	WriteTypeBatchLog      = WriteType("BATCH_LOG")
	WriteTypeView          = WriteType("VIEW")
	WriteTypeCdc           = WriteType("CDC")
)

type DataType uint16

const (
	DataTypeCustom    = DataType(0x0000)
	DataTypeAscii     = DataType(0x0001)
	DataTypeBigint    = DataType(0x0002)
	DataTypeBlob      = DataType(0x0003)
	DataTypeBoolean   = DataType(0x0004)
	DataTypeCounter   = DataType(0x0005)
	DataTypeDecimal   = DataType(0x0006)
	DataTypeDouble    = DataType(0x0007)
	DataTypeFloat     = DataType(0x0008)
	DataTypeInt       = DataType(0x0009)
	DataTypeTimestamp = DataType(0x000B)
	DataTypeUuid      = DataType(0x000C)
	DataTypeVarchar   = DataType(0x000D)
	DataTypeVarint    = DataType(0x000E)
	DataTypeTimeuuid  = DataType(0x000F)
	DataTypeInet      = DataType(0x0010)
	DataTypeDate      = DataType(0x0011)
	DataTypeTime      = DataType(0x0012)
	DataTypeSmallint  = DataType(0x0013)
	DataTypeTinyint   = DataType(0x0014)
	DataTypeDuration  = DataType(0x0015) //v5
	DataTypeList      = DataType(0x0020)
	DataTypeMap       = DataType(0x0021)
	DataTypeSet       = DataType(0x0022)
	DataTypeUdt       = DataType(0x0030)
	DataTypeTuple     = DataType(0x0031)
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
	StatusChangeTypeUo   = StatusChangeType("UP")
	StatusChangeTypeDown = StatusChangeType("DOWN")
)

type BatchType uint8

const (
	BatchTypeLogged   = BatchType(0x00)
	BatchTypeUnlogged = BatchType(0x01)
	BatchTypeCounter  = BatchType(0x02)
)

type ByteFlag uint8

const (
	HeaderFlagCompressed    = ByteFlag(0x01)
	HeaderFlagTracing       = ByteFlag(0x02)
	HeaderFlagCustomPayload = ByteFlag(0x04)
	HeaderFlagWarning       = ByteFlag(0x08)
	HeaderFlagUseBeta       = ByteFlag(0x10)
)

func (f ByteFlag) contains(other ByteFlag) bool {
	return f&other == other
}

func (f ByteFlag) add(other ByteFlag) ByteFlag {
	return f | other
}

type IntFlag uint32

const (
	// query flags are uint8 in v3 and v4
	QueryFlagValues            = IntFlag(0x00000001)
	QueryFlagSkipMetadata      = IntFlag(0x00000002)
	QueryFlagPageSize          = IntFlag(0x00000004)
	QueryFlagPagingState       = IntFlag(0x00000008)
	QueryFlagSerialConsistency = IntFlag(0x00000010)
	QueryFlagDefaultTimestamp  = IntFlag(0x00000020)
	QueryFlagValueNames        = IntFlag(0x00000040)
	QueryFlagWithKeyspace      = IntFlag(0x00000080)
	QueryFlagNowInSeconds      = IntFlag(0x00000100)
)

const (
	RowsFlagGlobalTablesSpec = IntFlag(0x00000001)
	RowsFlagHasMorePages     = IntFlag(0x00000002)
	RowsFlagNoMetadata       = IntFlag(0x00000004)
	RowsFlagMetadataChanged  = IntFlag(0x00000008)
)

func (f IntFlag) contains(other IntFlag) bool {
	return f&other == other
}

func (f IntFlag) add(other IntFlag) IntFlag {
	return f | other
}
