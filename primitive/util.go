package primitive

import (
	"fmt"
)

func AllProtocolVersions() []ProtocolVersion {
	return []ProtocolVersion{
		ProtocolVersion3,
		ProtocolVersion4,
		ProtocolVersion5,
		ProtocolVersionDse1,
		ProtocolVersionDse2,
	}
}

func AllOssProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v.IsOss() })
}

func AllDseProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v.IsDse() })
}

func AllBetaProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v.IsBeta() })
}

func AllNonBetaProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return !v.IsBeta() })
}

func AllProtocolVersionsGreaterThanOrEqualTo(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v >= version })
}

func AllProtocolVersionsGreaterThan(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v > version })
}

func AllProtocolVersionsLesserThanOrEqualTo(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v <= version })
}

func AllProtocolVersionsLesserThan(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v < version })
}

func matchingProtocolVersions(filters ...func(version ProtocolVersion) bool) []ProtocolVersion {
	var result []ProtocolVersion
	for _, v := range AllProtocolVersions() {
		include := true
		for _, filter := range filters {
			if !filter(v) {
				include = false
				break
			}
		}
		if include {
			result = append(result, v)
		}
	}
	return result
}

func CheckValidProtocolVersion(version ProtocolVersion) error {
	for _, v := range AllProtocolVersions() {
		if v == version {
			return nil
		}
	}
	return fmt.Errorf("invalid protocol version: %v", version)
}

func IsValidProtocolVersion(version ProtocolVersion) bool {
	return CheckValidProtocolVersion(version) == nil
}

func CheckValidOssProtocolVersion(version ProtocolVersion) error {
	for _, v := range AllOssProtocolVersions() {
		if v == version {
			return nil
		}
	}
	return fmt.Errorf("invalid protocol version: %v", version)
}

func IsValidOssProtocolVersion(version ProtocolVersion) bool {
	return CheckValidOssProtocolVersion(version) == nil
}

func CheckValidDseProtocolVersion(version ProtocolVersion) error {
	for _, v := range AllDseProtocolVersions() {
		if v == version {
			return nil
		}
	}
	return fmt.Errorf("invalid protocol version: %v", version)
}

func IsValidDseProtocolVersion(version ProtocolVersion) bool {
	return CheckValidDseProtocolVersion(version) == nil
}

func CheckValidOpCode(code OpCode) error {
	switch code {
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
		return fmt.Errorf("invalid opcode: %v", code)
	}
	return nil
}

func IsValidOpCode(code OpCode) bool {
	return CheckValidOpCode(code) == nil
}

func CheckValidRequestOpCode(code OpCode) error {
	if err := CheckValidOpCode(code); err != nil {
		return err
	}
	if !code.IsRequest() {
		return fmt.Errorf("invalid request opcode: %v", code)
	}
	return nil
}

func IsValidRequestOpCode(code OpCode) bool {
	return CheckValidRequestOpCode(code) == nil
}

func CheckValidResponseOpCode(code OpCode) error {
	if err := CheckValidOpCode(code); err != nil {
		return err
	}
	if code.IsRequest() {
		return fmt.Errorf("invalid response opcode: %v", code)
	}
	return nil
}

func IsValidResponseOpCode(code OpCode) bool {
	return CheckValidResponseOpCode(code) == nil
}

func CheckValidDseOpCode(code OpCode) error {
	if err := CheckValidOpCode(code); err != nil {
		return err
	}
	switch code {
	case OpCodeDseRevise:
	default:
		return fmt.Errorf("invalid DSE opcode: %v", code)
	}
	return nil
}

func IsValidDseOpCode(code OpCode) bool {
	return CheckValidDseOpCode(code) == nil
}

func CheckValidConsistencyLevel(consistency ConsistencyLevel) error {
	switch consistency {
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
		return fmt.Errorf("invalid consistency level: %v", consistency)
	}
	return nil
}

func IsValidConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckValidConsistencyLevel(consistency) == nil
}

func CheckValidNonSerialConsistencyLevel(consistency ConsistencyLevel) error {
	if err := CheckValidConsistencyLevel(consistency); err != nil {
		return err
	}
	if consistency.IsSerial() {
		return fmt.Errorf("invalid non-serial consistency level: %v", consistency)
	}
	return nil
}

func IsValidNonSerialConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckValidNonSerialConsistencyLevel(consistency) == nil
}

func CheckValidSerialConsistencyLevel(consistency ConsistencyLevel) error {
	if err := CheckValidConsistencyLevel(consistency); err != nil {
		return err
	}
	if !consistency.IsSerial() {
		return fmt.Errorf("invalid serial consistency level: %v", consistency)
	}
	return nil
}

func IsValidSerialConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckValidSerialConsistencyLevel(consistency) == nil
}

func CheckValidLocalConsistencyLevel(consistency ConsistencyLevel) error {
	if err := CheckValidConsistencyLevel(consistency); err != nil {
		return err
	}
	if !consistency.IsLocal() {
		return fmt.Errorf("invalid local consistency level: %v", consistency)
	}
	return nil
}

func IsValidLocalConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckValidLocalConsistencyLevel(consistency) == nil
}

func CheckValidEventType(eventType EventType) error {
	switch eventType {
	case EventTypeSchemaChange:
	case EventTypeTopologyChange:
	case EventTypeStatusChange:
	default:
		return fmt.Errorf("invalid event type: %v", eventType)
	}
	return nil
}

func IsValidEventType(eventType EventType) bool {
	return CheckValidEventType(eventType) == nil
}

func CheckValidWriteType(writeType WriteType) error {
	switch writeType {
	case WriteTypeSimple:
	case WriteTypeBatch:
	case WriteTypeUnloggedBatch:
	case WriteTypeCounter:
	case WriteTypeBatchLog:
	case WriteTypeView:
	case WriteTypeCdc:
	default:
		return fmt.Errorf("invalid write type: %v", writeType)
	}
	return nil
}

func IsValidWriteType(writeType WriteType) bool {
	return CheckValidWriteType(writeType) == nil
}

func CheckValidBatchType(batchType BatchType) error {
	switch batchType {
	case BatchTypeLogged:
	case BatchTypeUnlogged:
	case BatchTypeCounter:
	default:
		return fmt.Errorf("invalid BATCH type: %v", batchType)
	}
	return nil
}

func IsValidBatchType(batchType BatchType) bool {
	return CheckValidBatchType(batchType) == nil
}

func CheckValidDataTypeCode(code DataTypeCode) error {
	switch code {
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
	case DataTypeCodeDate:
	case DataTypeCodeTime:
	case DataTypeCodeSmallint:
	case DataTypeCodeTinyint:
	case DataTypeCodeDuration:
	default:
		return fmt.Errorf("invalid primitive data type code: %v", code)
	}
	return nil
}

func IsValidDataTypeCode(code DataTypeCode) bool {
	return CheckValidDataTypeCode(code) == nil
}

func CheckValidSchemaChangeType(t SchemaChangeType) error {
	switch t {
	case SchemaChangeTypeCreated:
	case SchemaChangeTypeUpdated:
	case SchemaChangeTypeDropped:
	default:
		return fmt.Errorf("invalid schema change type: %v", t)
	}
	return nil
}

func IsValidSchemaChangeType(t SchemaChangeType) bool {
	return CheckValidSchemaChangeType(t) == nil
}

func CheckValidSchemaChangeTarget(target SchemaChangeTarget) error {
	switch target {
	case SchemaChangeTargetKeyspace:
	case SchemaChangeTargetTable:
	case SchemaChangeTargetType:
	case SchemaChangeTargetFunction:
	case SchemaChangeTargetAggregate:
	default:
		return fmt.Errorf("invalid schema change target: %v", target)
	}
	return nil
}

func IsValidSchemaChangeTarget(target SchemaChangeTarget) bool {
	return CheckValidSchemaChangeTarget(target) == nil
}

func CheckValidStatusChangeType(t StatusChangeType) error {
	switch t {
	case StatusChangeTypeUp:
	case StatusChangeTypeDown:
	default:
		return fmt.Errorf("invalid status change type: %v", t)
	}
	return nil
}

func IsValidStatusChangeType(t StatusChangeType) bool {
	return CheckValidStatusChangeType(t) == nil
}

func CheckValidTopologyChangeType(t TopologyChangeType) error {
	switch t {
	case TopologyChangeTypeNewNode:
	case TopologyChangeTypeRemovedNode:
	default:
		return fmt.Errorf("invalid topology change type: %v", t)
	}
	return nil
}

func IsValidTopologyChangeType(t TopologyChangeType) bool {
	return CheckValidTopologyChangeType(t) == nil
}

func CheckValidResultType(t ResultType) error {
	switch t {
	case ResultTypeVoid:
	case ResultTypeRows:
	case ResultTypeSetKeyspace:
	case ResultTypePrepared:
	case ResultTypeSchemaChange:
	default:
		return fmt.Errorf("invalid result type: %v", t)
	}
	return nil
}

func IsValidResultType(t ResultType) bool {
	return CheckValidResultType(t) == nil
}

func CheckValidDseRevisionType(t DseRevisionType) error {
	switch t {
	case DseRevisionTypeCancelContinuousPaging:
	case DseRevisionTypeMoreContinuousPages:
	default:
		return fmt.Errorf("invalid DSE revision type: %v", t)
	}
	return nil
}

func IsValidDseRevisionType(t DseRevisionType) bool {
	return CheckValidDseRevisionType(t) == nil
}

func CheckValidFailureCode(c FailureCode) error {
	switch c {
	case FailureCodeUnknown:
	case FailureCodeTooManyTombstonesRead:
	case FailureCodeIndexNotAvailable:
	case FailureCodeCdcSpaceFull:
	case FailureCodeCounterWrite:
	case FailureCodeTableNotFound:
	case FailureCodeKeyspaceNotFound:
	default:
		return fmt.Errorf("invalid failure code: %v", c)
	}
	return nil
}

func IsValidFailureCode(t FailureCode) bool {
	return CheckValidFailureCode(t) == nil
}
