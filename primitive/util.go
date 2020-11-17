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

import (
	"fmt"
)

func AllProtocolVersions() []ProtocolVersion {
	return []ProtocolVersion{
		ProtocolVersion2,
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
	if !IsValidProtocolVersion(version) {
		return fmt.Errorf("invalid protocol version: %v", version)
	}
	return nil
}

func IsValidProtocolVersion(version ProtocolVersion) bool {
	for _, v := range AllProtocolVersions() {
		if v == version {
			return true
		}
	}
	return false
}

func CheckValidOssProtocolVersion(version ProtocolVersion) error {
	if !IsValidOssProtocolVersion(version) {
		return fmt.Errorf("invalid OSS protocol version: %v", version)
	}
	return nil
}

func IsValidOssProtocolVersion(version ProtocolVersion) bool {
	for _, v := range AllOssProtocolVersions() {
		if v == version {
			return true
		}
	}
	return false
}

func CheckValidDseProtocolVersion(version ProtocolVersion) error {
	if !IsValidDseProtocolVersion(version) {
		return fmt.Errorf("invalid DSE protocol version: %v", version)
	}
	return nil
}

func IsValidDseProtocolVersion(version ProtocolVersion) bool {
	for _, v := range AllDseProtocolVersions() {
		if v == version {
			return true
		}
	}
	return false
}

func CheckValidOpCode(code OpCode) error {
	if !IsValidOpCode(code) {
		return fmt.Errorf("invalid opcode: %v", code)
	}
	return nil
}

func IsValidOpCode(code OpCode) bool {
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
		return false
	}
	return true
}

func CheckValidRequestOpCode(code OpCode) error {
	if !IsValidRequestOpCode(code) {
		return fmt.Errorf("invalid request opcode: %v", code)
	}
	return nil
}

func IsValidRequestOpCode(code OpCode) bool {
	if !IsValidOpCode(code) {
		return false
	}
	return code.IsRequest()
}

func CheckValidResponseOpCode(code OpCode) error {
	if !IsValidResponseOpCode(code) {
		return fmt.Errorf("invalid response opcode: %v", code)
	}
	return nil
}

func IsValidResponseOpCode(code OpCode) bool {
	if !IsValidOpCode(code) {
		return false
	}
	return !code.IsRequest()
}

func CheckValidDseOpCode(code OpCode) error {
	if !IsValidDseOpCode(code) {
		return fmt.Errorf("invalid DSE opcode: %v", code)
	}
	return nil
}

func IsValidDseOpCode(code OpCode) bool {
	if !IsValidOpCode(code) {
		return false
	}
	switch code {
	case OpCodeDseRevise:
	default:
		return false
	}
	return true
}

func CheckValidConsistencyLevel(consistency ConsistencyLevel) error {
	if !IsValidConsistencyLevel(consistency) {
		return fmt.Errorf("invalid consistency level: %v", consistency)
	}
	return nil
}

func IsValidConsistencyLevel(consistency ConsistencyLevel) bool {
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
		return false
	}
	return true
}

func CheckValidNonSerialConsistencyLevel(consistency ConsistencyLevel) error {
	if !IsValidNonSerialConsistencyLevel(consistency) {
		return fmt.Errorf("invalid non-serial consistency level: %v", consistency)
	}
	return nil
}

func IsValidNonSerialConsistencyLevel(consistency ConsistencyLevel) bool {
	if !IsValidConsistencyLevel(consistency) {
		return false
	}
	return !consistency.IsSerial()
}

func CheckValidSerialConsistencyLevel(consistency ConsistencyLevel) error {
	if !IsValidSerialConsistencyLevel(consistency) {
		return fmt.Errorf("invalid serial consistency level: %v", consistency)
	}
	return nil
}

func IsValidSerialConsistencyLevel(consistency ConsistencyLevel) bool {
	if !IsValidConsistencyLevel(consistency) {
		return false
	}
	return consistency.IsSerial()
}

func CheckValidLocalConsistencyLevel(consistency ConsistencyLevel) error {
	if !IsValidLocalConsistencyLevel(consistency) {
		return fmt.Errorf("invalid local consistency level: %v", consistency)
	}
	return nil
}

func IsValidLocalConsistencyLevel(consistency ConsistencyLevel) bool {
	if !IsValidConsistencyLevel(consistency) {
		return false
	}
	return consistency.IsLocal()
}

func CheckValidNonLocalConsistencyLevel(consistency ConsistencyLevel) error {
	if !IsValidNonLocalConsistencyLevel(consistency) {
		return fmt.Errorf("invalid non-local consistency level: %v", consistency)
	}
	return nil
}

func IsValidNonLocalConsistencyLevel(consistency ConsistencyLevel) bool {
	if !IsValidConsistencyLevel(consistency) {
		return false
	}
	return !consistency.IsLocal()
}

func CheckValidEventType(eventType EventType) error {
	if !IsValidEventType(eventType) {
		return fmt.Errorf("invalid event type: %v", eventType)
	}
	return nil
}

func IsValidEventType(eventType EventType) bool {
	switch eventType {
	case EventTypeSchemaChange:
	case EventTypeTopologyChange:
	case EventTypeStatusChange:
	default:
		return false
	}
	return true
}

func CheckValidWriteType(writeType WriteType) error {
	if !IsValidWriteType(writeType) {
		return fmt.Errorf("invalid write type: %v", writeType)
	}
	return nil
}

func IsValidWriteType(writeType WriteType) bool {
	switch writeType {
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

func CheckValidBatchType(batchType BatchType) error {
	if !IsValidBatchType(batchType) {
		return fmt.Errorf("invalid BATCH type: %v", batchType)
	}
	return nil
}

func IsValidBatchType(batchType BatchType) bool {
	switch batchType {
	case BatchTypeLogged:
	case BatchTypeUnlogged:
	case BatchTypeCounter:
	default:
		return false
	}
	return true
}

func CheckValidDataTypeCode(code DataTypeCode, version ProtocolVersion) error {
	if !IsValidDataTypeCode(code, version) {
		return fmt.Errorf("invalid data type code for %v: %v", version, code)
	}
	return nil
}

func IsValidDataTypeCode(code DataTypeCode, version ProtocolVersion) bool {
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
	case DataTypeCodeText:
		return version <= ProtocolVersion2
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
		return version >= ProtocolVersion3
	case DataTypeCodeTuple:
		return version >= ProtocolVersion3
	default:
		return false
	}
	return true
}

func CheckValidPrimitiveDataTypeCode(code DataTypeCode, version ProtocolVersion) error {
	if !IsValidPrimitiveDataTypeCode(code, version) {
		return fmt.Errorf("invalid primitive data type code for %v: %v", version, code)
	}
	return nil
}

func IsValidPrimitiveDataTypeCode(code DataTypeCode, version ProtocolVersion) bool {
	if !IsValidDataTypeCode(code, version) {
		return false
	}
	return code.IsPrimitive()
}

func CheckValidSchemaChangeType(t SchemaChangeType) error {
	if !IsValidSchemaChangeType(t) {
		return fmt.Errorf("invalid schema change type: %v", t)
	}
	return nil
}

func IsValidSchemaChangeType(t SchemaChangeType) bool {
	switch t {
	case SchemaChangeTypeCreated:
	case SchemaChangeTypeUpdated:
	case SchemaChangeTypeDropped:
	default:
		return false
	}
	return true
}

func CheckValidSchemaChangeTarget(target SchemaChangeTarget, version ProtocolVersion) error {
	if !IsValidSchemaChangeTarget(target, version) {
		return fmt.Errorf("invalid schema change target for %v: %v", version, target)
	}
	return nil
}

func IsValidSchemaChangeTarget(target SchemaChangeTarget, version ProtocolVersion) bool {
	switch target {
	case SchemaChangeTargetKeyspace:
	case SchemaChangeTargetTable:
	case SchemaChangeTargetType:
		return version >= ProtocolVersion3
	case SchemaChangeTargetFunction:
		return version >= ProtocolVersion4
	case SchemaChangeTargetAggregate:
		return version >= ProtocolVersion4
	default:
		return false
	}
	return true
}

func CheckValidStatusChangeType(t StatusChangeType) error {
	if !IsValidStatusChangeType(t) {
		return fmt.Errorf("invalid status change type: %v", t)
	}
	return nil
}

func IsValidStatusChangeType(t StatusChangeType) bool {
	switch t {
	case StatusChangeTypeUp:
	case StatusChangeTypeDown:
	default:
		return false
	}
	return true
}

func CheckValidTopologyChangeType(t TopologyChangeType, version ProtocolVersion) error {
	if !IsValidTopologyChangeType(t, version) {
		return fmt.Errorf("invalid topology change type for %v: %v", version, t)
	}
	return nil
}

func IsValidTopologyChangeType(t TopologyChangeType, version ProtocolVersion) bool {
	switch t {
	case TopologyChangeTypeNewNode:
	case TopologyChangeTypeRemovedNode:
	case TopologyChangeTypeMovedNode:
		return version >= ProtocolVersion3
	default:
		return false
	}
	return true
}

func CheckValidResultType(t ResultType) error {
	if !IsValidResultType(t) {
		return fmt.Errorf("invalid result type: %v", t)
	}
	return nil
}

func IsValidResultType(t ResultType) bool {
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

func CheckValidDseRevisionType(t DseRevisionType, version ProtocolVersion) error {
	if !IsValidDseRevisionType(t, version) {
		return fmt.Errorf("invalid DSE revision type for %v: %v", version, t)
	}
	return nil
}

func IsValidDseRevisionType(t DseRevisionType, version ProtocolVersion) bool {
	switch t {
	case DseRevisionTypeCancelContinuousPaging:
	case DseRevisionTypeMoreContinuousPages:
		return version >= ProtocolVersionDse2
	default:
		return false
	}
	return true
}

func CheckValidFailureCode(c FailureCode) error {
	if !IsValidFailureCode(c) {
		return fmt.Errorf("invalid failure code: %v", c)
	}
	return nil
}

func IsValidFailureCode(c FailureCode) bool {
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
