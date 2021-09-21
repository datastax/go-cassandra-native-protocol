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

// SupportedProtocolVersions returns a slice containing all the protocol versions supported by this library.
func SupportedProtocolVersions() []ProtocolVersion {
	return []ProtocolVersion{
		ProtocolVersion2,
		ProtocolVersion3,
		ProtocolVersion4,
		ProtocolVersion5,
		ProtocolVersionDse1,
		ProtocolVersionDse2,
	}
}

func SupportedOssProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v.IsOss() })
}

func SupportedDseProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v.IsDse() })
}

func SupportedBetaProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v.IsBeta() })
}

func SupportedNonBetaProtocolVersions() []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return !v.IsBeta() })
}

func SupportedProtocolVersionsGreaterThanOrEqualTo(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v >= version })
}

func SupportedProtocolVersionsGreaterThan(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v > version })
}

func SupportedProtocolVersionsLesserThanOrEqualTo(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v <= version })
}

func SupportedProtocolVersionsLesserThan(version ProtocolVersion) []ProtocolVersion {
	return matchingProtocolVersions(func(v ProtocolVersion) bool { return v < version })
}

func matchingProtocolVersions(filters ...func(version ProtocolVersion) bool) []ProtocolVersion {
	var result []ProtocolVersion
	for _, v := range SupportedProtocolVersions() {
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

func CheckSupportedProtocolVersion(version ProtocolVersion) error {
	if !version.IsSupported() {
		return fmt.Errorf("invalid protocol version: %v", version)
	}
	return nil
}

func CheckDseProtocolVersion(version ProtocolVersion) error {
	if !version.IsDse() {
		return fmt.Errorf("invalid DSE protocol version: %v", version)
	}
	return nil
}

func CheckValidOpCode(code OpCode) error {
	if !code.IsValid() {
		return fmt.Errorf("invalid opcode: %v", code)
	}
	return nil
}

func CheckRequestOpCode(code OpCode) error {
	if !code.IsRequest() {
		return fmt.Errorf("expected request opcode, but got: %v", code)
	}
	return nil
}

func CheckResponseOpCode(code OpCode) error {
	if !code.IsResponse() {
		return fmt.Errorf("expected response opcode, but got: %v", code)
	}
	return nil
}

func CheckValidConsistencyLevel(consistency ConsistencyLevel) error {
	if !consistency.IsValid() {
		return fmt.Errorf("invalid consistency level: %v", consistency)
	}
	return nil
}

func CheckSerialConsistencyLevel(consistency ConsistencyLevel) error {
	if !consistency.IsSerial() {
		return fmt.Errorf("invalid serial consistency level: %v", consistency)
	}
	return nil
}

func CheckValidEventType(eventType EventType) error {
	if !eventType.IsValid() {
		return fmt.Errorf("invalid event type: %v", eventType)
	}
	return nil
}

func CheckValidWriteType(writeType WriteType) error {
	if !writeType.IsValid() {
		return fmt.Errorf("invalid write type: %v", writeType)
	}
	return nil
}

func CheckValidBatchType(batchType BatchType) error {
	if !batchType.IsValid() {
		return fmt.Errorf("invalid BATCH type: %v", batchType)
	}
	return nil
}

func CheckValidDataTypeCode(code DataTypeCode, version ProtocolVersion) error {
	if !code.IsValid() || !version.SupportsDataType(code) {
		return fmt.Errorf("invalid data type code for %v: %v", version, code)
	}
	return nil
}

func CheckValidSchemaChangeType(t SchemaChangeType) error {
	if !t.IsValid() {
		return fmt.Errorf("invalid schema change type: %v", t)
	}
	return nil
}

func CheckValidSchemaChangeTarget(target SchemaChangeTarget, version ProtocolVersion) error {
	if !target.IsValid() || !version.SupportsSchemaChangeTarget(target) {
		return fmt.Errorf("invalid schema change target for %v: %v", version, target)
	}
	return nil
}

func CheckValidStatusChangeType(t StatusChangeType) error {
	if !t.IsValid() {
		return fmt.Errorf("invalid status change type: %v", t)
	}
	return nil
}

func CheckValidTopologyChangeType(t TopologyChangeType, version ProtocolVersion) error {
	if !t.IsValid() || !version.SupportsTopologyChangeType(t) {
		return fmt.Errorf("invalid topology change type for %v: %v", version, t)
	}
	return nil
}

func CheckValidResultType(t ResultType) error {
	if !t.IsValid() {
		return fmt.Errorf("invalid result type: %v", t)
	}
	return nil
}

func CheckValidDseRevisionType(t DseRevisionType, version ProtocolVersion) error {
	if !t.IsValid() || !version.SupportsDseRevisionType(t) {
		return fmt.Errorf("invalid DSE revision type for %v: %v", version, t)
	}
	return nil
}

func CheckValidFailureCode(c FailureCode) error {
	if !c.IsValid() {
		return fmt.Errorf("invalid failure code: %v", c)
	}
	return nil
}
