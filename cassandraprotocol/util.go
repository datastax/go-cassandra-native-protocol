package cassandraprotocol

import (
	"errors"
	"fmt"
)

func CheckOpCode(code OpCode) error {
	switch code {
	case OpCodeStartup:
	case OpCodeOptions:
	case OpCodeQuery:
	case OpCodePrepare:
	case OpCodeExecute:
	case OpCodeRegister:
	case OpCodeBatch:
	case OpCodeAuthResponse:
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

func IsOpCode(code OpCode) bool {
	return CheckOpCode(code) == nil
}

func CheckRequestOpCode(code OpCode) error {
	switch code {
	case OpCodeStartup:
	case OpCodeOptions:
	case OpCodeQuery:
	case OpCodePrepare:
	case OpCodeExecute:
	case OpCodeRegister:
	case OpCodeBatch:
	case OpCodeAuthResponse:
	default:
		return fmt.Errorf("invalid request opcode: %v", code)
	}
	return nil
}

func IsRequestOpCode(code OpCode) bool {
	return CheckRequestOpCode(code) == nil
}

func CheckResponseOpCode(code OpCode) error {
	switch code {
	case OpCodeError:
	case OpCodeReady:
	case OpCodeAuthenticate:
	case OpCodeSupported:
	case OpCodeResult:
	case OpCodeEvent:
	case OpCodeAuthChallenge:
	case OpCodeAuthSuccess:
	default:
		return fmt.Errorf("invalid response opcode: %v", code)
	}
	return nil
}

func IsResponseOpCode(code OpCode) bool {
	return CheckResponseOpCode(code) == nil
}

func CheckConsistencyLevel(consistency ConsistencyLevel) error {
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

func IsConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckConsistencyLevel(consistency) == nil
}

func CheckNonSerialConsistencyLevel(consistency ConsistencyLevel) error {
	switch consistency {
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
		return fmt.Errorf("invalid non-serial consistency level: %v", consistency)
	}
	return nil
}

func IsNonSerialConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckNonSerialConsistencyLevel(consistency) == nil
}

func CheckSerialConsistencyLevel(consistency ConsistencyLevel) error {
	switch consistency {
	case ConsistencyLevelLocalSerial:
	case ConsistencyLevelSerial:
	default:
		return fmt.Errorf("invalid serial consistency level: %v", consistency)
	}
	return nil
}

func IsSerialConsistencyLevel(consistency ConsistencyLevel) bool {
	return CheckSerialConsistencyLevel(consistency) == nil
}

func CheckEventType(eventType EventType) error {
	switch eventType {
	case EventTypeSchemaChange:
	case EventTypeTopologyChange:
	case EventTypeStatusChange:
	default:
		return fmt.Errorf("invalid event type: %v", eventType)
	}
	return nil
}

func IsEventType(eventType EventType) bool {
	return CheckEventType(eventType) == nil
}

func CheckWriteType(writeType WriteType) error {
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

func IsWriteType(writeType WriteType) bool {
	return CheckWriteType(writeType) == nil
}

func CheckBatchType(batchType BatchType) error {
	switch batchType {
	case BatchTypeLogged:
	case BatchTypeUnlogged:
	case BatchTypeCounter:
	default:
		return errors.New(fmt.Sprintf("invalid BATCH type: %v", batchType))
	}
	return nil
}

func IsBatchType(batchType BatchType) bool {
	return CheckBatchType(batchType) == nil
}

func CheckPrimitiveDataTypeCode(code DataTypeCode) error {
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

func IsPrimitiveDataTypeCode(code DataTypeCode) bool {
	return CheckPrimitiveDataTypeCode(code) == nil
}
