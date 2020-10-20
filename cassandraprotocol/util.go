package cassandraprotocol

import (
	"errors"
	"fmt"
)

func CheckConsistencyLevel(consistency ConsistencyLevel) error {
	switch consistency {
	case ConsistencyLevelAny:
		return nil
	case ConsistencyLevelOne:
		return nil
	case ConsistencyLevelTwo:
		return nil
	case ConsistencyLevelThree:
		return nil
	case ConsistencyLevelQuorum:
		return nil
	case ConsistencyLevelAll:
		return nil
	case ConsistencyLevelLocalQuorum:
		return nil
	case ConsistencyLevelEachQuorum:
		return nil
	case ConsistencyLevelSerial:
		return nil
	case ConsistencyLevelLocalSerial:
		return nil
	case ConsistencyLevelLocalOne:
		return nil
	default:
		return fmt.Errorf("invalid consistency level: %v", consistency)
	}
}

func CheckWriteType(writeType WriteType) error {
	switch writeType {
	case WriteTypeSimple:
		return nil
	case WriteTypeBatch:
		return nil
	case WriteTypeUnloggedBatch:
		return nil
	case WriteTypeCounter:
		return nil
	case WriteTypeBatchLog:
		return nil
	case WriteTypeView:
		return nil
	case WriteTypeCdc:
		return nil
	default:
		return fmt.Errorf("invalid write type: %v", writeType)
	}
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
